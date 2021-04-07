import numpy as np
import pandas as pd
import os
import ray
import json
import warnings
import pickle
import time
import pdb

path = '/home/jakobs/Documents/IndEcol/OASES/pylcaio/src/Databases/ecoinvent3.5_exiobase3/baci_price_data_2012_data_cpc21_hs12/'



def Match_BACI_data_to_ecoinvent(
        PRO,
        path_to_BACI_data,
        outputDir=None,
        USD_EURO_exr=None,
        Nsamples=3000,
        mapping_data_dir='../mapping_data/',
        region_mapping_dict_name='ecoinvent35-baci_region_mapping.json',
        commodity_mapping_dict_name='ecoinvent35-HS12_mapping.json',
        n_cores=None
        ):
    """
    This function matches BACI price distribution data to ecoinvent activties
    based on their reference product and geography. The distribution is the
    volume (in tons) weighted price distribution of all trade flows exported from
    the activity's region (geography). For every actitvity with a match it outputs
    a pickle file in the outputDir. It relies on Ray multiprocessing.
    
    Input:

    PRO                     Pandas DataFrame with ecoinvent process meta data.
                            output of ecospold2matrix:
                            https://github.com/majeau-bettez/ecospold2matrix
    path_to_BACI_data       Path to BACI trade data csv file
    outputDir               Directory to save the output pickles
    USD_EURO_exr            Exchange rate USD to EURO to use. the USD price will
                            divided by this number to obtain the euro price.
                            Default is 1.
    Nsamples                Number of samples to draw from the price distribution
    mapping_data_dir        Directory with the mapping dictionaries. Default:
                            '../mappind_data'
    region_mapping_dict_name
                            Name of region mapping dict in the above directory.
                            Default: 'ecoinvent35-baci_region_mapping.json',
    commodity_mapping_dict_name
                            Name of the commodity mapping dictionary in the
                            mapping directory. Default: 'ecoinvent35-HS12_mapping.json',
    n_cores                 Number of cores to use for the multiprocessing.
                            Default: N_max-1, where N_max = # available cpu's
   
    """
    

    # Check if necessary inputs have been given
    if outputDir==None:
        raise Exception("Please provide a path to save the pickle files")
    if USD_EURO_exr==None:
        warnings.warn("No exchange given. Default rate of 1 will be used")
        USD_EURO_exr = 1 
    
    # Check if mapping dictionaries are available
    mapping_data = os.path.realpath(mapping_data_dir)
    if region_mapping_dict_name==None or commodity_mapping_dict_name==None:
        if not ((region_mapping_dict_name in os.listdir(mapping_data)) and
                (commodity_mapping_dict_name in os.listdir(mapping_data))):
            raise Exception("Please provide directory with mapping dictionaries\
                    between activities and regions and HS12 commodities. As well\
                    as the correct file names if different from dedault")
    # Check if BACI file exists:
    if not os.path.isfile(path_to_BACI_data):
        raise Exception("Could not find {}. Please provide a valid path to\
                          BACI data file".format(path_to_BACI_data))
    
    # Load dictionaries
    eco_HS12_mapping, eco_baci_region_mapping_dic = read_mapping_dicts(
            mapping_data, region_mapping_dict_name, commodity_mapping_dict_name)


    # Load BACI data:
    baci_data = readDataBACI(path_to_BACI_data, USD_EURO_exr)



    # Check if outDir exists and if not make it.
    outputDir = os.path.realpath(outputDir)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
        print('Created the output directory {}'.format(outputDir))

    # Prepare dictionary for fast row lookup:
    print('prepare lookup dictionary for faster row lookup...')
    baci_i_row_dic = {}
    for reg in baci_data.i.unique():
        baci_i_row_dic[reg] = np.where(baci_data.i == reg)[0].tolist()
    

    # If not specified, use 1 less than the available number of cores.
    if n_cores==None:
        n_cores = os.cpu_count()-1
    
    print("Initializing Ray multiprocessing with {} cores".format(n_cores))
    ray.init(num_cpus=n_cores)
     
    for i,act in enumerate(PRO.loc[(PRO['unitName']=='kg') &
        (PRO['cpc'].str.split(':').str.get(0).str.len() >= 4)].itertuples()):
        # only iterate over processes with the right units and with a cpc 
        # code that has at least 4 digits

        if i%100 ==0:
            print('{} of total {}'.format(i+1, len(PRO.loc[(PRO['unitName']=='kg') &
                         (PRO['cpc'].str.split(':').str.get(0).str.len() >=4)])))
        
        get_baci_price_data.remote(act.Index, act, eco_baci_region_mapping_dic,
                                   eco_HS12_mapping, baci_data, baci_i_row_dic,
                                   Nsamples, outputDir)
    
    print('Done matching {} activties to BACI price data'.format(i))
    # Close ray remote
    print('Shutting down Ray multiprocessing')
    ray.shutdown()
    

def readDataBACI(path_to_BACI_data, USD_EURO_exr=1):
    """
    Input:
    path_to_BACI_data   path to BACI data file for year to use

    USD_EURO_exr        USD per EURO exchange rate default is 1. See EUROSTAT:
                        https://ec.europa.eu/eurostat/databrowser/bookmark/bc107428-d077-4a9a-86a5-a4f045cf63e9?lang=en

    """
    print('reading in BACI data...')
    baci_data = pd.read_csv(path_to_BACI_data, sep=',',
                            dtype={'t':int, 'i':int,
                                   'j':int, 'k':str,
                                   'v':float, 'q':float})
    baci_data['p'] = baci_data['v']/baci_data['q']
    baci_data['p_euro'] = baci_data['p']/USD_EURO_exr
    return baci_data


def read_mapping_dicts(mapping_dir, region_mapping_dict_name,
                commodity_mapping_dict_name):

    with open(os.path.join(mapping_dir, commodity_mapping_dict_name), 'r') as fh:
        eco_HS12_mapping = json.load(fh)
    with open(os.path.join(mapping_dir, region_mapping_dict_name), 'r') as fh:
        eco_baci_region_mapping_dic = json.load(fh)

    return eco_HS12_mapping, eco_baci_region_mapping_dic




@ray.remote
def get_baci_price_data(proc_index, act, eco_baci_region_mapping_dic,
                        eco_HS12_mapping, baci_data, baci_i_row_dic,
                        draw_nsamples=3000, outDir=None):
    """Maps baci prices to ecoinvent processes. Uses volume weighted price
    price distribution from which it draws 'draw_nsamples=3000' samples.
    Not all ecoinvent activity reference products have 3000 flows which means
    multiple prices will be sampled more often. This however will not affect the
    distribution in a significant way.
    Input:
    proc_index          the UUID of the ecoinvent activity
    act                 all meta data from the PRO dataframe for this activity
    eco_baci_region_mapping_dic
                        dict mapping ecoinvent regions to BACI region codes
    eco_HS12_mapping    dict mapping ecoinvent act to HS12 commodities
    baci_data           Dataframe with BACI data
    baci_i_row_dic      Dictionary mapping rows for each exporting counrty
    draw_nsamples       Numberof samples to draw from the price distribution
    outDir              Directory to save the pickle files
    
    
    """
    try:
        eco_HS12_mapping[proc_index]
    except KeyError:
        pdb.set_trace()
    
    if eco_HS12_mapping[proc_index] != None:
        proc, prod = proc_index.split('_')
        #print(i, ' : ', proc_index) 
        if act.geography != 'RoW':
            baci_regs = eco_baci_region_mapping_dic[act.geography]
            indices_regs = [row for reg in baci_regs if reg in baci_i_row_dic.keys() for row in baci_i_row_dic[reg]]
        else:
            baci_regs = eco_baci_region_mapping_dic[proc_index]
            indices_regs = [row for reg in baci_regs if reg in baci_i_row_dic.keys() for row in baci_i_row_dic[reg]]
           
        hs12_codes = eco_HS12_mapping[proc_index]
        indices_prods = [row for prod in hs12_codes for row in np.where(baci_data.k ==prod)[0]]
        
        total_mask = list(set(indices_regs).intersection(indices_prods))
        total_mask.sort()
        
        
        prices_euro = baci_data.loc[total_mask, 'p_euro']
        weights = baci_data.loc[total_mask, 'q']

        try:
            avg, std = weighted_avg_and_std(prices_euro, weights)
            sample_price = np.random.choice(prices_euro, size=draw_nsamples,
                                            p=weights/weights.sum())
            
            price_dic = {}
            price_dic['activityName'] = act.activityName
            price_dic['geography'] = act.geography
            price_dic['productName'] = act.productName
            price_dic['cpc'] = act.cpc
            price_dic['unitName'] = act.unitName
            
            price_dic['prices_euro'] = prices_euro
            price_dic['weights'] = weights
            price_dic['price_sample'] = sample_price
            price_dic['price_baci_mean'] = avg
            price_dic['price_baci_std'] = std
            price_dic['price_percentiles'] = np.percentile(sample_price, [2.5,16,50,84,97.5])
            price_dic['price_baci_min'] = np.min(prices_euro)
            price_dic['price_baci_max'] = np.max(prices_euro)
            price_dic['nr_baci_flows'] = len(prices_euro)
            
            with open(os.path.join(outDir, '{}.pickle'.format(proc_index)), 'wb') as fh:
                pickle.dump(price_dic, fh)
            
        except ZeroDivisionError:
            pass
            
        except ValueError:
            print(proc_index)
            print(price_dic)
            pdb.set_trace()



def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values-average)**2, weights=weights)
    return (average, np.sqrt(variance))
