import pandas as pd
import numpy as np
import pickle
import warnings
import os
import scipy.stats as stats







def make_price_df(PRO, price_data_dir, hybridized_processes, 
                  outputFileName, outputDir=None, var_dic=None):
    """
    Inputs:
    PRO                     DataFrame with metadata of processes. This is the
                            output from pylcaio as it contains more info on the
                            activities' prices.
    price_data_dir          Directory containing the pickle files with the BACI
                            price information for the activties.
    hybridized_processes    list of UUID's of hybridized processes
    outputFileName          File name for the feather file withthe price samples
    outputDir               Directory where to save the file. If None, uses
                            price_data_dir.
    var_dic                 Dictionary with coefficient of variations to use for
                            for the modelling of price distributions for those
                            activities without BACI price info. Should have the
                            following format (default numbers):
                            {'c_elec': 0.27,
                             'c_heat': 0.27,
                             'c_construction': 1.05,
                             'c_freight': 0.44,
                             'c_finance': 1.05,
                             'c_services': 1.05,
                             'c_waste': 1.05,
                             'c_other': 1.05
                             }
                            See paper where these numbers come from.

    """

    if outputDir==None:
        warnings.warn("No output directory given, using price_data_dir...")
        outputDir = os.path.realpath(price_data_dir)

    if var_dic==None:
        print("No specific variance dictionary given. Using the default:\
                {'c_elec': 0.27, 'c_heat': 0.27, 'c_construction': 1.05,\
                 'c_freight': 0.44, 'c_finance': 1.05, 'c_services': 1.05,\
                 'c_waste': 1.05, 'c_other': 1.05}")
        var_dic = {'c_elec': 0.27,
                   'c_heat': 0.27,
                   'c_construction': 1.05,
                   'c_freight': 0.44,
                   'c_finance': 1.05,
                   'c_services': 1.05,
                   'c_waste': 1.05,
                   'c_other': 1.05
                   }

    for index in PRO.index.values:
        if PRO.loc[index, 'priceless_scale_vector'] > 0:
            PRO.loc[index,'effective_price'] = PRO.loc[index, 'priceless_scale_vector']
        else:
            PRO.loc[index,'effective_price'] = PRO.loc[index, 'price']

    # Read in BACI price data
    price_data, acts_with_baci_price = read_BACI_price_data(PRO, price_data_dir)

    # Model processes without a BACI price distribution with a lognormal around
    # the default price:
    price_data = model_variance_per_category(price_data, PRO,
                                             acts_with_baci_price, var_dic,
                                             hybridized_processes)

    # Check if outDir exists and if not make it.
    outputDir = os.path.realpath(outputDir)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
        print('Created the output directory {}'.format(outputDir))

    # Save as feather file:
    price_data.reset_index(inplace=True)  # Feather does not allow for index column so make it a normal column
    price_data.columns = price_data.columns.astype(str)
    filePath = os.path.join(outputDir, outputFileName)
    price_data.to_feather(filePath)

    print('Price DataFrame saved as feather file to {}'.format(filePath))


def read_BACI_price_data(PRO, price_data_path):
    """Reads in baci price data and returns a data frame with dimensions
    (N_act,N_samples) where N_act is the number of ecoinvent processes and
    N_samples is the size of the price distributions sample in the pickle files.
    The indices of the data frame are the same as PRO i.e. the UUID's of the
    ecoinvent processes. Processes that do not have a BACI price dist have
    the 'effective_price' from the PRO meta data df.
    """
    processlist = [x.split('.')[0] for x in os.listdir(price_data_path) if x.endswith('.pickle')]
    # read the first pickle to determine the size of the distribution sample
    with open(os.path.join(price_data_path, '{}.pickle'.format(processlist[0])), 'rb') as fh:
        procdic = pickle.load(fh)
        n_samples = len(procdic['price_sample'])
    # Make df of appropriate size (N_acts, N_samples) with default prices
    price_data = pd.DataFrame(index=PRO.index, data = np.ones((len(PRO.index), n_samples)))
    price_data = price_data.multiply(PRO['effective_price'], axis='index')
    # n_flows = []
    # price_data_dic = {}
    for i,proc in enumerate(processlist):
        with open(os.path.join(price_data_path, '{}.pickle'.format(proc)), 'rb') as fh:
            procdic = pickle.load(fh)
            price_data.loc[proc] = procdic['price_sample']
            # n_flows.append(procdic['nr_baci_flows'])
            # price_data_dic[proc] = {'prices':procdic['prices_euro'], 'weights':procdic['weights'], 'nflows':procdic['nr_baci_flows']}
    # n_flows = np.array(n_flows)
    return price_data, processlist


def model_variance_per_category(price_data, PRO, processlist, var_dic,
                                hybridized_processes):
    """Models price distributions for activities without a baci price dist with
    lognormal distributions with a width specified by the var_dic. Returns an
    updated price_data data frame.
    """
    hybrid_processes_without_baci_price = [x for x in
            hybridized_processes if not x in processlist]

    processes_with_proxy_data = []

    # Electricity Processes cpc 'electrical energy' (cpc 17100)
    elec_indices = PRO.loc[hybrid_processes_without_baci_price][
                        PRO.loc[hybrid_processes_without_baci_price,'cpc'
                        ].str.split(':').str.get(0) == '17100'].index
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            elec_indices, var_dic['c_elec'],
                            processes_with_proxy_data)

    # Steam and Hot Water, often from 'heat and power cogeneration' (cpc 17300)
    heat_indices = PRO.loc[hybrid_processes_without_baci_price][
                        PRO.loc[hybrid_processes_without_baci_price,'cpc'
                        ].str.split(':').str.get(0) == '17300'].index
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            heat_indices, var_dic['c_heat'],
                            processes_with_proxy_data)

    # Construction work (cpc code that starts with 5)
    construction_indices = [i[0] for i in
                    PRO.loc[hybrid_processes_without_baci_price].iterrows()
                    if i[1].cpc!= None and i[1].cpc.split(':')[0][0] in ['5']]
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            construction_indices, var_dic['c_construction'],
                            processes_with_proxy_data)

    # Freight transport (cpc code that starts with 6)
    freight_indices = [i[0] for i in
                    PRO.loc[hybrid_processes_without_baci_price].iterrows()
                    if i[1].cpc!= None and i[1].cpc.split(':')[0][0] in ['6']]
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            freight_indices, var_dic['c_freight'],
                            processes_with_proxy_data)

    # Financial related services (cpc code that starts with 7)
    finance_indices = [i[0] for i in
                    PRO.loc[hybrid_processes_without_baci_price].iterrows()
                    if i[1].cpc!= None and i[1].cpc.split(':')[0][0] in ['7']]
    # So far these service are not included in ecoinvent or are not hybridised, 
    # so check if they exist
    if finance_indices != []:
        price_data, processes_with_proxy_data = update_price_data(price_data,
                            finance_indices, var_dic['c_finance'],
                            processes_with_proxy_data)

    # Services (cpc code that starts with 8)
    services_indices = [i[0] for i in
                    PRO.loc[hybrid_processes_without_baci_price].iterrows()
                    if i[1].cpc!= None and i[1].cpc.split(':')[0][0] in ['8']]
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            services_indices, var_dic['c_services'],
                            processes_with_proxy_data)

    # Waste and Scraps (cpc starts with 39)
    dummy_df = PRO.loc[hybrid_processes_without_baci_price,'cpc'
                                ].str.split(':').str.get(0).str.startswith('39')
    waste_and_scrap_indices = dummy_df.loc[dummy_df.values == True].index
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            waste_and_scrap_indices,
                            var_dic['c_waste'],
                            processes_with_proxy_data)

    # Remaining activities
    left_over_indices = [index for index in hybrid_processes_without_baci_price
                         if index not in processes_with_proxy_data]
    price_data, processes_with_proxy_data = update_price_data(price_data,
                            left_over_indices, var_dic['c_other'],
                            processes_with_proxy_data)

    return price_data



def update_price_data(price_data, indices, s, processes_with_proxy_data):

    n = price_data.shape[1]
    scale_multiplier = np.exp(-s**2/2)
    for index in indices:
        price = price_data.loc[index,0]  # this is a fixed number so just take the first
        scale = price*scale_multiplier
        price_data.loc[index] = stats.lognorm(s=s, scale=scale).rvs(size=n, random_state=1)
        processes_with_proxy_data.append(index)
    return price_data, processes_with_proxy_data
