import pandas as pd
import numpy as np
import os
import constructive_geometries as cg
import sys
# Append paths to region concordance code
sys.path.append('/home/jakobs/Documents/IndEcol/OASES/ensemble/ensemble/concordance/')
# sys.path.append('/home/jakobs/Documents/IndEcol/OASES/ensemble/ensemble/ecoparser/')
sys.path.append('/home/jakobs/Documents/IndEcol/OASES/ensemble/ensemble/cutoffMatrix/')
sys.path.append('/home/jakobs/Documents/IndEcol/OASES/ensemble/ensemble/utools/')
from regions_concordance import RegionConcordance
import alogging
import cutoffmatrix as cm
import time
import seaborn as sns
import itertools
import json
import feather


productConcFile = '/home/jakobs/data/EcoBase/Product_Concordances-ei35-Update-20-12-2018.xlsx'
productOutDir = '/home/jakobs/data/EcoBase/'
#productPickleFile = None
productPickleFile = '/home/jakobs/data/EcoBase/EcoExioProductConcordance_2019-01-03.pickle'

regionEcoDir = '/home/jakobs/data/ecoinvent/ecoinvent 3.5_cutoff_ecoSpold02/ecoinvent 3.5_cutoff_ecoSpold02/MasterData'
regionExioDir = '/home/jakobs/data/EXIOBASE/'
regionOutDir = '/home/jakobs/data/EcoBase/'
regionPickleFile = '/home/jakobs/data/EcoBase/EcoExioRegionConcordance_2019-01-03.pickle'



logger = alogging.Logger('./', 'BACI_price_uncertainty', __file__)
country_codes  = pd.read_csv('/home/jakobs/Documents/IndEcol/Data/BACI/country_code_baci96/country_codes_baci.csv', sep=',', encoding = 'iso-8859-1')
PC,RC = cm.GetConcordance(productConcFile,productPickleFile,regionEcoDir,regionExioDir,regionPickleFile, productOutDir, logger)

def create_eco_baci_geography_mapping(PRO, outPath=None, outFile=None):
    eco_baci_region_mapping_dic = {}
    covered_regions = []
    for i,proc_index in enumerate(PRO.index.values):
        if PRO.loc[proc_index, 'geography'] != 'RoW' and PRO.loc[proc_index, 'geography'] in covered_regions:
            continue
        else:
            if PRO.loc[proc_index, 'geography'] != 'RoW':
                covered_regions.append(PRO.loc[proc_index, 'geography'])
                # print(len(covered_regions))
            proc, prod = proc_index.split('_')
            #print(proc_index)
            ecoRegion, excluded = cm.GetEcoRegion(proc, prod, PRO)
            if ecoRegion == ecoRegion:
                try:
                    baci_regs = eco2BACI_region(eco_reg=ecoRegion, excluded=excluded)
                    if ecoRegion != 'RoW':
                        eco_baci_region_mapping_dic[ecoRegion] = baci_regs
                    else:
                        eco_baci_region_mapping_dic[proc_index] =baci_regs
                except ValueError:
                    print('\n region problem')
                    print(hybridized_PRO.loc[proc_index])
                    continue
    if not outPath and not outFile:
        print('No output path nor file given. Dictionary will be returned but not saved...')
        
        return eco_baci_region_mapping_dic
    else:
        if not outPath:
            outPath = os.getcwd()
        if not outFile:
            outFile = 'ecoinvent35-baci_region_mapping.json'
        filePath = os.path.join(outPath, outFile)
        with open(filePath, 'w') as fh:
            json.dump(eco_baci_region_mapping_dic, fh)
        print('dictionary saved to {}'.format(filePath))

        return eco_baci_region_mapping_dic



def eco2BACI_region(eco_reg, country_codes=country_codes, excluded=None, RC=RC):
    """This function takes an ecoinvent region and gives back the BACI country codes as a list of floats
    Input:
    - eco_reg         :  ecoinvent region, string
    - country_codes   :  Pandas dataframe with BACI countries and codes

    Output
    - baci_codes      :  list of baci country code(s) as floats

    """
    if eco_reg != eco_reg: # filter for NaN's
        raise NameError('Invalid ecoinvent region "NaN"')
    if eco_reg in RC.regionExceptions.keys():  # check if the region is in one of the exceptions
        eco_reg = RC.regionExceptions[eco_reg] # These are some electiricty grids in the US etc

    baci_reg_codes = country_mapping.set_index('Code_eco').loc[eco_reg, 'CODE_BACI'].tolist()
    # some countries have multiple mappings (i.e. Namibia as it is not in BACI but is listed in the BACI list,
    # and is also mapped to Africa NES)
    if not baci_reg_codes != baci_reg_codes:
        baci_codes = [int(code) for code in FilterBaciCountries(baci_reg_codes)]

    elif eco_reg != 'RoW' and eco_reg != 'GLO':
            try:
                countries = RC.CountryList(eco_reg)
            except KeyError:
                raise KeyError("Can't find region {} in constructive geometries package".format(eco_reg))
            try:
                baci_reg_codes = country_mapping.set_index('Code_eco').loc[countries,'CODE_BACI'].unique().tolist()
                baci_codes = [int(code) for code in FilterBaciCountries(baci_reg_codes)]
            except KeyError:
                raise KeyError('Region "{}" does not contain valid countries or regions. Returned None'.format(eco_reg))

    elif eco_reg == 'GLO':
            baci_codes = [int(code) for code in
                          FilterBaciCountries(country_mapping['CODE_BACI'].unique().tolist())]
    elif eco_reg == 'RoW':
            if excluded != None and excluded != []:
                countries = RC.RoW(excluded=excluded)
                baci_reg_codes = country_mapping.set_index('Code_eco').loc[countries,'CODE_BACI'].unique().tolist()
                baci_codes = [int(code) for code in FilterBaciCountries(baci_reg_codes)]
            else:
                # Assume Global because no excluded countries given
                baci_codes =  [int(code) for code in
                               FilterBaciCountries(country_mapping['CODE_BACI'].unique().tolist())]

    return baci_codes

def FilterBaciCountries(codes, country_codes=country_codes):
    """Filters a list of BACI country codes for nans and if they are present in BACI.
    Input:
    - codes            :   List of baci country codes

    Output:            :   subset of input list w/o nan's nor countries nit featuring in BACI
    """
    codes = assert_list(codes)
    return [code for code in codes if code == code and
                country_codes.set_index('CODE').loc[code, 'IN_BACI'] == 1]

