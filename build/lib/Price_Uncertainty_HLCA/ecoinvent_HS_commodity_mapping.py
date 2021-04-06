import fnmatch
import pandas as pd
import os
import numpy as np
import json



def ecoHS12_dic(concordance_file_path, PRO, save_dir=None, dict_name=None):
    """Function maps ecoinvent processes to HS commodities based on cpc21-HS12
    concordance.
    Inputs
    concordance_file_path     path to concordance 'file cpc21-hs2012.txt'
                              which is the UN Stats concordance file. See
                              https://unstats.un.org/unsd/classifications/econ
    
    PRO                       pandas dataframe with ecoinvent activity meta data

    save_dir                  dir to save the dictionary json file, if not given
                              but dict_name is given, it will be saved in the
                              working dir

    dict_name                 file name for the json file. If None, dict will
                              not be saved.
    """

    cpc21_hs12 = pd.read_csv(concordance_file_path, header=0, dtype='str')
    cpc21_hs12['HS12code'] = cpc21_hs12['HS12code'].str.replace('.','')

    cpc_codes = PRO['cpc'].str.split(':').str.get(0)
    
    eco_hs12_mapping_dic, _ = create_dict_ecoinvent_HS12(cpc21_hs12,
                                    PRO.index, cpc_codes)

    if dict_name==None:
        print('No Path and Filename provided. Not saving the dictionary.')
        return eco_hs12_mapping_dic
    else:
        if save_dir==None:
            print('No Path to save File specified, saving in current wd.')
            save_dir = os.getcwd()
        save_dir = os.path.realpath(save_dir)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        filePath = os.path.join(save_dir,'ecoinvent35-HS12_mapping.json')
        print("Saving dictionary to: '{}'".format(filePath))
        with open(filePath, 'w') as fh:
            json.dump(eco_hs12_mapping_dic, fh)
        return eco_hs12_mapping_dic




def create_dict_ecoinvent_HS12(cpc21_hs12, UUIDs, cpc_codes):
    """
    Create a dictionanry between the ecoinvent process_referenceProduct UUID
    and the matching HS codes if existend.
    Input:
    cpc21_hs12       A dataframe of the concordance file cpc21-hs12
    PRO              A dataframe with the ecoinvent process data as defined pylcaio
    
    Output           A dictionary with the process UUID's as keys, and the matching
                     HS codes as a list of string(s) as values.
    Depends on:
    
    - fnmatch
    """
    len_codes = np.zeros(len(UUIDs))
    eco_hs12_mapping_dic = {}
    for i,(index, code) in enumerate(zip(UUIDs, cpc_codes)):
        # all numbers starting with >=5 are services and not present in HS
        if code != None and int(code[0]) <= 4:
            l = len(code)
            len_codes[i] = l
            if l == 5:
                try:
                    eco_hs12_mapping_dic[index] = cpc21_hs12.set_index('CPC21code').loc[code, 'HS12code'].unique().tolist()
                except AttributeError:  # only 1 match,
                    eco_hs12_mapping_dic[index] = [cpc21_hs12.set_index('CPC21code').loc[code, 'HS12code']]
                except KeyError:
                    print(l, index, code)
            else:
                # only section (1 digit) group (3 digits) or class (4digits) have been given.
                # Use wilde card to match. 
                cpc_codes = fnmatch.filter(cpc21_hs12['CPC21code'].tolist(),code+'*')
                try:
                    eco_hs12_mapping_dic[index] = cpc21_hs12.set_index('CPC21code').loc[cpc_codes, 'HS12code'].unique().tolist()
                except KeyError:
                    print(l, index, code)
        else:
            eco_hs12_mapping_dic[index] = None
    return eco_hs12_mapping_dic, len_codes
