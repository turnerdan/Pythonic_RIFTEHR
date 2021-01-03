"""
Perform some basic quality checks and encrypt the patient demographic data file.

USAGE
Use file containing patient demographic data.

RETURNS 
1. Returns encrypted patient demographic data in file "(directory where all patient data file exist)/AllPatients_DummyTable_enc.csv"  
2. Returns hash map mapping MRN values to there encrypted values in file "(directory where all patient data file exist)/AllPatients_DummyTable_map.csv"

"""

import hashlib
import os
from pathlib import Path

import pandas as pd


def encrypt_pt_demog_data(file_path): # file_path => file containing all patient demographic data

    # Collect paths for later use
    file_dir, file_name = os.path.split(file_path)
    file_name, file_ext = os.path.splitext(file_name)

    # read in the patient demographics data
    pt_df = pd.read_csv(file_path)

    h = pt_df.columns.to_list()
    exp_header = ['MRN', 'FirstName', 'LastName', 'PhoneNumber', 'Zipcode']

    if not h == exp_header:
        raise Exception("Patient demographic data file (%s) doesn't have the header expected: %s" % (file_path, exp_header))

    # creating encrypted table
    pt_mrn_hashes = dict()
    encrypted = []
    
    for i, (mrn, fn, ln, phone, zipcode) in pt_df.iterrows():
        fn = fn.strip().lower()
        ln = ln.strip().lower()
        
        first_names = [fn]
        if fn.find('-') != -1:
            first_names += fn.split('-')
        
        last_names = [ln]
        if ln.find('-') != -1:
            last_names += ln.split('-')
        pt_mrn_hashes[mrn] = hashlib.sha224(str(mrn).encode('utf-8')).hexdigest()
        for fn_comp in first_names:
            for ln_comp in last_names:
                encrypted_row = map(lambda x: hashlib.sha224(str(x).encode('utf-8')).hexdigest(), [mrn, fn_comp, ln_comp, phone, zipcode])
                encrypted.append(encrypted_row)

    tgt_df = pd.DataFrame(encrypted, columns=exp_header)

    tgt_folder = Path(file_dir + '/EncryDemogData')
    try:
        os.mkdir(tgt_folder)
    except FileExistsError:
        pass

    enc_pt_fn = Path(tgt_folder / (file_name + "_enc" + file_ext))
    tgt_df.to_csv(enc_pt_fn, index=False)

    #writing "Encrypted/AllPatients_DummyTable_map.csv"

    map_df = pd.DataFrame(pt_mrn_hashes.items(), columns=['mrn', 'hashed_mrn'])
    map_fn = Path(tgt_folder / (file_name + "_map" + file_ext))
    map_df.to_csv(map_fn, index=False)
    return enc_pt_fn, map_fn
