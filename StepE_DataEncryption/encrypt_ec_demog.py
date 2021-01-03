"""
Perform some basic quality checks and encrypt the emergency contact demographic data file.

USAGE
Use file containing emergency contact demographic data.

RETURNS
1. Returns encrypted emergency contact demographic data in file "(directory where emergency contact data file exist)/EmergencyContact_DummyTable_enc.csv"
2. Returns hash map mapping MRN values to there encrypted values in file "(directory where emergency contact data file exist)/EmergencyContact_DummyTable_map.csv"
"""


import hashlib
import os
from pathlib import Path

import pandas as pd


def encrypt_ec_demog_data(file_path): # file_path => file containing emergrency contact data

    # Collect paths for later use
    file_dir, file_name = os.path.split(file_path)
    file_name, file_ext = os.path.splitext(file_name)

    # read in the emergency contact data from file
    pt_df = pd.read_csv(file_path)

    h = pt_df.columns.to_list()
    exp_header = ['MRN_1', 'EC_FirstName', 'EC_LastName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship']

    if not h == exp_header:
        raise Exception("Patient emergency contact data file (%s) doesn't have the header expected:%s" % (file_path, exp_header))

    # writing "EmergencyContact_DummyTable_enc.csv" file
    ec_mrn_hashes = dict()
    encrypted = []

    for i , (mrn, fn, ln, phone, zipcode, rel) in pt_df.iterrows():
        fn = fn.strip().lower()
        ln = ln.strip().lower()
    
        first_names = [fn]
        if fn.find('-') != -1:
            first_names += fn.split('-')
    
        last_names = [ln]
        if ln.find('-') != -1:
            last_names += ln.split('-')
    
        ec_mrn_hashes[mrn] = hashlib.sha224(str(mrn).encode('utf-8')).hexdigest()
    
        for fn_comp in first_names:
            for ln_comp in last_names:
                encrypted_row = map(lambda x: hashlib.sha224(str(x).encode('utf-8')).hexdigest(), [mrn, fn_comp, ln_comp, phone, zipcode])
                encrypted.append(list(encrypted_row) + [rel])

    tgt_df = pd.DataFrame(encrypted, columns=exp_header)

    tgt_folder = Path(file_dir + '/EncryDemogData')

    enc_ec_fn = Path(tgt_folder / (file_name + "_enc" + file_ext))
    tgt_df.to_csv(enc_ec_fn, index=False)

    # writing "EmergencyContact_DummyTable_map.csv" file

    map_df = pd.DataFrame(ec_mrn_hashes.items(), columns=['mrn', 'hashed_mrn'])
    map_fn = Path(tgt_folder / (file_name + "_map" + file_ext))
    map_df.to_csv(map_fn, index=False)
    return enc_ec_fn, map_fn
