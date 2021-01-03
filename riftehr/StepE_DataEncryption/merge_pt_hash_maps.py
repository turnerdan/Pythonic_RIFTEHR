"""
Encrypting the patient data produces two hash maps (one for all patients, one for emergency contacts), this script
merges them into one hash map. 

USAGE
1. Use file containing patient data hash map 
2. Use file containing emergency contact hash map 

RETURNS
Returns merged hash map in file "(directory containing patient data hash map file and emergency contact hash map file)/merged_map.csv"
"""

import os
from pathlib import Path

import pandas as pd


def merge_hash_maps(ec_map_fn, pt_map_fn):

    data_dir = os.path.dirname(pt_map_fn)

    # read in the patient demographics map data from "AllPatients_DummyTable_map.csv"
    pt_map_df = pd.read_csv(pt_map_fn)

    h = pt_map_df.columns.to_list()
    exp_header = ['mrn', 'hashed_mrn']

    if not h == exp_header:
        raise Exception("Patient demographics map data file (%s) doesn't have the header expected:%s" % (pt_map_fn, exp_header))

    hashed_data = dict()
    for i, (mrn, hashed_mrn) in pt_map_df.iterrows():
        hashed_data[mrn] = hashed_mrn

    # read in the emergency contacts map data from "../DummyFiles/EmergencyContact_DummyTable_map.csv"

    ec_map_df = pd.read_csv(ec_map_fn, dtype = str)
    h = ec_map_df.columns.to_list()

    if not h == exp_header:
        raise Exception("Emergency contacts map data file (%s) doesn't have the header expected:%s" % (ec_map_fn, exp_header))

    for i, (mrn, hashed_mrn) in ec_map_df.iterrows():
        if mrn not in hashed_data:
            hashed_data[mrn] = hashed_mrn

    #write merged hashed map in "../DummyFiles/merged_map.csv"
    merged_df = pd.DataFrame(hashed_data.items(), columns=['mrn', 'hased_mrn'])
    merged_fn = Path(data_dir + "merged_map.csv")
    merged_df.to_csv(merged_fn, index=False)
    return merged_fn
