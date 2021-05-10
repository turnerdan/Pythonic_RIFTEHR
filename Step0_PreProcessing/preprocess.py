import os
from pathlib import Path
from __main__ import * # DT

import numpy as np
import pandas as pd


# For the purposes of this project, you should always read_csv's with `dtype=str`
# This is to avoid pandas trying to interpret mrn's as int dtypes, which while more efficient, will complicate some scripts

# DT added Age, Sex

def preprocess(pt_fp, ec_fp, relmap_fp):

    # Collect paths for later use
    file_dir, pt_file_name = os.path.split(pt_fp)
    pt_file_name, file_ext = os.path.splitext(pt_file_name)
    file_dir, ec_file_name = os.path.split(ec_fp)
    ec_file_name, file_ext = os.path.splitext(ec_file_name)
    
    # LOADING DATA
    
    # PATIENT DATA

    # Read in Pt Demographic Data, Confirm it appears as expected, sort by MRN
    pt_df = pd.read_csv(pt_fp, dtype=str).replace(np.nan, '') # Here's a Nana
    exp_header_pt = ['MRN', 'FirstName', 'LastName', 'PhoneNumber', 'Zipcode', 'age', 'Sex'] # expected header for pt data 
    
    # Prepare PT data
    # pt_df.drop(pt_df.columns[[0,1]], axis=1, inplace=True) # Drop blank cols (specific to this dataset)
    pt_df = pt_df.rename(columns={"age": "Age"}) # DT change colnames if needed (specific to this dataset)

    # Check the header is as expected
    # assert (pt_df.columns == exp_header_pt).all(), "Patient demographic data file (%s) doesn't have the header expected: %s" % (pt_fp, exp_header_pt)
    pt_df = pt_df.sort_values(by="MRN", ascending=True)

    # EMERGENCY CONTACT DATA
    # Read in EC Data, Confirm it appears as expected, sort by MRN
    ec_df = pd.read_csv(ec_fp, dtype=str).replace(np.nan, '') # another Nana

    # Fixing the file colnames
    # ec_df.drop(ec_df.columns[[0]], axis=1, inplace=True) # Drop blank cols (specific to this dataset)
    ec_df = ec_df.rename(columns={"age": "Age"}) # DT change colnames if needed (specific to this dataset)

    exp_header_ec = ['MRN_1', 'EC_LastName', 'EC_FirstName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'age', 'Sex']

    # Same as above^
    # assert (ec_df.columns == exp_header_ec).all(), "Emergency contact data file (%s) doesn't have the header expected: %s" % (ec_fp, exp_header_ec)

    ec_df = ec_df.sort_values(by="MRN_1", ascending=True)
    
    
    
    # PREPROCESSING

    # Pre-process first name columns
    print("Pre-processing First and Last Name columns")
    pt_df.FirstName = process_fn(pt_df.FirstName)
    ec_df.EC_FirstName = process_fn(ec_df.EC_FirstName)

    # Pre-process last name columns
    pt_df.LastName = process_ln(pt_df.LastName)
    ec_df.EC_LastName = process_ln(ec_df.EC_LastName)

    # Pre-process Phone Number Columns
    print("Pre-processing Phone Number columns")
    pt_df.PhoneNumber = process_phones(pt_df.PhoneNumber)
    ec_df.EC_PhoneNumber = process_phones(ec_df.EC_PhoneNumber)

    # Pre-process Zip Codes
    print("Pre-processing Zip Codes")
    pt_df.Zipcode = process_zips(pt_df.Zipcode)
    ec_df.EC_Zipcode = process_zips(ec_df.EC_Zipcode)
    
    print("samp3\n", ec_df.head() ) #testing

    # Pre-process the Relationships in ec_df
    print("Pre-processing EC Relationships")
    ec_df.EC_Relationship = process_relations(ec_df.EC_Relationship, relmap_fp)

    # If hyphens ('-') are in either the first name or last name, split the name and create permutations.
    print("Patient DF Name splitting")

    # Eg, if name is Robert Smith-Johnson, create records for "Robert Smith-Johnson", "Robert Smith", and "Robert Johnson".
    ## pt_df first
    pt_df = pt_df.join(pt_df['FirstName'].str.split("-", expand=True), how="left")
    pt_df = pt_df.drop(columns="FirstName")
    pt_df = pt_df.melt(id_vars=['MRN', 'LastName', 'PhoneNumber', 'Zipcode', 'Age', 'Sex'])
    pt_df = pt_df.dropna(subset=["value"])
    pt_df = pt_df.rename(columns={'value':'FirstName'})
    pt_df = pt_df[['MRN', 'FirstName', 'LastName', 'PhoneNumber', 'Zipcode', 'Age', 'Sex']]

    pt_df = pt_df.join(pt_df['LastName'].str.split("-", expand=True), how="left")
    pt_df = pt_df.drop(columns="LastName")
    pt_df = pt_df.melt(id_vars=['MRN', 'FirstName', 'PhoneNumber', 'Zipcode', 'Age', 'Sex'])
    pt_df = pt_df.dropna(subset=["value"])
    pt_df = pt_df.rename(columns={'value': 'LastName'})
    pt_df = pt_df[['MRN', 'FirstName', 'LastName', 'PhoneNumber', 'Zipcode', 'Age', 'Sex']]
    pt_df = pt_df.sort_values(by=['MRN', 'LastName', 'FirstName', 'PhoneNumber', 'Zipcode', 'Age', 'Sex'])


    ## ec_df second
    print("EC DF Name splitting")
    ec_df = ec_df.join(ec_df['EC_FirstName'].str.split("-", expand=True), how="left")
    ec_df = ec_df.drop(columns="EC_FirstName")
    ec_df = ec_df.melt(id_vars=['MRN_1', 'EC_LastName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'Age', 'Sex'])
    ec_df = ec_df.dropna(subset=["value"])
    ec_df = ec_df.rename(columns={'value': 'EC_FirstName'})
    ec_df = ec_df[['MRN_1', 'EC_FirstName', 'EC_LastName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'Age', 'Sex']]

    ec_df = ec_df.join(ec_df['EC_LastName'].str.split("-", expand=True), how="left")
    ec_df = ec_df.drop(columns="EC_LastName")
    ec_df = ec_df.melt(id_vars=['MRN_1', 'EC_FirstName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'Age', 'Sex'])
    ec_df = ec_df.dropna(subset=["value"])
    ec_df = ec_df.rename(columns={'value': 'EC_LastName'})
    ec_df = ec_df[['MRN_1', 'EC_FirstName', 'EC_LastName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'Age', 'Sex']]
    ec_df = ec_df.sort_values(by=['MRN_1', 'EC_LastName', 'EC_FirstName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship', 'Age', 'Sex'])

    # New paths
    new_pt_file_path = Path(Path(file_dir) / ("preprocessed_" + pt_file_name + ".csv"))
    new_ec_file_path = Path(Path(file_dir) / ("preprocessed_" + ec_file_name + ".csv"))

    # Write out the results to inputs/Preprocessed
    if not skip_writing:
        print("Writing out pre-processing results")
        pt_df.to_csv(new_pt_file_path, index=False)
        ec_df.to_csv(new_ec_file_path, index=False)
    else:
        print("Skipping writing results")

    return new_pt_file_path, new_ec_file_path

def process_fn(df_column):
    return df_column.str.strip().str.lower()  # Trim whitespace and make lowercase

def process_ln(df_column):
    return df_column.str.strip().str.lower()  # Trim whitespace and make lowercase

def process_phones(df_column):
    the_column = df_column.copy().str.lower()
    the_column = the_column.apply(lambda x: x.rsplit(sep="x", maxsplit=1)[0] if "x" in x else x).str.strip()  # Dropped extensions of the form 'x1234'
    the_column = the_column.replace(".*000-0000", "", regex=True)
    the_column = the_column.replace('.*999-9999', '', regex=True)
    the_column = the_column.replace('111-111-1111', '')
    the_column = the_column.replace('222-222-2222', '')
    the_column = the_column.replace('333-333-3333', '')
    the_column = the_column.replace('444-444-4444', '')
    the_column = the_column.replace('555-555-5555', '')
    the_column = the_column.replace('666-666-6666', '')
    the_column = the_column.replace('777-777-7777', '')
    the_column = the_column.replace('888-888-8888', '')
    the_column = the_column.replace('312-926-2000', '')  # Northwestern University main line
    the_column = the_column.replace('312-312-3123', '')
    the_column = the_column.str.replace(r'\s', '')  #Remove all internal white space
    #the_column = the_column.str.replace("+", "", regex=False)  # I assume international numbers are the ones with "+" signs

    if the_column.str.contains("[A-z]").any():
        print("This PhoneNumber column has non numeric characters")
        print(the_column.loc[the_column.str.contains("[+A-z]")])

    return the_column

def process_zips(df_column):
    the_column = df_column.copy()
    the_column = the_column.apply(lambda x: x.split("-")[0])
    the_column = the_column.str.replace(r'\s', '')  #Remove all internal white space

    if the_column.str.contains("[A-z]").any():
        print("This Zip column has non numeric characters")
        print(the_column.loc[the_column.str.contains("[A-z]")])
    return the_column

# Your hospital's/dataset's relationship value set may differ from those used in the project:
# Providing a relation_map is necessary to ensure relationship identification can occur

def process_relations(df_column, relmap_fp):
    relmap_df = pd.read_csv(relmap_fp, dtype=str)
    relation_map = {a.input_relation: a.output_relation for ix, a in relmap_df.iterrows()}

    the_column = df_column.map(relation_map)
    return the_column

