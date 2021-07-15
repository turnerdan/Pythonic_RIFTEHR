"""
PURPOSE: This script iterates through the matches table and marks those relationships (or sets of relationships) that are in conflict.
    For example, if Patient_1 is related to Patient_2 as Spouse, but Patient_2 is related back to Patient_1 as brother, then this is a conflict.
    This script divides up the table by pairings of patients, then marks those pairings that have conflicting relationships.
OUTPUT: `matched_inferred_conflicts.csv`
"""

import pandas as pd
import numpy as np
from pathlib import Path
from __main__ import *

conflict_count = 0

def find_conflicts(inferred_df, main_inputs_path):

    df = inferred_df.copy()
 
    # For the conflicts to be properly identified, each pairing needs to be in the same order, so we flip MRNs temporarily
    df['flip'] = df.pt_mrn > df.matched_mrn
            
    # Flip pt_mrn and matched_mrn
    df.loc[df['flip'], ['pt_mrn', 'matched_mrn']] = df.loc[df['flip'], ['matched_mrn', 'pt_mrn']].values

    # Prepare flags for evaluate_group
    df['conflict'] = 0
    df['conflict_group'] = -1

    # Evaluate groups
    grp = df.groupby(by=['pt_mrn', 'matched_mrn'])
    df = grp.apply(evaluate_group)

    # Flip pt_mrn and matched_mrn back
    df.loc[df['flip'], ['pt_mrn', 'matched_mrn']] = df.loc[df['flip'], ['matched_mrn', 'pt_mrn']].values
    
    # Drop the flip column
    df = df.drop(columns=["flip"])

    return df

# Receives a subset of the larger dataframe that only contains one pairing of MRN's and one or more of their relationships
# Marks those subsets that have conflicts, for further investigation
def evaluate_group(df):
    global conflict_count
    if len(df) == 1:
        return df

    # Certain relationship pairings can be ruled out and ignored.
    if len(df) == 2:
        rel_set = set(df.ec_relation)
        if rel_set == {'spouse'}:
            return df
        elif rel_set == {'parent', 'child'}:
            return df
        elif rel_set == {'sibling'}:
            return df
        else:
            df.conflict = 1
            conflict_count = conflict_count + 1
            df['conflict_group'] = conflict_count
            return df

    # Two family members should not be related in more than 2 ways
    if len(df) > 2:
        df.conflict = 1
        conflict_count = conflict_count + 1
        df['conflict_group'] = conflict_count
        return df

    # The patient and match should have different identifiers
    df['conflict_count'] = np.where(df["pt_mrn"] == df["matched_mrn"], df["conflict_count"] + 1, df["conflict_count"])
        
    
# Check whether children are younger than their parents
# Marks those subsets that have conflicts, for further investigation
def process_age(inferred_df, main_inputs_path):

    df = inferred_df.copy()

    demographics = pd.read_csv(patients_file_path, dtype=str).replace(np.nan, '') # read in the primary data to extract demographics

    demographics = demographics[demographics.columns[demographics.columns.isin(['MRN', 'Sex', 'age'])]] # take mrn sex, and age from pt input as gender_df
    pt_demographics = demographics.rename(columns={'MRN' : 'pt_mrn', 'age' : 'pt_age', 'Sex' : 'pt_sex'}) # change the colname to drop the key col on merge
    ec_demographics = demographics.rename(columns={'MRN' : 'matched_mrn', 'age' : 'matched_age', 'Sex' : 'matched_sex'})
    
    # Merge 
    df = df.merge(pt_demographics, how = 'left') # add pt_sex and pt_age
    df = df.merge(ec_demographics, how = 'left') # add matched_sex and matched_age
 
    # Convert age cols form string to float
    # convert_dict = {'pt_age':float,'matched_age':float} # convert from string to float
    # df = df.astype(convert_dict)
    
    # df['pt_age'] = df['pt_age'].astype(float)
    # df['matched_age'] = df['pt_age'].astype(float)
    
    # new 6-15-21 dt
    df['pt_age'] = pd.to_numeric(df['pt_age'], errors='coerce')
    df['matched_age'] = pd.to_numeric(df['matched_age'], errors='coerce')
    
    # Calculate the difference
    df["age_diff"] = df["pt_age"] - df["matched_age"] # do the math

    # Flip pt_mrns and match_mrns for children-relative relationships so child is younger (fixes directionality errors)

    # Define the groups we'll flip
    inferred_younger = ['child', 'grandchild', 'great-grandchild', 'great-great-grandchild']
    inferred_older = ['parent', 'grandparent', 'great-grandparent', 'great-great-grandparent']

    # Flag ec_relations so X-children are always younger (negative #) than their X-parent (positive #)
    df['age_flipped'] = False       # By default we do not flip any rows
    df['age_flipped'] = np.where((df.age_diff < 0 ) & (df['ec_relation'].isin(inferred_younger)), True, df['age_flipped'])
    df['age_flipped'] = np.where((df.age_diff > 0 ) & (df['ec_relation'].isin(inferred_older)), True, df['age_flipped'])
    print("Flipping ", sum(df['age_flipped']), " relationships.")
    
    # Flip based on the boolean in age_flipped
    df.loc[df['age_flipped'], ['pt_mrn', 'matched_mrn']] = df.loc[df['age_flipped'], ['matched_mrn', 'pt_mrn']].values

    # Compare age_diff within each ec_relation
    
    # child-parent (+/- 10yr)
    child_parent_ec = ['child', 'parent']
    child_parent_conflicts = df.loc[df['ec_relation'].isin(child_parent_ec) & df['age_diff'].between(-10, 10, inclusive = True)]
    
    # grandchild-grandparent (+/- 20yr)
    grandchild_grandparent_ec = ['grandchild', 'grandparent']
    grandchild_grandparent_conflicts = df.loc[df['ec_relation'].isin(grandchild_grandparent_ec) & df['age_diff'].between(-20, 20, inclusive = True)]
    
    # great_grandchild-grandparent (+/- 30yr)
    great_grandchild_grandparent_ec = ['great-grandchild', 'great-grandparent']
    great_grandchild_grandparent_conflicts = df.loc[df['ec_relation'].isin(great_grandchild_grandparent_ec) & df['age_diff'].between(-30, 30, inclusive = True)]
    
    # great_great_grandchild-grandparent (+/- 40yr)
    great_great_grandchild_grandparent_ec = ['great-great-grandchild', 'great-great-grandparent']
    great_great_grandchild_grandparent_conflicts = df.loc[df['ec_relation'].isin(great_great_grandchild_grandparent_ec) & df['age_diff'].between(-40, 40, inclusive = True)]
    
    # spouse (flag < 17yr spouses)
    spouse_conflicts = df.loc[df['ec_relation'].isin(['spouse']) & df[['pt_age', 'matched_age']].between(1, 16, inclusive = True)]
    
    # Join all the groups of conflicts that we identified
    age_conflicts = pd.DataFrame().append([child_parent_conflicts, grandchild_grandparent_conflicts, great_grandchild_grandparent_conflicts, great_great_grandchild_grandparent_conflicts, spouse_conflicts])
    
    # Add conflict flag value for all rejected matches
    age_conflicts['age_conflict'] = int(1)
    
    # Transfer the flags to the main dataset, which now will have the above column 
    df = pd.merge(df, age_conflicts, how='left')
    df['age_conflict'] = df['age_conflict'].fillna(0) # turn NA's to 0's for the new column
    
    # If no conflict, mark 0 (better than NaN)
    df['age_conflict'] = df['age_conflict'].replace(np.nan, 0)

    return df
