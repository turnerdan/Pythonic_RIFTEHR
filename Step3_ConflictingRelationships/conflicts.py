"""
PURPOSE: This script iterates through the matches table and marks those relationships (or sets of relationships) that are in conflict.
    For example, if Patient_1 is related to Patient_2 as Spouse, but Patient_2 is related back to Patient_1 as brother, then this is a conflict.
    This script divides up the table by pairings of patients, then marks those pairings that have conflicting relationships.
OUTPUT: `matched_inferred_conflicts.csv`
"""

import pandas as pd
import numpy as np
from pathlib import Path
from __main__ import * # DT

conflict_count = 0

def find_conflicts(inferred_df, main_inputs_path):

    df = inferred_df.copy()
 
    # For the conflicts to be properly identified, each pairing needs to be in the same order, so we flip MRNs temporarily
    df['flip'] = df.pt_mrn > df.matched_mrn
    for ix in range(len(df)):
        if df.loc[ix, 'flip']:
            tmp = df.loc[ix, 'pt_mrn']
            df.loc[ix, 'pt_mrn'] = df.loc[ix, 'matched_mrn']
            df.loc[ix, 'matched_mrn'] = tmp

    df['conflict'] = 0
    df['conflict_group'] = -1

    grp = df.groupby(by=['pt_mrn', 'matched_mrn'])
    df = grp.apply(evaluate_group)

    # Flip pairings back
    for ix in range(len(df)):
        if df.loc[ix, 'flip']:
            tmp = df.loc[ix, 'pt_mrn']
            df.loc[ix, 'pt_mrn'] = df.loc[ix, 'matched_mrn']
            df.loc[ix, 'matched_mrn'] = tmp

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

    
    
# Check whether children are younger than their parents
# Marks those subsets that have conflicts, for further investigation
def process_age(inferred_df, main_inputs_path):

    df = inferred_df.copy()

    # Read-in age data
    age_df = pd.read_csv(age_file_path, dtype=str).replace(np.nan, '') # replace NAs
    age_df.columns = ['row', 'pt_mrn', 'pt_age'] # first we set the colname for pt
    
    # Left join to add age for pt_mrn
    df = df.merge(age_df, on='pt_mrn', how='left') # merge to match
    
    # Left join to add age for matched_mrn
    age_df.columns = ['row', 'matched_mrn', 'matched_age'] # just for convenient merge
    df = df.merge(age_df, on='matched_mrn', how='left')
    
    # Calculate the difference
    df = df.apply(pd.to_numeric, errors='ignore') # convert strings to numerics to do math
    df["age_diff"] = df["pt_age"] - df["matched_age"]
    
    # Assign the calculated column to the incoming df, so original datatypes are preserved
    df = inferred_df.assign(age_diff = df["age_diff"]) 
    
    # Flip pt_mrns and match_mrns for children-relative relationships so child is younger (fixes directionality errors)

    # Define the groups we'll flip
    inferred_younger = ['child', 'grandchild', 'great-grandchild', 'great-great-grandchild']
    inferred_older = ['parent', 'grandparent', 'great-grandparent', 'great-great-grandparent' ]

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
    
    # Join all the groups of conflicts that we identified
    age_conflicts = pd.DataFrame().append([child_parent_conflicts, grandchild_grandparent_conflicts, great_grandchild_grandparent_conflicts, great_great_grandchild_grandparent_conflicts])
    
    # Add conflict flag value for all rejected matches
    age_conflicts['age_conflict'] = 1
    
    # Transfer the flags to the main dataset, which now will have the above column 
    df = pd.merge(df, age_conflicts, how='left')

    return df
