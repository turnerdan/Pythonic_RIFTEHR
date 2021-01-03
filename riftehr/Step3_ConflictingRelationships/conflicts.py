"""
PURPOSE: This script iterates through the matches table and marks those relationships (or sets of relationships) that are in conflict.
    For example, if Patient_1 is related to Patient_2 as Spouse, but Patient_2 is related back to Patient_1 as brother, then this is a conflict.
    This script divides up the table by pairings of patients, then marks those pairings that have conflicting relationships.
OUTPUT: `matched_inferred_conflicts.csv`
"""

import pandas as pd
import numpy as np
from pathlib import Path

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

    output_fn = Path(main_inputs_path / ("matched_inferred_conflicts.csv"))
    df.to_csv(output_fn, index=False)

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
