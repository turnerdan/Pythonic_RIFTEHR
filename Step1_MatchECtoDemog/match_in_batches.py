"""
INPUT: Paths to Preprocessed Patient Demographics (Pt) and Emergency Contact (EC) Tables
PURPOSE: This script iterates through the EC table, searching the Pt table for patients who meet the description of the Emergency Contact.
    In other words, looks for ECs who are also Patients.
OUTPUT: `matched.csv` - A table of Patients linked by a relationship.
"""

from collections import defaultdict
from pathlib import Path

import pandas as pd

class BatchMatcher:
    def __init__(self, main_input_path, input_preprocessed_pt_fp, input_preprocessed_ec_fp, skip_hashing=False):
        self.main_input_path = main_input_path
        self.input_pt_fp = input_preprocessed_pt_fp
        self.input_ec_fp = input_preprocessed_ec_fp
        self.skip_hashing = skip_hashing

    def run(self):

        pt_df = pd.read_csv(self.input_pt_fp, dtype=str)
        assert (pt_df.columns == ['MRN', 'FirstName', 'LastName', 'PhoneNumber', 'Zipcode']).all(), "Step 1: The input Patient Dataframe has column names that are unexpected or out of order."
        ec_df = pd.read_csv(self.input_ec_fp, dtype=str)
        assert (ec_df.columns == ['MRN_1', 'EC_FirstName', 'EC_LastName', 'EC_PhoneNumber', 'EC_Zipcode', 'EC_Relationship']).all(), "Step 1: The input Emergency Contact Dataframe has column names that are unexpected or out of order."

        # Hashes are a much faster for searching.
        # Each hashmap/dictionary represents a way of accessing a whole patient record using just one identifier:
        # Ie, first_hash is a way to search for patients by first names: key:value pairs are as follows:
        #   <first_name> : {MRN's of all patients that share that <first_name>}
        def_dict = defaultdict(set)
        firstname_hash = pt_df.groupby('FirstName')['MRN'].apply(set).to_dict(def_dict)
        lastname_hash = pt_df.groupby('LastName')['MRN'].apply(set).to_dict(def_dict)
        phone_hash = pt_df.groupby('PhoneNumber')['MRN'].apply(set).to_dict(def_dict)
        zip_hash = pt_df.groupby('Zipcode')['MRN'].apply(set).to_dict(def_dict)

        # We then use the generated hashmaps to conduct our match searches right inside of the dataframe.
        # Each dataframe row will then have 4 (Sets) of values that coincide with the original Emergency Contact Identifiers
        # This allows us to then calculate intersections of those (Sets) to zero in on unique matches.
        ec_df.EC_FirstName = ec_df.EC_FirstName.map(firstname_hash)
        ec_df.EC_LastName = ec_df.EC_LastName.map(lastname_hash)
        ec_df.EC_PhoneNumber = ec_df.EC_PhoneNumber.map(phone_hash)
        ec_df.EC_Zipcode = ec_df.EC_Zipcode.map(zip_hash)

        # Remove each Patient MRN from their own hashmaps (ie their own row's hashmaps of Firstnames, Lastnames, etc)
        ec_df.EC_FirstName = [b.difference(a) for a, b in zip(ec_df['MRN_1'].apply(lambda x: {x}), ec_df.EC_FirstName)]
        ec_df.EC_LastName = [b.difference(a) for a, b in zip(ec_df['MRN_1'].apply(lambda x: {x}), ec_df.EC_LastName)]
        ec_df.EC_PhoneNumber = [b.difference(a) for a, b in zip(ec_df['MRN_1'].apply(lambda x: {x}), ec_df.EC_PhoneNumber)]
        ec_df.EC_Zipcode = [b.difference(a) for a, b in zip(ec_df['MRN_1'].apply(lambda x: {x}), ec_df.EC_Zipcode)]

        # Begin matching
        # Note: Originally planned to use parallel processing, however when I did so, my speed improvements were minimal.
        # For now, the code is written such that you may wrap the below line with a parallelizer of some form. (Multiprocessing Library, etc)
        # If a future programmer wants to re-try re-parallelizing this, just feed index subsets to the find_matches() function
        # and append the results as they return. Indexes are [inclusive_start:inclusive_end]. See find_matches() below for additional details.
        matches_df = find_matches(ec_df, 0, len(ec_df)-1)

        matches_df = matches_df.rename(columns={'MRN_1': 'pt_mrn',
                                                'EC_Relationship': 'ec_relation',
                                                'match': 'matched_mrn'})
        matches_df = matches_df[matches_df['ec_relation'] != ""]
        matches_df = matches_df[matches_df['matched_mrn'] != ""]
        matches_df = matches_df.dropna(how='any', subset=['ec_relation', 'matched_mrn'])
        matches_df = matches_df.drop_duplicates(keep='first')
        matches_df.to_csv(Path(self.main_input_path / 'matched.csv'), index=False)
        return matches_df

# This function vectorizes the matching process, running a whole column of matches at a time.
# It is also set up to be parallelized with subsetting, available through the `start` and `end` parameters.
# It uses `ec_df` which now holds (Sets) of matches as data elements, to calculate intersections of MRNs per row.
# There are a total of 4 iterations, each time requiring less identifier data elements to match.
# In the first iteration, we try strict matching by using all 4 of the original data elements (First Name, Last Name, etc).
# If a singular match is found, that row is removed from the following iterations.
# Next we try matching by using 3 of the data elements, then just 2. The option has been left to search by a single element as well.

# Parameters: <start>, <end> - Start and End Indexes of the total EC Dataframe to search through with this function.
# The option of indexing is for future developers who wish to explore parallelizing this function.
def find_matches(ec_df, start, end):

    subset_mask = ec_df.index.slice_indexer(start,end)  # The slice indexer is inclusive

    # Match all 4
    print("\tSubProcessor %s to %s" % (start, end), "Starting matching by 4 data elements.")
    ec_df.loc[subset_mask, 'match'] = [set.intersection(a, b, c, d) if len(set.intersection(a, b, c, d)) == 1
                                       else set() for a, b, c, d in
                                       zip(ec_df.loc[subset_mask, 'EC_LastName'],
                                           ec_df.loc[subset_mask, 'EC_FirstName'],
                                           ec_df.loc[subset_mask, 'EC_PhoneNumber'],
                                           ec_df.loc[subset_mask, 'EC_Zipcode'])]

    # Match by Three
    print("\tSubProcessor %s to %s" % (start, end), "Starting matching by 3 data elements.")
    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b, c) if len(set.intersection(a, b, c)) == 1
                                         else set() for a, b, c in
                                         zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                             ec_df.loc[no_match_mask, 'EC_FirstName'],
                                             ec_df.loc[no_match_mask, 'EC_PhoneNumber'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b, c) if len(set.intersection(a, b, c)) == 1
                                         else set() for a, b, c in
                                         zip(ec_df.loc[no_match_mask, 'EC_FirstName'],
                                             ec_df.loc[no_match_mask, 'EC_PhoneNumber'],
                                             ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b, c) if len(set.intersection(a, b, c)) == 1
                                         else set() for a, b, c in
                                         zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                             ec_df.loc[no_match_mask, 'EC_FirstName'],
                                             ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b, c) if len(set.intersection(a, b, c)) == 1
                                         else set() for a, b, c in
                                         zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                             ec_df.loc[no_match_mask, 'EC_PhoneNumber'],
                                             ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    # Match by Two
    print("\tSubProcessor %s to %s" % (start, end), "Starting matching by 2 data elements.")
    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                                                 ec_df.loc[no_match_mask, 'EC_FirstName'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_FirstName'],
                                                                 ec_df.loc[no_match_mask, 'EC_PhoneNumber'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                                                 ec_df.loc[no_match_mask, 'EC_PhoneNumber'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_PhoneNumber'],
                                                                 ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_FirstName'],
                                                                 ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [set.intersection(a, b) if len(set.intersection(a, b)) == 1
                                         else set() for a, b in zip(ec_df.loc[no_match_mask, 'EC_LastName'],
                                                                 ec_df.loc[no_match_mask, 'EC_Zipcode'])]

    """
    # Match by a single field
    print("\tSubProcessor %s to %s" % (start, end), "Starting matching by 1 data element.")
    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [a if len(a) == 1 else set() for a in
                                                 ec_df.loc[no_match_mask, 'EC_PhoneNumber']]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [a if len(a) == 1 else set() for a in
                                                 ec_df.loc[no_match_mask, 'EC_FirstName']]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [a if len(a) == 1 else set() for a in
                                                 ec_df.loc[no_match_mask, 'EC_LastName']]

    still_no_match = ec_df.loc[subset_mask, 'match'] == set()
    no_match_mask = still_no_match.loc[still_no_match == True].index
    ec_df.loc[no_match_mask, 'match'] = [a if len(a) == 1 else set() for a in
                                                 ec_df.loc[no_match_mask, 'EC_Zipcode']]
    """

    matches_subset = ec_df.loc[subset_mask, ['MRN_1', 'EC_Relationship', 'match']]
    matches_subset.loc[:, 'match'] = matches_subset.loc[:, 'match'].apply(lambda x: x.pop() if len(x) > 0 else "")
    print("\tSubProcessor %s to %s" % (start, end), "Finished")
    return matches_subset

