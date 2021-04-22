"""
INPUT: Dataframe output from Step 1
PURPOSE: This script iterates through the matches and relationships from Step 1, looking for additional relationships it can infer.
OUTPUT: `matched_table_inferred.csv` - A table of patients linked by relationship, with added inferred relationships.
"""

from collections import defaultdict
from pathlib import Path
from __main__ import * # DT

import pandas as pd

def tuple_contains(test_relationship, test_mrn, pair_to_compare):
    test_relationship: str
    test_mrn: int
    for w in range(len(pair_to_compare)):
        if test_relationship == pair_to_compare[w][0]:
            if test_mrn == pair_to_compare[w][1]:
                 return True
    return False

def infer_relationships(input_df, main_inputs_path):
    #Read file into dataframe
    df = input_df

    #Reading in the df as a dictionary
    matches_dict = defaultdict(list)
    for index, row in df.iterrows():
        matches_dict[row['pt_mrn']].append((row['ec_relation'], row['matched_mrn'], 0))

    b = 0
    while True:
        mc = matches_dict
        a = 0
        b += 1
        for pt_mrn in mc.keys():
            for pt_relation in mc[pt_mrn]:
                if pt_relation[1] in mc.keys():  # If pt's relative's mrn is also in our keys
                    for rel_relation in mc[pt_relation[1]]:
                        if rel_relation[1] == pt_mrn:  # We won't infer relationships from the individual to themselves
                            continue
                        if pt_relation[0] == "parent":
                            if rel_relation[0] == "sibling":
                                if tuple_contains("aunt/uncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("aunt/uncle", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "aunt/uncle":
                                if tuple_contains("grandaunt/granduncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandaunt/granduncle", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "child":
                                if tuple_contains("sibling", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("child/nephew/niece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("child/nephew/niece", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("great-grandparent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandparent", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("cousin", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("cousin", rel_relation[1], b))
                                    a += 1
                            elif rel_relation[0] == "parent":
                                if tuple_contains("grandparent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandparent", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "child":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("sibling/sibling-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling/sibling-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("grandchild", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandchild", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("great-grandchild", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandchild", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("parent/parent-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("parent/parent-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("grandchild/grandchild-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandchild/grandchild-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("spouse", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("spouse", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("child", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("child", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "sibling":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("aunt/uncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("aunt/uncle", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("nephew/niece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("nephew/niece", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("grandnephew/grandniece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandnephew/grandniece", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("grandparent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandparent", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("child/nephew/niece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("child/nephew/niece", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("parent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("parent", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("sibling", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "aunt/uncle":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("grandaunt/granduncle/grandaunt-in-law/granduncle-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandaunt/granduncle/grandaunt-in-law/granduncle-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("cousin", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("cousin", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("first cousin once removed", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("first cousin once removed", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("great-grandparent/great-grandparent-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandparent/great-grandparent-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("sibling/cousin", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling/cousin", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("grandparent/grandparent-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandparent/grandparent-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("parent/aunt/uncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("parent/aunt/uncle", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "grandchild":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("child/child-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("child/child-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("great-grandchild", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandchild", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("great-great-grandchild", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-great-grandchild", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("spouse", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("spouse", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("great-grandchild/great-grandchild-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandchild/great-grandchild-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("child/child-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("child/child-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("grandchild", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandchild", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "grandparent":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("great-grandaunt/great-granduncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandaunt/great-granduncle", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("parent/aunt/uncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("parent/aunt/uncle", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("sibling/cousin", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling/cousin", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("great-great-grandparent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-great-grandparent", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("first cousin once removed", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("first cousin once removed", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("great-grandparent", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandparent", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("grandaunt/granduncle", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandaunt/granduncle", rel_relation[1], b))
                                    a += 1

                        if pt_relation[0] == "nephew/niece":
                            if rel_relation[0] == "aunt/uncle":
                                if tuple_contains("sibling/sibling-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling/sibling-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "child":
                                if tuple_contains("grandnephew/grandniece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandnephew/grandniece", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandchild":
                                if tuple_contains("great-grandnephew/great-grandniece", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("great-grandnephew/great-grandniece", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "grandparent":
                                if tuple_contains("parent/parent-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("parent/parent-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "nephew/niece":
                                if tuple_contains("grandnephew/grandniece/grandnephew-in-law/grandniece-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("grandnephew/grandniece/grandnephew-in-law/grandniece-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "parent":
                                if tuple_contains("sibling/sibling-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("sibling/sibling-in-law", rel_relation[1], b))
                                    a += 1

                            elif rel_relation[0] == "sibling":
                                if tuple_contains("nephew/niece/nephew-in-law/niece-in-law", rel_relation[1], mc[pt_mrn]) == False:
                                    mc[pt_mrn].append(("nephew/niece/nephew-in-law/niece-in-law", rel_relation[1], b))
                                    a += 1
        if a == 0:
            break

    L = [(k, *t) for k, v in matches_dict.items() for t in v]
    df = pd.DataFrame(L, columns=['pt_mrn','ec_relation','matched_mrn', 'inferred'])
    
    if not skip_writing:
        # DT: only relevant for the current itteration. to test output, you should join the different itterations first. 
        # output_fn = Path(main_inputs_path / ("matched_inferred.csv"))
        # df.to_csv(output_fn, index=False)
        pass

    return df

