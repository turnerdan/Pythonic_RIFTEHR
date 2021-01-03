"""
PURPOSE: Use graph theory packages to identify disconnected subgraphs of the inferred relationship.
    Each disconnected subgraph is called a "family." Each family is assigned a single identifer.
OUTPUT: "final_matches_and_families.csv"

Authors: Fernanda Polubriaginof and Nicholas Tatonetti
"""
from pathlib import Path

import networkx as nx
import pandas as pd

# Todo: Clean up this function, Inefficient as written
def familyLinkage(input_df, main_inputs_path):

    # Read Infere_Realtionship file
    reader = input_df

    a=[]
    b=[]
    rel=[]
    all_relationships = []
    for i, row in reader.iterrows():
        a.append(row['pt_mrn'])
        b.append(row['matched_mrn'])
        rel.append(row['ec_relation'])

    for i in range(len(a)):
        all_relationships.append(tuple([a[i], b[i], rel[i]]))


    u = nx.Graph() #directed graph

    for i in range(len(all_relationships)):
        u.add_edge(all_relationships[i][0], all_relationships[i][1], rel = all_relationships[i][2])

    # Components sorted by size
    comp = sorted(nx.connected_components(u), key=len, reverse=True)


    data = []
    for family_id in range(len(comp)):
        for individual_id in comp[family_id]:
            data.append([family_id, individual_id])


    output_fn = Path(main_inputs_path / "familyLinkage.csv")

    df = pd.DataFrame(data, columns=['family_id', 'individual_id'])
    df.to_csv(output_fn, index=False)

    # Now reconnect familyIDs and place them in the inferred_relationships file
    df = df.set_index('individual_id').sort_index()
    input_df['pt_fam_id'] = input_df['pt_mrn'].apply(lambda x: df.loc[x]['family_id'])
    input_df['match_fam_id'] = input_df['matched_mrn'].apply(lambda x: df.loc[x]['family_id'])

    if input_df.pt_fam_id.equals(input_df.match_fam_id):
        input_df = input_df.drop(columns="match_fam_id")
        input_df = input_df.rename(columns={"pt_fam_id": "family_id"})
    else:
        assert input_df.pt_fam_id.equals(input_df.match_fam_id), "Critical Error: There is a patient and Matched MRN who were assigned different families."

    input_df.to_csv(main_inputs_path / "final_matches_and_families.csv", index=False)

    return output_fn
