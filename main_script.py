# Central script to run RIFTEHR program.
# 
# USAGE:
#   -----
#   From Command Line:
#   python main_script.py <Absolute Path to Inputs Folder> <Patient_Demographics_Table_Name.csv> <Emergency_Contact_Table_Name.csv> <relation_map.csv> (--skip_preprocessing)
# 
# Optional Parameters:
#   --skip_preprocessing - If you have already preprocessed and would like to save time, you may set this field.
# You can then give paths to the preprocessed files instead when you call the script.
# 
# For example:
#   python "C:\Users\research user\input folder\" All_Patients.csv Emergency_Contacts.csv relation_map.csv
# 
# You may also define the default paths and variables below, and simply run the program as:
#     python main_script.py
# 
# AUTHORS:
# -------
# Original Version: Fernanda Polubriaginof and Nicholas Tatonetti
# This Version: Farhad Ghamsari


import os
import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path  # Aiming for cross Windows/Mac/Linux compatibility: we will use Python 3's pathlib for all paths


# Default Paths: These paths will be used if running from command line without supplied arguments, or if running main_script.py from within an IDE
# main_inputs_path contains your input_files and also serves as the output folder
# This folder can be separate from the folder which contains your code
main_inputs_path = Path("/Volumes/fsmresfiles/PrevMed/Projects/Family_Linkage")

# Paths to the input files themselves
patients_file_path = Path(main_inputs_path / "100K SAMPLE/100k_patientsample.csv")
emergency_contacts_file_path = Path(main_inputs_path / "100K SAMPLE/100k_emergencycontactsample.csv")
relation_map_file_path = Path(main_inputs_path / "relation_map.csv")

# Path to preprocessed files: If you have set the skip_preprocessing flag below to True, then ensure these paths are correct
preprocessed_pt_fp = Path(main_inputs_path / "preprocessed_To_useAllPatients_TableFinal_withConflicts.csv")
preprocessed_ec_fp = Path(main_inputs_path / "preprocessed_To_useEmergencyContact_TableFinal_withConflicts.csv")

# Flags
skip_preprocessing = False # Set this flag if you have already preprocessed once and would like to save some time
encrypt_first = False # Encryption is not currently implemented
skip_hashing = False # Not currently implemented
debugging = False # Not currently implemented
skip_writing = True # Set this flag True if you don't want to write any data (for testing purposes)

pt_df = pd.DataFrame()        # container for patient data
ec_df = pd.DataFrame()        # container for emergency contact data
id_conflicts = pd.DataFrame() # container for conflicting relationships

def the_work():
    global preprocessed_pt_fp
    global preprocessed_ec_fp

    # Begin Step 0 - PreProcessing
    time_step0 = time.time()

    import Step0_PreProcessing.preprocess as PreProcessor
    # Pre-process the dataframes - See script for steps.
    if not skip_writing and not skip_preprocessing: # paths not created if skip_writing is True
        preprocessed_pt_fp, preprocessed_ec_fp = PreProcessor.preprocess(patients_file_path, emergency_contacts_file_path, relation_map_file_path)

    # Todo: If ever you implement encryption, this is where it will go
    time_step1 = time.time()
    print("Time Taken for Step 0: ", time_step1 - time_step0)
    
    # Begin Step 1 - Identify matches across the patient and emergency_contact dataframes; Output goes to matched_table.csv
    from Step1_MatchECtoDemog.match_in_batches import BatchMatcher
    match_df = BatchMatcher(main_inputs_path, preprocessed_pt_fp, preprocessed_ec_fp, id_conflicts = None, skip_hashing = skip_hashing).run() # Pass #1

    time_step2 = time.time()
    print("Time Taken for Step 1: ", time_step2 - time_step1)

    # Begin Step 2 - Relationship Inference - Infer relationships from match_df
    import Step2_Relationship_Inference.Infer_Relationships as Infer_relationships
    inferred_df = Infer_relationships.infer_relationships(match_df, main_inputs_path)

    time_step3 = time.time()
    print("Time Taken for Step 2: ", time_step3 - time_step2)

    # Begin Step 3 - Identify Relationships in Conflict
    import Step3_ConflictingRelationships.conflicts as conflicts
    
    # 3.1 find_conflicts()
    process_match = conflicts.find_conflicts(inferred_df, main_inputs_path)
    
    time_step3_1 = time.time()
    print("Time Taken for Step 3.1: ", time_step3_1 - time_step3)

    # 3.2 process_age() over matches + conflicts
    process_match = conflicts.process_age(process_match, main_inputs_path)
    
    time_step3_2 = time.time()
    print("Time Taken for Step 3.2: ", time_step3_2 - time_step3_1)

    # 3.3 rematch conflicts by breaking those connections and rebatching
    rematch_df = BatchMatcher(main_inputs_path, preprocessed_pt_fp, preprocessed_ec_fp, id_conflicts = process_match, skip_hashing = skip_hashing).run() # Pass #1
    rematch_df = Infer_relationships.infer_relationships(rematch_df, main_inputs_path)
    
    time_step3_3 = time.time()
    print("Time Taken for Step 3.3: ", time_step3_3 - time_step3_2)
    
    # 3.4 process the rematch dataset for conflicts
    process_rematch = conflicts.find_conflicts(rematch_df, main_inputs_path)
    process_rematch = conflicts.process_age(process_rematch, main_inputs_path)
    
    time_step3_4 = time.time()
    print("Time Taken for Step 3.4: ", time_step3_4 - time_step3_3)
    
    # 3.5 merge clean relationships from pass 1 and 2
    clean_pass_1 = process_match.loc[(process_match['conflict'] == 0) & (process_match['age_conflict'] == 0) ]
    clean_pass_2 = process_rematch.loc[(process_rematch['conflict'] == 0) & (process_rematch['age_conflict'] == 0) ]
    no_conflicts = clean_pass_1.merge(clean_pass_2, how='outer')
    #no_filter = process_match.merge(process_rematch, how='outer')
    
    time_step3_5 = time.time()
    print("Time Taken for Step 3.5: ", time_step3_5 - time_step3_4)
    
    # 3.6 create a high confidence dataset with only age-validated relationships
    confident = no_conflicts.loc[~pd.isnull(no_conflicts['age_diff'])] # only take non-zero age differences
    
    time_step4 = time.time()
    print("Time Taken for Step 3 (Total): ", time_step4 - time_step3)

    # Begin Step 4 - Establish Family linkage
    
    # 4.1 Add specific_relationship
    
    no_conflicts['specific_relationship'] = '' # new field
    no_conflicts['specific_relationship'] = np.where((no_conflicts.ec_relation == 'parent' ) & (no_conflicts.pt_sex == 'F' ), 'mother', no_conflicts['ec_relation']) # flag mothers, else general relation
    no_conflicts['specific_relationship'] = np.where((no_conflicts.ec_relation == 'parent' ) & (no_conflicts.pt_sex == 'M' ), 'father', no_conflicts['specific_relationship']) # flag fathers
    
    # 4.2 Check linkage inputs & check output of previous steps
    process_match.to_csv( Path(main_inputs_path / ("dt_process_match_out.csv")), index=False)
    process_rematch.to_csv( Path(main_inputs_path / ("dt_process_rematch_df_out.csv")), index=False)
    clean_pass_1.to_csv( Path(main_inputs_path / ("dt_clean_pass_1_out.csv")), index=False)
    clean_pass_2.to_csv( Path(main_inputs_path / ("dt_clean_pass_2_out.csv")), index=False)
    no_conflicts.to_csv( Path(main_inputs_path / ("dt_no_conflicts_out.csv")), index=False)
    #no_filter.to_csv( Path(main_inputs_path / ("dt_no_filter_out.csv")), index=False)
    confident.to_csv( Path(main_inputs_path / ("high_confidence_matches.csv")), index=False)
    
    # 4.2 Assign family IDs
    import Step4_AssignFamilyIDs.family_linkage as FamilyLinkage
    families_fn = FamilyLinkage.familyLinkage(no_conflicts, main_inputs_path)

    time_step5 = time.time()
    print("Time Taken for Step 4: ", time_step5 - time_step4)

if __name__ == '__main__':
    print("Arguments provided: ", sys.argv)

    # First checks if enough arguments were provided. If not, program uses defaults at top of script.
    if len(sys.argv) < 5:  # sys.argv[0] is the main_script.py's path
        print("\tNot enough arguments provided - expected at least 4:\n\t\t1) Main input path, \n\t\t2) Patient Demographics file path, \n\t\t3) Emergency Contacts file path\n\t\t4) Relationship Map file path\n\n\tWill use default paths and parameters as defined at top of main_script.py")

        # Validating default parameters as found at top of this script
        assert os.path.exists(main_inputs_path), "Main Input Folder Path is invalid. Modify value at top of main_script.py"
        assert os.path.exists(patients_file_path), "Default Patient Demographics File Path is invalid. Modify value at top of main_script.py"
        assert os.path.exists(emergency_contacts_file_path), "Default Emergency Contacts File Path is invalid. Modify value at top of main_script.py"
        assert os.path.exists(relation_map_file_path), "Default Relationship Map File Path is invalid. Modify value at top of main_script.py"

        if skip_preprocessing:
            print("\t`skip_preprocessing' flag is set to True. Will skip preprocessing.\n")
            assert os.path.exists(preprocessed_pt_fp), "Default PreProcessed Patient Demographics Path is invalid. Modify value at top of main_script.py"
            assert os.path.exists(preprocessed_ec_fp), "Default PreProcessed Emergency Contacts File path is invalid.  Modify value at top of main_script.py"

    # If enough arguments were provided, will validate parameters then begin running.
    else:
        tmp_main_path = Path(sys.argv[1])
        assert os.path.exists(tmp_main_path), "Main Inputs Folder was not found. Exiting."

        tmp_pt_path = Path(tmp_main_path / sys.argv[2])
        assert os.path.exists(tmp_pt_path), "Patient Demographics File was not found. Exiting."

        tmp_ec_path = Path(tmp_main_path / sys.argv[3])
        assert os.path.exists(tmp_ec_path), "Emergency Contacts File was not found. Exiting."

        tmp_reln_path = Path(tmp_main_path / sys.argv[4])
        assert os.path.exists(relation_map_file_path), "Relationship Mapping File was not found. Exiting."

        main_inputs_path = tmp_main_path
        patients_file_path = tmp_pt_path
        emergency_contacts_file_path = tmp_ec_path
        relation_map_file_path = tmp_reln_path

        if "--skip_preprocessing" in sys.argv:
            skip_preprocessing = True
            preprocessed_pt_fp = sys.argv[2]
            preprocessed_ec_fp = sys.argv[3]
            print("Skipping Pre-Processing, using provided paths as paths to preprocessed files.")
        else:
            skip_preprocessing = False

    the_work()

