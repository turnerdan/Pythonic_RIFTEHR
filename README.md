# RIFTEHR

Relationship Inference From The EHR (RIFTEHR) is an automated algorithm for identifying relatedness between patients in an institution's Electronic Health Records.

Original Authors: Fernanda Polubriaginof and Nicholas Tatonetti

This Version: Farhad Ghamsari

http://riftehr.tatonettilab.org

Remember to always respect patient privacy.

---
## What is different about this version?

- Fully Python, no dependencies on SQL or Julia
- Much, much faster, thanks to vectorization of functions

## Setting up your files
<b>Patient Demographics Table</b> is a comma delimited file with the following headers. Each of these values corresponds to the patient:

    - MRN, FirstName, LastName, PhoneNumber, Zipcode

<b>Emergency Contacts Table</b> is a comma delimited file with the following headers. MRN_1 corresponds to the MRN of the patient. (It is the link to the Patient Demographics Table.)
The rest of the values correspond to the Patient's Emergency Contact.
EC_Relationship refers to the relationship between Patient and EC. (If EC_Relationship is Parent, then the EC is the Patient's Parent.) 

    - MRN_1, EC_LastName, EC_FirstName, EC_PhoneNumber, EC_Zipcode, EC_Relationship

## Customization
- Go to Step 0 > `preprocess.py` > `process_phones()`:
    - Remove any additional phone numbers that are recurrent in your data set. For example, our team had to remove the Northwestern University main line as it was a common placeholder for emergency contact's phone numbers.
- See `relation_map.csv`. The input_relation column contains emergency contact relationships as they appear in your dataset, and the output_relations column is what they should map to, as required by the RIFTEHR program.
- In Step 1 > `match_in_batches.py` > `find_matches()` I felt that searching by a single data element for a possible match was too nonspecific, and opted to leave it commented out. You may experiment with it by uncommenting the section.

## Contact
Should you have any questions, comments, suggestions, please don't hesitate to reach out:

fghamsari@tulane.edu  