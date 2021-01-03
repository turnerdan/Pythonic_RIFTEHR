# Step E. Data Encryption

The scripts in this directory were written by the original authors, and I have yet to have time to implement them (ie ensure they work) in this version. That is a project for another time.
However, they are still provided here in the event that you would like to try working them out for yourself. 

----
The scripts in this directory provide you with tools to encrypt your patient data for analysis. They
share no dependencies with the rest of the analysis and use only standard Python 3 libraries. They 
have been designed to be run by an agent with access to the identifiable data. The agent can then
provide the encrypted patient dataset to you to perform the remaining analysis allowing you to run
the relationships extraction without having to expose identifiable patient details. 

This step is completely optional. The code will run on the identifiable patient data as well. Further,
your institution can, if they wish to, use/write their own tools for building a encrypted dataset. In
that case, we recommend reading through the following paragraphs that explain how the names, addresses,
and phone numbers are normalized to maximize the chances of a match. 



----
Remember to always respect patient privacy.
