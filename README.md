# Sample Refined Zone
## Overview
This is <b>Spark/Scala</b> based ETL refined zone's sample code.
It provides functionality to read data from different types of files, 
apply configured validations, generate output, exception files and store data
in configured output storage.

## Features
1. Different Types of input files:

Reads csv files from configured input folder, creates Input Dataframe, normalize
input Dataframe based configuration and stores output dataframe to Cassandra.

2. Normalization of Input Dataframe:  
a. Input Dataframe column name normalization as per destination table columns
   It supports renaming input Dataframe column names as per configuration and in optimized way.  
   <br />
   Sample config entry:  
   input.fields.mapping=customer_id:customer_id,customer_unique_id:customer_uid,customer_zip_code_prefix:zipcode_prefix,address:address,customer_city:city,customer_country:country,first_name:fname,last_name:lname
   
b.  Input Data validation against destination field data type:  
   
TBD


