# x_company_request - large files program

# Requirements
API Reference:
* Program requires to have API authorization from Kaggle. 
API documentation: https://github.com/Kaggle/kaggle-api#api-credentials
Important: dowloaded file kaggle.json must be placed in ~/.kaggle/kaggle.json directory.
* Not less than 150 GB free space in local space.

# How to use? 
* Set up Kaggle API;
* Run gitpull_transformation.py script. 

# About current program:
* Download large .csv files (>17GB) to your local space;
* Read these .csv by chunks;
* After simple filtering it's uploading to SQLite local database;
* Now it's possisble to query database and dig into it.

# Possible ways to improve:
* If files need to download every month (in future) it's possible to filter only most important data (like specific language, customers etc.) and such way save space / have quicker processing;
* Move .csv files reading / quering to cloud solutionn. (like GCP BigQuery, AWS Redshift, Azure Synapse Analytics);
* Don't store downloaded files.   
