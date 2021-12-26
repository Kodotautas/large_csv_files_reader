# x_company_request large files program

About program:
* Download large .csv files (>17GB) to your local space;
* Read these .csv by chunks;
* After simple filtering it's uploading to SQLite local database;
* Now it's possisble to query database and dig into it.

Possible ways to improve:
* If files need to download every month it's possible to filter only most important data (like specific language, customers etc.) and such way save space / have quicker processing;
* Move .csv files reading / quering to cloud solutionn. (like GCP BigQuery, AWS Redshift, Azure Synapse Analytics);
* Don't store downloaded files.   
