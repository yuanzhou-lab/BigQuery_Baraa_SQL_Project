/*  ---Data with Baraa SQL Full Course Practice Project---
            ---Note: Bronze Layer Tables Loading---

>>>Purpose:
Load the preset raw csv data into the Bronze Layer.

>>>System:
Google Cloud BigQuery

>>>Task:
According to Baraa, we should use the following query in the SQL Server Express:
TRUNCATE TABLE crm_cust_info;
BULK INSERT crm_cust_info 
FROM 'C:\SQL\project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

>>>Issue:
The BigQuery does not offer reading CSV from local files directly from SQL query. This is due to the nature of the BigQuery cloud service.

>>>Google Cloud Assist Suggestion:
1.  Use Google cloud service to import csv file. 
1.1 Optional: Use the bq command-line tool offered also in the google cloud SDK, which interact with BigQuery directly from your terminal or script, with the following command example:
    bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --field_delimiter=',' \
    --autodetect \
    --write_disposition=WRITE_TRUNCATE \
    `data-with-baraa-sql-projects:bronze.crm_cust_info` \
    gs://your-gcs-bucket-name/source_crm/cust_info.csv
2.  Upload local csv file to BigQuery.

>>>Solution:
I use the suggestion #1 without its option:
1.  Upload the CSV files to Google Cloud Storage (GCS) into Bucket created under the same project name.   
2.  Initiate a BigQuery load job. This job tells BigQuery to read the data from csv files and insert them into specified tables.
3.  6 tables were loaded in total.

>>>Observation:
After the load job, I found there is a detailing page in the job history, showing the start and end time of the job, duration, source and destination and so on. This is what Baraa was trying to do by using writing commands like printing the job status real time and calculating the job duration etc. So in BigQuery, one checks the Job Details after the running, unlike in SQL Server Express, one writes query to ask the system to show running status real time.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/