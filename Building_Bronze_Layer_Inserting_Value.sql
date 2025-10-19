/* After the defining the Dataset and its tables, now we have the task of inserting data into the tables.
According to Baraa, we are using the following query in the SQL Server Express:

TRUNCATE TABLE crm_cust_info;
BULK INSERT crm_cust_info 
FROM 'C:\SQL\project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
The bigquery cloud assist suggested to use Google cloud service to import csv file. It also mentioned the bq command-line tool offered also in the google cloud SDK, which interact with BigQuery directly from your terminal or script.

bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --field_delimiter=',' \
    --autodetect \
    --write_disposition=WRITE_TRUNCATE \
    `data-with-baraa-sql-projects:bronze.crm_cust_info` \
    gs://your-gcs-bucket-name/source_crm/cust_info.csv

I will use the typical 'BigQuery Load Job' for this task:
1.Upload your CSV file to Google Cloud Storage (GCS). BigQuery primarily loads data from GCS buckets. You can't directly load from a local file path like C:\SQL\... into BigQuery.
2.Initiate a BigQuery load job. This job tells BigQuery to read the data from your GCS file and insert it into a specified table.

After the load job, I found there is a detailing page in the job history, showing the start and end time of the job, duration, source and destination and so on. Although Baraa gave a stored procedure giving codes like printing the job status real time and calculating the job duration etc, the BigQuery system does not work the same way as SQL Server and has the similar function realised in another way.
*/