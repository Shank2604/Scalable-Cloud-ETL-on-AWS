Scalable Cloud ETL on AWS

A serverless data pipeline that performs Extract, Transform, and Load (ETL) operations on CSV data using AWS S3, AWS Glue, and AWS Athena.

Project Overview
This project demonstrates a robust and scalable serverless architecture for data processing on the AWS cloud. The pipeline ingests raw CSV files from an S3 bucket, processes them using a serverless AWS Glue ETL job, stores the transformed data back into S3 in an optimized format (like Parquet), and makes it available for analysis using standard SQL through AWS Athena.

Architecture
The architecture is designed to be cost-effective and scalable, as it only uses resources when data is being processed.

<img width="1910" height="901" alt="Screenshot (5)" src="https://github.com/user-attachments/assets/76c3a289-7f3d-488e-88ec-517d45efa706" />

Data Ingestion (S3): Raw CSV files are uploaded to a designated source S3 bucket, which acts as the data lake's landing zone.

Data Cataloging (Glue): An AWS Glue Crawler scans the source data to infer the schema and populates the AWS Glue Data Catalog with table metadata.

Data Processing (Glue): An AWS Glue ETL job, written in PySpark, reads the raw data from the source S3 bucket, performs necessary transformations (e.g., data cleaning, type casting, filtering), and writes the processed data to a destination S3 bucket.

Data Serving (Athena): AWS Athena uses the Glue Data Catalog to run interactive SQL queries directly on the transformed data stored in the destination S3 bucket.


Technologies Used


Data Storage: AWS S3 (Simple Storage Service)

ETL & Data Catalog: AWS Glue

Data Querying: AWS Athena

Language/Framework: Python (PySpark)

Query Language: SQL


Workflow


Upload: A user or an automated process uploads one or more CSV files into the /raw folder of the source S3 bucket.

Crawl: The AWS Glue Crawler is run (manually or on a schedule) to catalog the newly uploaded data, creating or updating the table schema in the Glue Data Catalog.

Transform: The AWS Glue ETL job is executed. It reads the data based on the catalog, applies the transformation logic defined in the script, and converts the data into Parquet format.

Load: The transformed Parquet files are loaded into the /processed folder of the destination S3 bucket.


Query: Users can immediately query the processed, structured data using AWS Athena to gain insights.
Setup and Deployment


Follow these steps to set up the pipeline in your AWS account:


Create S3 Buckets:

Create a bucket for the source data (e.g., my-etl-source-data-bucket).

Create another bucket for the output/processed data (e.g., my-etl-processed-data-bucket).

Create an IAM Role for Glue:

Navigate to IAM > Roles > Create Role.

Select Glue as the AWS service.

Attach the AWSGlueServiceRole policy.

Attach policies that grant read/write access to your S3 buckets (e.g., AmazonS3FullAccess for simplicity, or a more restrictive custom policy for production).

Name the role (e.g., GlueETLRole) and create it.

Configure Glue Crawler:

Navigate to AWS Glue > Crawlers > Add crawler.

Name the crawler and select the IAM role created above.

Set the data source to the source S3 bucket path.

Choose an existing or create a new database for the catalog (e.g., csv_processing_db).

Finish the setup and run the crawler.

Create and Configure Glue ETL Job:

Navigate to AWS Glue > ETL jobs > Add job.

Name the job, select the IAM role, and choose a Spark engine.

Point the job to your Python/PySpark script (you can upload it to S3 or write it in the inline editor).

Save the job and run it.

Configure Athena:

Navigate to the AWS Athena console.

If this is your first time, you will be prompted to set up a query result location in an S3 bucket.

On the left panel, select the database created by your crawler (csv_processing_db). You should see the table(s) corresponding to your source data.

Usage

Once the pipeline is deployed and the ETL job has run successfully:


Go to the AWS Athena console.

Select the correct Data source and Database.

You can now query the processed data. For example:

SELECT * FROM "your_table_name" LIMIT 10;

SELECT status, COUNT(*)
FROM "your_table_name"
WHERE registration_year > 2020
GROUP BY status;


Future Enhancements

Automation: Use AWS Lambda triggers to automatically start the Glue ETL job upon new file uploads to the source S3 bucket.

Monitoring: Set up CloudWatch Alarms and Dashboards to monitor job failures, execution times, and costs.

Data Quality: Integrate a data quality library like Deequ into the Glue job to validate data before it's written to the destination.

CI/CD: Implement a CI/CD pipeline using AWS CodeCommit and CodePipeline to manage and deploy changes to the Glue ETL script.

Visualization: Connect AWS QuickSight to Athena to create interactive dashboards and visualizations from the processed data.
