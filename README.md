# Data Engineer for AI Applications: Project 3 - Cloud Data Warehouse

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms the data into a set of dimensional tables for analytics.

## How to run 
0. Edit the configs in *dwh.cfg* and insert AWS Key & Secret

1. Use IaC approach to:
     - Create clients for EC2, S3, IAM and Redshift
     - Create IAM role, attach policies and get IAM role ARN
     - Create Redshift Cluster
     - Obtain DWH Endpoint
     - Confige TCP port
     - Test DB connection </br>

    `python start_aws_clients_IaC.py`</br>

2. Connect to the DB and create the tables as specified in *sql_queries.py*</br>
`python create_tables.py`

3. Extract the data from S3 into the staging tables, transform and insert the data from the staging tables into the tables of the star schema</br>
`python etl.py`

4. At the end, delete the Redshift Cluster, detach the roles and reset the configs</br>
`python cleanup.py`