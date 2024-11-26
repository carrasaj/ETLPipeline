## ETL Pipeline

This project is a data pipeline with a monitoring API built using AWS. It is meant to automatically load csv data uploaded to an S3 bucket into redshift. The uploads are logged in DynamoDB with an API to query the DynamoDB table.

## Overview 

Main components:

1. Data Pipeline and Data Lake
  * An S3 bucket serves as the central data lake.
  * When a CSV file is uploaded to the bucket, an EventBridge event is triggered. This event initiates a step function.
  * The first lamda in this step function connects to a Redshift cluster and writes the data from the csv into a table in the cluster.
  * The second half of the step function logs the metadata into a DynamoDB table to keep track of the flow through the ETL pipeline.
  
2. Pipeline Monitoring API
  * Provides a REST API endpoint via Amazon API Gateway for querying DynamoDB processing logs.
  * API calls take in table_name as a parameter, landed_timestamp as an optional additional parameter.

# Architecture

* S3: Stores incoming CSV files.
* Lambda: Loads CSV data into Redshift and logs processing information into DynamoDB
* Redshift: Stores CSV data for fast SQL querying
* DynamoDB: Logs metadata and keeps track of data flowing through pipeline
* API Gateway: Provies an API for querying the DynamoDB table
* Step Function: Orchestrates execution of Lambda functions

# Setup Instructions


  
