## ETL Pipeline

This project is a data pipeline with a monitoring API built using AWS. It is meant to automatically load csv data into Redshift from a central data lake in S3. This process is monitored using DynamoDB with an API to fetch the pipeline monitoring data.

## Overview 

Main components:

1. Data Pipeline and Data Lake
  * An S3 bucket serves as the central data lake.
  * When a CSV file is uploaded to the bucket, an EventBridge event is triggered. This event initiates a step function.
  * The lambdas in this step function first connect to a Redshift cluster and writes the data from the csv into a table in the cluster.
  * The second half of the step function logs the metadata into a DynamoDB table to keep track of the flow through the ETL pipeline.
  
2. Pipeline Monitoring API
  * Provides a REST API endpoint via Amazon API Gateway for querying processing logs
  * API calls take in table_name as a parameter, landed_timestamp as an optional additional parameter.
