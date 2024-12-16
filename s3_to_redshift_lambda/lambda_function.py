import json
import psycopg2
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """
    Main Lambda handler function that:
    - Extracts information about the incoming CSV file from the S3 event.
    - Determines the action (append, truncate, schema) based on the S3 key path.
    - Fetches and validates schema information from S3.
    - Connects to Redshift, modifies the table as needed (create/truncate), and loads the CSV data.
    - Returns metadata and load details for downstream processing.
    """

    # Record the time at which the file landed in S3
    landed_timestamp = datetime.utcnow().isoformat()

    # Extract the S3 bucket and key from the event input
    s3_bucket = event['s3_bucket']
    s3_key = event['s3_key']
    from_path = f"s3://{s3_bucket}/{s3_key}"
    print("s3_key:", s3_key)
    print("from_path:", from_path)

    # The expected S3 key format is: dev/<action>/table_name/filename.csv
    # Parse the key to retrieve database_name, action, and table_name
    s3_key_split = s3_key.split('/')
    if len(s3_key_split) < 4:
        raise ValueError("Invalid s3_key format. Expected: dev/<action>/table_name/file.csv")

    database_name = s3_key_split[0]
    action = s3_key_split[1].lower()   # action could be 'append', 'truncate', or 'schema'
    table_name = s3_key_split[2]
    file_name = '/'.join(s3_key_split[3:])

    # Assume a fixed schema (e.g., 'public') in Redshift for simplicity
    full_table_name = f"public.{table_name}"

    # Retrieve the file size from S3
    s3 = boto3.client('s3')
    try:
        file_info = s3.head_object(Bucket=s3_bucket, Key=s3_key)
        file_size = file_info['ContentLength']
    except Exception as e:
        print(f"Failed to retrieve metadata for {s3_key}: {str(e)}")
        raise e

    # Load environment variables for Redshift connection and IAM role
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    iam_arn = os.getenv('redshift_iam_arn')

    # Fetch the latest schema file key from S3 for this table
    schema_file_key = get_latest_schema_key(s3, s3_bucket, table_name, database_name, action)
    # Load and parse the schema JSON
    schema_data = get_schema_from_s3(s3, s3_bucket, schema_file_key)
    schema_version = schema_data.get('schema_version', 'unknown')

    # If action is append or truncate, validate the incoming CSV columns against the schema
    if action in ['append', 'truncate']:
        validate_csv_against_schema(s3, s3_bucket, s3_key, schema_data)

    # Connect to Redshift
    connection = psycopg2.connect(
        dbname=database_name,
        host=host,
        port='5439',
        user=user,
        password=password
    )
    curs = connection.cursor()

    # Perform Redshift operations based on the action
    if action == 'append':
        # Append mode: just COPY new data into the existing table
        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "append"
    elif action == 'truncate':
        # Truncate mode: empty the table before loading new data
        truncate_query = f"TRUNCATE TABLE {full_table_name};"
        print("Executing TRUNCATE query:", truncate_query)
        curs.execute(truncate_query)
        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "truncate"
    elif action == 'schema':
        # Schema mode: drop and recreate the table according to the schema, then load data
        drop_query = f"DROP TABLE IF EXISTS {full_table_name};"
        print("Executing DROP TABLE query:", drop_query)
        curs.execute(drop_query)
        create_table_query = build_create_table_query(full_table_name, schema_data['columns'])
        print("Executing CREATE TABLE query:", create_table_query)
        curs.execute(create_table_query)
        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "schema"
    else:
        raise ValueError(f"Unknown action '{action}' specified in S3 key.")

    # Commit Redshift changes and close the connection
    connection.commit()
    curs.close()
    connection.close()

    # Record the time after data is successfully loaded into Redshift
    loaded_timestamp = datetime.utcnow().isoformat()

    # Prepare output metadata including schema version, load mode, and table info
    output = {
        's3_key': s3_key,
        'landed_timestamp': landed_timestamp,
        'loaded_timestamp': loaded_timestamp,
        'file_size': file_size,
        'table_name': full_table_name,
        'schema_version': schema_version,
        'redshift_table_exists': True,
        'load_mode': load_mode
    }

    return output

def get_latest_schema_key(s3, s3_bucket, table_name, database_name, action):
    """
    Finds the latest schema file for the given table by listing objects in the schema directory and
    selecting the one with the most recent timestamp in its key. Assumes schema files follow a naming
    convention like schema_<timestamp>.json.
    """
    prefix = f"dev/schema/{table_name}/"
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    if 'Contents' not in response or not response['Contents']:
        raise ValueError(f"No schema files found under {prefix} in {s3_bucket}")

    # Select the latest schema file by comparing keys (filenames)
    objects = response['Contents']
    latest = max(objects, key=lambda x: x['Key'])
    return latest['Key']

def get_schema_from_s3(s3, s3_bucket, key):
    """
    Retrieves and parses the schema JSON file from S3.
    """
    obj = s3.get_object(Bucket=s3_bucket, Key=key)
    return json.loads(obj['Body'].read())

def validate_csv_against_schema(s3, bucket, key, schema_data):
    """
    Compares the CSV header against the schema-defined columns to ensure a match.
    If there's a discrepancy, raises a ValueError to prevent loading incorrect data.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_content = obj['Body'].read().decode('utf-8')
    csv_lines = csv_content.splitlines()
    header_line = csv_lines[0]
    csv_columns = [col.strip() for col in header_line.split(',')]
    schema_columns = [col['name'] for col in schema_data['columns']]

    if csv_columns != schema_columns:
        raise ValueError(f"CSV columns {csv_columns} do not match schema columns {schema_columns}.")

def build_create_table_query(full_table_name, columns):
    """
    Constructs a CREATE TABLE SQL statement using the column definitions from the schema.
    Properly quotes column names to handle spaces or special characters.
    """
    cols_def = []
    for col in columns:
        col_name = col['name']
        col_type = col['type']
        nullable = '' if col.get('nullable', True) else 'NOT NULL'
        cols_def.append(f"\"{col_name}\" {col_type} {nullable}")
    create_table_query = f"CREATE TABLE {full_table_name} (\n" + ",\n".join(cols_def) + "\n);"
    return create_table_query

def make_copy_query(full_table_name, from_path, iam_arn):
    """
    Builds the COPY command to load CSV data from S3 into Redshift using the provided IAM role.
    Assumes CSV format with a header that should be ignored (IGNOREHEADER 1).
    """
    return (
        f"COPY {full_table_name} FROM '{from_path}' "
        f"IAM_ROLE '{iam_arn}' "
        "CSV IGNOREHEADER 1;"
    )
