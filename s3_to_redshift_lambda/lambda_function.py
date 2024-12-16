import psycopg2
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """
    Function overview:
    - Extracts columns from the CSV header to determine the schema.
    - For 'append' and 'truncate' actions, ensures the table exists and matches the CSV columns.
    - For 'schema' action, drops and recreates the table according to the CSV columns.
    - Loads the CSV data into the Redshift table via COPY.
    """

    # Record the time at which the file landed in S3
    landed_timestamp = datetime.utcnow().isoformat()

    # Extract the S3 bucket and key from the event input
    s3_bucket = event['s3_bucket']
    s3_key = event['s3_key']
    from_path = f"s3://{s3_bucket}/{s3_key}"
    print("s3_key:", s3_key)
    print("from_path:", from_path)

    # The expected S3 key format: dev/<action>/table_name/filename.csv
    s3_key_split = s3_key.split('/')
    if len(s3_key_split) < 4:
        raise ValueError("Invalid s3_key format. Expected: dev/<action>/table_name/file.csv")

    database_name = s3_key_split[0]
    action = s3_key_split[1].lower()   # action could be 'append', 'truncate', 'schema'
    table_name = s3_key_split[2]
    file_name = '/'.join(s3_key_split[3:])

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

    # Extract columns from the CSV file header
    csv_columns = get_csv_columns(s3, s3_bucket, s3_key)
    if not csv_columns:
        raise ValueError("CSV file has no columns or is empty.")

    # Connect to Redshift
    connection = psycopg2.connect(
        dbname=database_name,
        host=host,
        port='5439',
        user=user,
        password=password
    )
    curs = connection.cursor()

    # Check if table exists
    table_exists = check_table_exists(curs, full_table_name)

    # Handle schema logic
    if action == 'schema':
        # Schema action: Drop and recreate the table based on the new CSV columns
        drop_query = f"DROP TABLE IF EXISTS {full_table_name};"
        print("Executing DROP TABLE query:", drop_query)
        curs.execute(drop_query)

        create_table_query = build_create_table_query(full_table_name, csv_columns)
        print("Executing CREATE TABLE query:", create_table_query)
        curs.execute(create_table_query)

        # Load the data
        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "schema"

    elif action == 'append':
        # Append: If table does not exist, create it using current columns
        # If table exists, ensure columns match
        if not table_exists:
            # Create table
            create_table_query = build_create_table_query(full_table_name, csv_columns)
            print("Executing CREATE TABLE query:", create_table_query)
            curs.execute(create_table_query)
        else:
            # Validate schema matches
            existing_columns = get_table_columns(curs, full_table_name)
            if existing_columns != csv_columns:
                # For append mode, we require schema action for changes
                raise ValueError("Table schema differs from CSV columns. Use 'schema' action to reconcile.")

        # Load the data
        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "append"

    elif action == 'truncate':
        # Truncate: If table does not exist, create it. Otherwise, ensure schema matches and truncate.
        if not table_exists:
            # Create table
            create_table_query = build_create_table_query(full_table_name, csv_columns)
            print("Executing CREATE TABLE query:", create_table_query)
            curs.execute(create_table_query)
        else:
            # Validate schema matches
            existing_columns = get_table_columns(curs, full_table_name)
            if existing_columns != csv_columns:
                # For truncate mode, we also require schema action for changes
                raise ValueError("Table schema differs from CSV columns. Use 'schema' action to reconcile.")

        # Truncate and load
        truncate_query = f"TRUNCATE TABLE {full_table_name};"
        print("Executing TRUNCATE query:", truncate_query)
        curs.execute(truncate_query)

        copy_query = make_copy_query(full_table_name, from_path, iam_arn)
        print("Executing COPY query:", copy_query)
        curs.execute(copy_query)
        load_mode = "truncate"

    else:
        raise ValueError(f"Unknown action '{action}' specified in S3 key.")

    # Commit Redshift changes and close the connection
    connection.commit()
    curs.close()
    connection.close()

    # Record the time after data is successfully loaded into Redshift
    loaded_timestamp = datetime.utcnow().isoformat()

    # Prepare output metadata
    output = {
        's3_key': s3_key,
        'landed_timestamp': landed_timestamp,
        'loaded_timestamp': loaded_timestamp,
        'file_size': file_size,
        'table_name': full_table_name,
        'redshift_table_exists': True,
        'load_mode': load_mode
    }

    return output

def get_csv_columns(s3, bucket, key):
    """
    Reads the CSV file from S3 and extracts the header line to determine column names.
    Returns a list of column names in the order they appear.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_content = obj['Body'].read().decode('utf-8')
    csv_lines = csv_content.splitlines()
    if not csv_lines:
        return []
    header_line = csv_lines[0]
    columns = [col.strip() for col in header_line.split(',')]
    return columns

def check_table_exists(curs, full_table_name):
    """
    Checks if a table exists in Redshift.
    """
    schema_name, table_only = full_table_name.split('.')
    query = (f"SELECT 1 FROM information_schema.tables "
             f"WHERE table_schema = '{schema_name}' AND table_name = '{table_only}';")
    curs.execute(query)
    return curs.fetchone() is not None

def get_table_columns(curs, full_table_name):
    """
    Retrieves the list of columns currently defined for the given Redshift table.
    Returns a list of column names in their defined order.
    """
    schema_name, table_only = full_table_name.split('.')
    query = (f"SELECT column_name FROM information_schema.columns "
             f"WHERE table_schema='{schema_name}' AND table_name='{table_only}' "
             f"ORDER BY ordinal_position;")
    curs.execute(query)
    rows = curs.fetchall()
    return [row[0] for row in rows]

def build_create_table_query(full_table_name, columns):
    """
    Constructs a CREATE TABLE SQL statement using the column names inferred from the CSV header.
    By default, all columns are created as VARCHAR(MAX).
    Adjust the data types as needed.
    """
    cols_def = []
    for col in columns:
        # Quoting column names for safety, default to VARCHAR for simplicity
        cols_def.append(f"\"{col}\" VARCHAR")
    create_table_query = f"CREATE TABLE {full_table_name} (\n" + ",\n".join(cols_def) + "\n);"
    return create_table_query

def make_copy_query(full_table_name, from_path, iam_arn):
    """
    Builds the COPY command to load CSV data from S3 into Redshift.
    Assumes CSV format with a header that should be ignored.
    """
    return (
        f"COPY {full_table_name} FROM '{from_path}' "
        f"IAM_ROLE '{iam_arn}' "
        "CSV IGNOREHEADER 1;"
    )
