import boto3
import os

def lambda_handler(event, context):
    # Extract data from the event
    s3_key = event['s3_key']
    landed_timestamp = event['landed_timestamp']
    loaded_timestamp = event['loaded_timestamp']
    file_size = event['file_size']
    tablename = event['table_name']
    schema_version = event.get('schema_version', 'unknown')
    redshift_table_exists = event.get('redshift_table_exists', False)
    load_mode = event.get('load_mode', 'append')

    # table_prefix can be database_name.schema_name.table_name
    # s3_key format: database_name/schema_name/table_name/action/file.csv
    s3_key_split = s3_key.split('/')
    database_name = s3_key_split[0]
    schema_name = s3_key_split[1]
    table_name = s3_key_split[2]
    table_prefix = f"{database_name}.{schema_name}.{table_name}"

    # Prepare item for DynamoDB
    item = {
        'table_prefix': table_prefix,
        'file_key': s3_key,
        'landed_timestamp': landed_timestamp,
        'loaded_timestamp': loaded_timestamp,
        'file_size': file_size,
        'table_name': tablename,
        'schema_version_timestamp': schema_version,
        'redshift_table_exists': redshift_table_exists,
        'load_mode': load_mode,
        'status': 'SUCCESS',  # since this lambda is called after successful load
        # Add table_status if needed, e.g. "exists/match"
        'table_status': 'exists/match'
    }
    print("Preparing to insert item into DynamoDB:", item)

    # Get the DynamoDB table name from environment variables
    dynamodb_table = os.getenv('dynamodb_table')

    # Insert the item into DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)
    try:
        response = table.put_item(Item=item)
        print(f"Successfully inserted item into DynamoDB: {item}")
    except Exception as e:
        print(f"Error inserting item into DynamoDB: {str(e)}")
        raise e

    return {
        'status': 'Success',
        'message': 'Item inserted into DynamoDB',
        'item': item
    }
