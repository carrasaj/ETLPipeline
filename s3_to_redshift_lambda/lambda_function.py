import boto3
import os

def lambda_handler(event, context):
    # Extract data from the event
    s3_key = event['s3_key']
    landed_timestamp = event['landed_timestamp']
    loaded_timestamp = event['loaded_timestamp']
    file_size = event['file_size']
    tablename = event['table_name']

    # Prepare the item to be inserted into DynamoDB
    item = {
        'file_key': s3_key,
        'landed_timestamp': landed_timestamp,
        'loaded_timestamp': loaded_timestamp,
        'file_size': file_size,
        'table_name': tablename,
        # Include any additional metadata here
    }
    print("Preparing to insert item into DynamoDB:", item)

    # Get the DynamoDB table name from environment variables
    dynamodb_table = os.getenv('dynamodb_table')

    # Initialize the DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)

    # Insert the item into DynamoDB
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
