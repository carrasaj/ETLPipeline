import json
import boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

# Helper function to convert Decimal to float/int
def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        # Convert Decimal to int if it's a whole number, else float
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj  # Return the object as-is if it's not a list, dict, or Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('etl_pipeline_monitoring')

def lambda_handler(event, context):
    try:
        # Log the incoming event for debugging
        print(f"Event: {event}")
        
        # Get query parameters
        query_params = event.get('queryStringParameters', {})
        print(f"Query Parameters: {query_params}")
        
        table_name = query_params.get('table_name')
        landed_timestamp = query_params.get('landed_timestamp', None)

        if not table_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'table_name parameter is required'})
            }

        # Build the filter expression
        filter_expression = Attr('table_name').eq(table_name)
        if landed_timestamp:
            try:
                landed_timestamp = int(landed_timestamp)  # Ensure timestamp is an integer
                filter_expression &= Attr('landed_timestamp').gte(landed_timestamp)
            except ValueError:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'timestamp must be an integer'})
                }

        print(f"FilterExpression: {filter_expression}")
        
        # Query the DynamoDB table
        response = table.scan(FilterExpression=filter_expression)
        print(f"Raw DynamoDB Response: {response}")
        
        # Extract and process items
        items = response.get('Items', [])
        if not isinstance(items, list):
            print(f"Unexpected Items type: {type(items)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Invalid Items format'})
            }

        # Convert items to JSON serializable format
        items = decimal_to_native(items)
        
        # Return the processed items
        return {
            'statusCode': 200,
            'body': json.dumps(items)
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
