import os
import boto3
import json

def handler(event, context, test_client=None):
    table_name = os.getenv('TABLE_NAME')

    if test_client:
        table = test_client
    else:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

    try:
        response = table.scan()

        items = response['Items']

        return {
            'statusCode': 200,
            'body': json.dumps(items)
        }

    except Exception as e:
        print(f"Error scanning table: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error scanning table'})
        }
