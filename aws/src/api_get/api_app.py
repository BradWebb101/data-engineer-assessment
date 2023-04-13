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
        # Check if headers query is provided
        if event.get('headers', {}).get('columns', None):
            columns = event.get('headers', None).get('columns', None)
            projection_expression = ", ".join(columns)
            print(projection_expression)
            response = table.scan(ProjectionExpression=projection_expression)

            items = response['Items']

            return {
                'statusCode': 200,
                'body': json.dumps(items)
            }
        else:
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
