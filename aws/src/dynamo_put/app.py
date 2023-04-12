import boto3
import csv
import os
from boto3.dynamodb.types import TypeSerializer

def dynamo_object_serialize(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }

def handler(event:dict, context:dict, test_s3_client=None, test_dynamodb_client=None) -> dict:
    region_name = os.getenv('REGION')
    
    if test_s3_client is not None:
        s3_client = test_s3_client
    else:
        s3_client = boto3.client("s3", region_name=region_name)

    if test_dynamodb_client is not None:
        dynamodb_client = test_dynamodb_client
    else:
        dynamodb_client = boto3.client("dynamodb", region_name=region_name)

    # Get the name of the S3 bucket and object from the event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]

    # Read the contents of the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    csv_data = response["Body"].read().decode("utf-8")

    # Parse the CSV data into a list of dictionaries
    reader = csv.DictReader(csv_data.splitlines())
    rows = [dynamo_object_serialize(row) for row in reader]

    #Putting items in dynamoDB
    table_name = os.getenv('DYNAMO_TABLE_NAME')
    for row in rows:
        dynamodb_client.put_item(
            TableName=table_name,
            Item=row
        )
        
    return {
        "statusCode": 200,
        "body": f"Processed {len(rows)} rows from {bucket_name}/{file_name}.",
    }
