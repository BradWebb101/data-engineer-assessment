import pytest
import boto3
import csv
import moto
from io import StringIO
from app import lambda_handler
import gzip

entry_bucket = 'entry-bucket'
output_bucket = 'output-bucket'
test_file = 'test-file.csv'
region_name = 'eu-central-1'

@pytest.fixture(scope="session")
def mock_s3_client():
    # Start moto mock S3 bucket
    with moto.mock_s3():
        # Create an S3 client
        location_contraint = {'LocationConstraint':region_name}
        s3 = boto3.client('s3', region_name=region_name)

        # Create a mock S3 entry bucket
        s3.create_bucket(Bucket=entry_bucket, CreateBucketConfiguration=location_contraint)

        # Create a mock S3 output bucket
        s3.create_bucket(Bucket=output_bucket, CreateBucketConfiguration=location_contraint)

        with open('sample.csv', 'rb') as data:
            s3.put_object(Bucket=entry_bucket, Key=test_file, Body=data)

        # Yield the bucket name so that the test function can use it
        yield s3

@pytest.fixture(scope="session")
def test_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": entry_bucket},
                    "object": {"key": test_file},
                }
            }
        ]
    }

def test_naming_convention(mock_s3_client, monkeypatch,test_event):
    #Setting env variables for the output bucket name
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    output_bucket_contents = mock_s3_client.list_objects_v2(Bucket=output_bucket)
    assert response['statusCode'] == 200
    print([i['Key'] for i in output_bucket_contents['Contents']])
    assert [i['Key'] for i in output_bucket_contents['Contents']] == ['lineItem/285916830885/2022/6/1/aws_usage_file.csv', 'lineItem/285916830885/2022/6/2/aws_usage_file.csv', 'lineItem/667663686041/2022/6/1/aws_usage_file.csv', 'lineItem/667663686041/2022/6/2/aws_usage_file.csv', 'lineItem/667663686041/2022/7/2/aws_usage_file.csv']

def test_no_file_named_user(mock_s3_client, monkeypatch,test_event):
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    output_bucket_contents = mock_s3_client.list_objects_v2(Bucket=output_bucket)
    assert 'user' not in [i['Key'].split('/')[1] for i in output_bucket_contents['Contents']] 

def test_no_0_byte_files(mock_s3_client, monkeypatch,test_event):
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    output_bucket_contents = mock_s3_client.list_objects_v2(Bucket=output_bucket)
    assert 0 not in [i['Size'] for i in output_bucket_contents['Contents']] 

def test_account_285916830885_has_three_items(mock_s3_client, monkeypatch,test_event):
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    response = mock_s3_client.get_object(Bucket=output_bucket, Key='lineItem/667663686041/2022/6/1/aws_usage_file.csv')
    contents = gzip.decompress(response['Body'].read()).decode('utf-8').splitlines()
    # Create a StringIO object to use as a file-like object
    csv_file = StringIO()
    # Create a CSV writer object
    writer = csv.writer(csv_file)
    # Write your data to the CSV file
    header = contents[0]
    writer.writerow(header)
    writer.writerows(contents[1:])
    # Get the CSV contents as a string
    csv_string = csv_file.getvalue()

    # Print the length of the CSV file
    print(f"Length of CSV file: {len(csv_string)}")

    # Parse the CSV file as a list of dictionaries
    csv_data = list(csv.DictReader(StringIO(csv_string)))
        # This is testing the data output
        #Len 3 is header plus 2 entities
    assert len(contents) == 3
    assert contents[0] == 'LineItemId,UsageEndDate,UsageAccountId,UnblendedCost'

def test_no_file_with_resourceTags_writter(mock_s3_client, monkeypatch,test_event):
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    output_bucket_contents = mock_s3_client.list_objects_v2(Bucket=output_bucket)
    assert 'resourceTags' not in [i.split('/')[0] for i in output_bucket_contents]
    
def test_sum_costs_field(mock_s3_client, monkeypatch,test_event):
    monkeypatch.setenv('DATABASE_BUCKET',output_bucket)
    monkeypatch.setenv('PRIMARY_KEY','identity/LineItemId')
    monkeypatch.setenv('TIME_KEY','lineItem/UsageEndDate')
    monkeypatch.setenv('ACCOUNT_KEY','lineItem/UsageAccountId')
    response = lambda_handler(test_event, {}, mock_s3_client)
    output_bucket_contents = mock_s3_client.list_objects_v2(Bucket=output_bucket)
    cost_values = []
    for i in output_bucket_contents['Contents']:
        print(i)
        response = mock_s3_client.get_object(Bucket=output_bucket, Key=i['Key'])
        contents = gzip.decompress(response['Body'].read()).decode('utf-8').splitlines()
        for row, value in enumerate(contents):
            if row == 0:
                pass
            else:
                cost_values.append(float(value.split(',')[3]))
    #This value taken from the analysis.py field    
    assert sum(cost_values) == -0.0492143666