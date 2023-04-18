import os
import csv
import boto3
from datetime import datetime
from io import StringIO
import gzip

def validate(row_json):
    #Picked up as only strings in that list that dont have capitals for the aws service
    if 'awskms' in row_json.get('lineItem/LineItemDescription',''):
        row_json['lineItem/LineItemDescription'].replace('awskms','AWSKMS')
    if 'awswaf' in row_json.get('lineItem/LineItemDescription',''):
        row_json['lineItem/LineItemDescription'].replace('awswaf','AWSWAF')
    #Picked up as data could not be converted to a int        
    if '"' in row_json.get('reservation/SubscriptionId',''):
        row_json['reservation/SubscriptionId'].replace('"','')
    #Date didnt convert to datetime
    if '02/06/2022' in row_json.get('lineItem/UsageEndDate',''):
        row_json['lineItem/UsageEndDate'].replace('02/06/2022', '2022-06-02T00:00:00Z')
    if row_json.get('lineItem/UsageEndDate','Z')[-1] != 'Z':
        row_json['lineItem/UsageEndDate'][-1] + 'Z'
    #Only string to not starting with capital
    if 'nil' in row_json.get('lineItem/Operation',''):
        row_json['lineItem/Operation'].replace('nil','Unknown')
    return row_json

def lambda_handler(event, context, test_client=None):
    #Setting client
    s3 = test_client or boto3.client('s3')

    # get the input file from the S3 bucket
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    database_bucket = os.getenv('DATABASE_BUCKET')
    primary_key = os.getenv('PRIMARY_KEY')
    time_key = os.getenv('TIME_KEY')
    account_key = os.getenv('ACCOUNT_KEY')
    file_key = event['Records'][0]['s3']['object']['key']
    response_data = s3.get_object(Bucket=bucket_name, Key=file_key)['Body'].read().decode('utf-8').splitlines()

    # Set up of dicts for sorting
    output_files = {}
    header_groups = {}

    #Creating dict reader object
    reader = csv.DictReader(response_data, delimiter=',')

    #Creating headers for the json object
    headers = reader.fieldnames

    #Error handling to make sure the file has the primary key, raise error if it doesnt
    if primary_key not in headers:
        raise TypeError('Primary key does not exist in the data')

    #This creates a header dictionary, that splits the columns into dictionarys product/ProductName. key would be product values ['primary_key', 'ProductName']
    for item in headers:
        prefix = item.split('/')[0]
        header_groups.setdefault(prefix, [primary_key]).append(item)
        
    #Iterating over the dictionary 
    for row in reader:
        try:
            #Removes all blank values from the file
            row = {key: value for key, value in row.items() if value != ''}
            #Validate function references above to fix easter eggs added to the data
            row = validate(row)
            #Creates datetime object for file name
            item_end_date = datetime.strptime(row[time_key], '%Y-%m-%dT%H:%M:%SZ')
            #Making values int for the file name(Needs to be int for Athena)
            account_number = int(row[account_key])
            year = int(item_end_date.year)
            month = int(item_end_date.month)
            day = int(item_end_date.day)
            #Building dictionary of file name and values for the file
            for group, columns in header_groups.items():
                #{account_number}-{year}-{month}-{day}-
                output_file_name = f"{group}/{account_number}/{year}/{month}/{day}/aws_usage_file.csv"
                if output_file_name not in output_files:
                    output_files[output_file_name] = []
                group_row = {key.split('/')[1]: value for key, value in row.items() if key in columns}
                output_files[output_file_name].append(group_row)

        except ValueError as e:
            #Sends all errors to a errors folder, with only errors in to manually check.
            #Explain the reason in the readme, short answer AWS data is always good, doesnt need t in etl
            file_name = file_key.split('.')[0]
            output_file_name = f'errors/{file_name}.csv'
            if output_file_name not in output_files:
                output_files[output_file_name] = []
            #Adds error file details to output if a value error is found
            output_files[output_file_name].append(row)

    for output_file_name, rows in output_files.items():
        # This says if the only unique key across all of the possiblities in the file are greate than 1 (eg more values than jsut primary key) write file. Otherwise pass
        if len(set().union(*(d.keys() for d in rows))) != 1:
            csv_data = StringIO()
            csv_writer = csv.DictWriter(csv_data, fieldnames=list(rows[0].keys()))
            csv_writer.writeheader()
            for i in rows:
                csv_writer.writerow(i)
            csv_data.seek(0)
            csv_data = gzip.compress(csv_data.getvalue().encode('utf-8'))
            s3.put_object(Bucket=database_bucket, Key=output_file_name, Body=csv_data, ContentEncoding='gzip')

    return {
        'statusCode': 200,
        'body': 'Files split successfully.'
    }

