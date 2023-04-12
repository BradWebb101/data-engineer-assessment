import boto3
import pytest
from moto import mock_dynamodb
import json


@pytest.fixture(scope="function")
def dynamodb_table():
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
        table_name = 'test_table'
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        table.put_item(
            Item={
                'id':'1',
                'first_name':'John',
                'age': '23'
            }
        )
        table.put_item(
            Item={
                'id': '2',
                'first_name':'Jane',
                'age':'27'
            }
        )
        yield table


def test_all_data_handler(dynamodb_table, monkeypatch):
    # Call the function and check the response
    monkeypatch.setenv('TABLE_NAME', 'test_table')
    from api_app import handler
    response = handler({}, {}, dynamodb_table)
    expected_response = {'statusCode': 200, 'body': json.dumps([{"id": "1", "first_name": "John", "age": "23"}, {"id": "2", "first_name": "Jane", "age": "27"}])}
    assert response == expected_response

