import boto3
import moto
import pytest
import requests
import json
import csv
from io import StringIO
from boto3.dynamodb.types import TypeDeserializer 

from app import handler

# Set the region and the DynamoDB table name
region_name = "eu-west-3"
bucket_name = "test-bucket"
file_name = 'test-file.csv'
table_name = "test-table"

# Create a test event
@pytest.fixture(scope='function')
def test_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket_name},
                    "object": {"key": file_name},
                }
            }
        ]
    }

@pytest.fixture(scope='function')
def s3():
    with moto.mock_s3():
        location_contraint = {'LocationConstraint':region_name}
        client = boto3.client("s3", region_name=region_name)
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location_contraint)
        # Add bucket policy
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }
        client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        url = "https://raw.githubusercontent.com/bobbiegorp/data-engineer-assessment/main/assets/data.csv"

        response = requests.get(url)
        reader = csv.reader(response.text.splitlines(), delimiter=',')

        row_list = []
        # iterate over the rows and add to the list until you have 10 rows
        for index, row in enumerate(reader):
            if index == 11:
                break
            row_list.append(row)

        with StringIO() as f:
            writer = csv.writer(f)
            writer.writerows(row_list)
            csv_str = f.getvalue()

        # encode CSV-formatted string as bytes and upload to S3
        csv_bytes = csv_str.encode('utf-8')

        
        client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_bytes)
        yield client

@pytest.fixture(scope="function")
def dynamodb():
    with moto.mock_dynamodb():
        client = boto3.client("dynamodb", region_name=region_name)
        client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    "AttributeName": "identity/LineItemId",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "identity/LineItemId",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        yield client

def dynamo_object_serialize(python_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v)
        for k, v in python_obj.items()
    }

def test_dynamo_put(s3, dynamodb, monkeypatch, test_event):
    #ENV vars passed to lambda as part of docker image
    env_vars = {
        'REGION': region_name,
        'DYNAMO_TABLE_NAME': table_name
    }
    #Adding key value pairs to monkeypatch
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


    response = handler(test_event, {}, test_s3_client=s3, test_dynamodb_client=dynamodb)

    db_data = dynamodb.scan(TableName=table_name)
    items = db_data["Items"]
    
    assert response['statusCode'] == 200
    assert response['body'] == f'Processed 10 rows from {bucket_name}/{file_name}.'
    assert len(items) == 10
    assert list(items[0].keys()) == ['identity/LineItemId', 'identity/TimeInterval', 'bill/InvoiceId', 'bill/InvoicingEntity', 'bill/BillingEntity', 'bill/BillType', 'bill/PayerAccountId', 'bill/BillingPeriodStartDate', 'bill/BillingPeriodEndDate', 'lineItem/UsageAccountId', 'lineItem/LineItemType', 'lineItem/UsageStartDate', 'lineItem/UsageEndDate', 'lineItem/ProductCode', 'lineItem/UsageType', 'lineItem/Operation', 'lineItem/AvailabilityZone', 'lineItem/ResourceId', 'lineItem/UsageAmount', 'lineItem/NormalizationFactor', 'lineItem/NormalizedUsageAmount', 'lineItem/CurrencyCode', 'lineItem/UnblendedRate', 'lineItem/UnblendedCost', 'lineItem/BlendedRate', 'lineItem/BlendedCost', 'lineItem/LineItemDescription', 'lineItem/TaxType', 'lineItem/NetUnblendedRate', 'lineItem/NetUnblendedCost', 'lineItem/LegalEntity', 'product/ProductName', 'product/PurchaseOption', 'product/accessType', 'product/alarmType', 'product/attachmentType', 'product/availability', 'product/availabilityZone', 'product/backupservice', 'product/baseProductReferenceCode', 'product/brokerEngine', 'product/bundle', 'product/bundleDescription', 'product/bundleGroup', 'product/cacheEngine', 'product/cacheMemorySizeGb', 'product/cacheType', 'product/capacity', 'product/capacitystatus', 'product/category', 'product/ciType', 'product/classicnetworkingsupport', 'product/clockSpeed', 'product/cloudformationresourceProvider', 'product/component', 'product/computeFamily', 'product/computeType', 'product/concurrencyscalingfreeusage', 'product/connectionType', 'product/contentType', 'product/cputype', 'product/currentGeneration', 'product/data', 'product/dataTransfer', 'product/databaseEdition', 'product/databaseEngine', 'product/datastoreStoragetype', 'product/datatransferout', 'product/dedicatedEbsThroughput', 'product/deploymentOption', 'product/describes', 'product/description', 'product/directConnectLocation', 'product/directorySize', 'product/directoryType', 'product/directoryTypeDescription', 'product/disableactivationconfirmationemail', 'product/durability', 'product/ecu', 'product/edition', 'product/endpoint', 'product/endpointType', 'product/engineCode', 'product/enhancedNetworkingSupport', 'product/enhancedNetworkingSupported', 'product/equivalentondemandsku', 'product/eventType', 'product/executionFrequency', 'product/executionLocation', 'product/feeCode', 'product/feeDescription', 'product/fileSystemType', 'product/findingGroup', 'product/findingSource', 'product/findingStorage', 'product/flow', 'product/freeQueryTypes', 'product/freeTrial', 'product/freeUsageIncluded', 'product/frequencyMode', 'product/fromLocation', 'product/fromLocationType', 'product/fromRegionCode', 'product/georegioncode', 'product/gets', 'product/gpu', 'product/gpuMemory', 'product/granularity', 'product/graphqloperation', 'product/group', 'product/groupDescription', 'product/indexingSource', 'product/insightstype', 'product/instance', 'product/instanceFamily', 'product/instanceFunction', 'product/instanceName', 'product/instanceType', 'product/instanceTypeFamily', 'product/intelAvx2Available', 'product/intelAvxAvailable', 'product/intelTurboAvailable', 'product/io', 'product/license', 'product/licenseModel', 'product/location', 'product/locationType', 'product/logsDestination', 'product/marketoption', 'product/maxIopsBurstPerformance', 'product/maxIopsvolume', 'product/maxThroughputvolume', 'product/maxVolumeSize', 'product/maximumExtendedStorage', 'product/maximumStorageVolume', 'product/memory', 'product/memoryGib', 'product/memorytype', 'product/messageDeliveryFrequency', 'product/messageDeliveryOrder', 'product/meteringType', 'product/minVolumeSize', 'product/minimumStorageVolume', 'product/networkPerformance', 'product/newcode', 'product/normalizationSizeFactor', 'product/operatingSystem', 'product/operation', 'product/opsItems', 'product/origin', 'product/osLicenseModel', 'product/parameterType', 'product/physicalCpu', 'product/physicalGpu', 'product/physicalProcessor', 'product/platoclassificationtype', 'product/platoinstancename', 'product/platoinstancetype', 'product/platopricingtype', 'product/platopricingunittype', 'product/platoprotocoltype', 'product/platoresourceactionmetrics', 'product/platostoragename', 'product/platostoragetype', 'product/platotrafficdirection', 'product/platotransfertype', 'product/platousagetype', 'product/platovolumetype', 'product/portSpeed', 'product/preInstalledSw', 'product/pricingUnit', 'product/pricingplan', 'product/processorArchitecture', 'product/processorFeatures', 'product/productFamily', 'product/protocol', 'product/provider', 'product/provisioned', 'product/purchaseterm', 'product/queueType', 'product/ratetype', 'product/realtimeoperation', 'product/recipient', 'product/region', 'product/regionCode', 'product/requestDescription', 'product/requestType', 'product/resourceEndpoint', 'product/resourceType', 'product/rootvolume', 'product/routingTarget', 'product/routingType', 'product/runningMode', 'product/servicecode', 'product/servicename', 'product/sku', 'product/softwareIncluded', 'product/softwareType', 'product/standardGroup', 'product/standardStorage', 'product/standardStorageRetentionIncluded', 'product/steps', 'product/storage', 'product/storageClass', 'product/storageFamily', 'product/storageMedia', 'product/storageType', 'product/subcategory', 'product/subscriptionType', 'product/subservice', 'product/tenancy', 'product/throughput', 'product/throughputCapacity', 'product/throughputClass', 'product/tiertype', 'product/toLocation', 'product/toLocationType', 'product/toRegionCode', 'product/transferType', 'product/type', 'product/updates', 'product/usageFamily', 'product/usageVolume', 'product/usagetype', 'product/uservolume', 'product/vcpu', 'product/version', 'product/videoMemoryGib', 'product/virtualInterfaceType', 'product/volumeApiName', 'product/volumeType', 'product/vpcnetworkingsupport', 'product/withActiveUsers', 'pricing/LeaseContractLength', 'pricing/OfferingClass', 'pricing/PurchaseOption', 'pricing/RateCode', 'pricing/RateId', 'pricing/currency', 'pricing/publicOnDemandCost', 'pricing/publicOnDemandRate', 'pricing/term', 'pricing/unit', 'reservation/AmortizedUpfrontCostForUsage', 'reservation/AmortizedUpfrontFeeForBillingPeriod', 'reservation/EffectiveCost', 'reservation/EndTime', 'reservation/ModificationStatus', 'reservation/NetAmortizedUpfrontCostForUsage', 'reservation/NetAmortizedUpfrontFeeForBillingPeriod', 'reservation/NetEffectiveCost', 'reservation/NetRecurringFeeForUsage', 'reservation/NetUnusedAmortizedUpfrontFeeForBillingPeriod', 'reservation/NetUnusedRecurringFee', 'reservation/NetUpfrontValue', 'reservation/NormalizedUnitsPerReservation', 'reservation/NumberOfReservations', 'reservation/RecurringFeeForUsage', 'reservation/ReservationARN', 'reservation/StartTime', 'reservation/SubscriptionId', 'reservation/TotalReservedNormalizedUnits', 'reservation/TotalReservedUnits', 'reservation/UnitsPerReservation', 'reservation/UnusedAmortizedUpfrontFeeForBillingPeriod', 'reservation/UnusedNormalizedUnitQuantity', 'reservation/UnusedQuantity', 'reservation/UnusedRecurringFee', 'reservation/UpfrontValue', 'discount/EdpDiscount', 'discount/BundledDiscount', 'discount/TotalDiscount', 'savingsPlan/TotalCommitmentToDate', 'savingsPlan/SavingsPlanARN', 'savingsPlan/SavingsPlanRate', 'savingsPlan/UsedCommitment', 'savingsPlan/SavingsPlanEffectiveCost', 'savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod', 'savingsPlan/RecurringCommitmentForBillingPeriod', 'savingsPlan/StartTime', 'savingsPlan/EndTime', 'savingsPlan/OfferingType', 'savingsPlan/PaymentOption', 'savingsPlan/PurchaseTerm', 'savingsPlan/Region', 'savingsPlan/NetSavingsPlanEffectiveCost', 'savingsPlan/NetAmortizedUpfrontCommitmentForBillingPeriod', 'savingsPlan/NetRecurringCommitmentForBillingPeriod', 'resourceTags/user:Application', 'resourceTags/user:Developer', 'resourceTags/user:Environment', 'resourceTags/user:Name', 'resourceTags/user:map-migrated', 'resourceTags/user:map-migrated-app', 'resourceTags/user:name']


    

        