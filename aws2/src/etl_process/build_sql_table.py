import pandas as pd
from datetime import datetime
import re

df = pd.read_csv('data.csv')
df = df.astype(str)
df = df.replace('nan','')
df['lineItem/LineItemDescription'] = df['lineItem/LineItemDescription'].str.replace('awskms','AWSKMS')
df['lineItem/LineItemDescription'] = df['lineItem/LineItemDescription'].str.replace('awswaf','AWSWAF')
df['reservation/SubscriptionId'] = df['reservation/SubscriptionId'].str.replace('"','')
df['lineItem/UsageEndDate'] = df['lineItem/UsageEndDate'].str.replace('02/06/2022', '2022-06-02T00:00:00Z')
df['lineItem/UsageEndDate'] = df['lineItem/UsageEndDate'].apply(lambda x: x+'Z' if x[-1] != 'Z' else x)

def dtype_function(x):
    if re.match('^-?\d+(?:\.\d+)?$',x):
        return 'double'
    elif re.match('^\d+$', x.strip()):
        return 'int'
    elif re.match('^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$',x):
        return 'timestamp'
    elif re.match('^\d{2}/\d{2}/\d{4}$', x):
        return 'timestamp'
    elif re.match('^[A-Za-z ]+$', x):
        return 'string'
    elif x == '':
        return 'string'
    elif re.match('^[A-Za-z0-9]+$',x):
        return 'string'
    elif re.match('^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', x):
        return 'string'
    elif re.match('^[A-Z0-9-]+:[A-Za-z0-9.-]+$', x):
        return 'string'
    else:
        return 'string'


column_list = []
df_test = df.applymap(dtype_function)
unique_values = {}
for column in df_test.columns:
    unique_values[column] = df_test[column].unique().tolist()[0]

new_dict = {} 
for key, value in unique_values.items():
    if key.split('/')[0] not in new_dict.keys():
        new_dict[key.split('/')[0]] = [{key.split('/')[1]:value}]
    else:
        new_dict[key.split('/')[0]].append({key.split('/')[1]:value})

# for t in new_dict.keys():
#     sql_string = f'CREATE EXTERNAL TABLE IF NOT EXISTS {t} ( \n'
#     #add fix to remove duplicate line item id from query
#     sql_string += 'LineItemId STRING \n'
#     for i in new_dict[t]:
#         sql_string += f'{list(i.keys())[0]} {list(i.values())[0]} \n'
#     sql_string += '''   )
#         PARTITIONED BY (account INT year INT, month INT, day INT)
#         STORED AS CSV
#         LOCATION 's3://${uploadBucket.bucketName}/data/'''
#     sql_string += f'{t} \n'
#     sql_string += '''TBLPROPERTIES ('csv.compress'='SNAPPY')'''
#     print(sql_string)
#     print('\n')
#     print('\n')

# f"""const table = new glue.CfnTable(this, '{t}', {
#       catalogId: cdk.Aws.ACCOUNT_ID,
#       databaseName: database.ref,
#       tableInput: {
#         name: 'my_table',
#         description: 'My Athena table',
#         parameters: {
#           'classification': 'csv',
#           'delimiter': ',',
#           'header': 'true',
#           'typeOfData': 'file'
#         },
#         storageDescriptor: {
#           columns: [
#             { name: 'column1', type: 'string' },
#             { name: 'column2', type: 'int' },
#             // add additional columns as needed
#           ],
#           location: `s3://${bucket.bucketName}/${objectPrefix}`,
#           inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
#           outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
#           serdeInfo: {
#             serializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
#             parameters: {
#               'separatorChar': ',',
#               'quoteChar': '\"',
#               'escapeChar': '\\'
#             }
#           }
#         },
#         partitionKeys: [
#           { name: 'partition_key1', type: 'string' },
#           { name: 'partition_key2', type: 'int' }
#           // add additional partition keys as needed
#         ],
#         tableType: 'EXTERNAL_TABLE'
#       }
#     });
# """

for t in new_dict.keys():
    table_string = f"const {t}table = new glue.CfnTable(this, '{t}Table',"
    table_string += "{ \n"
    table_string += "catalogId:env.account,\n"
    table_string += """databaseName: database.ref,
      tableInput: {"""
    table_string += f"name: 'aws-costs-{t}',\n"
    table_string += f"description: 'Athena table for storing {t} data for AWS costs',\n"
    table_string += """
    parameters: {
           'serialization.format': '1',
          'compressionType': 'snappy', // specify the compression codec as Snappy
          'delimiter': ',',
          'skip.header.line.count': '1'
        },
        storageDescriptor: {
          columns: [\n"""
    if t != 'identity':
        table_string += "{name: 'LineItemId', type: 'string'},"
    for i in new_dict[t]:
        table_string += "{"
        table_string += f"name: '{list(i.keys())[0]}', type: '{list(i.values())[0]}'"
        table_string += "}, \n"

    table_string += "],\n"
    table_string += "location: `s3://${uploadBucket.bucketName}/"
    table_string += f"{t}`, \n"
    table_string += """inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',\n
          outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',\n
          serdeInfo: {\n
            serializationLibrary: 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe', // specify the serde as LazyBinaryColumnarSerDe\n
            parameters: {\n
              'serialization.format': '1',\n
              'compressionType': 'snappy' // specify the compression codec as Snappy\n
            }\n
          }\n
        },\n
        partitionKeys: [\n"""
    table_string += "{ name: 'account', type: 'string' },\n"
    table_string += "{ name: 'year', type: 'int' },\n"
    table_string += "{ name: 'month', type: 'int' },\n"
    table_string += "{ name: 'day', type: 'string' },\n"
    table_string += "],\n"
    table_string += """tableType: 'EXTERNAL_TABLE'\n
      }
    });"""

    print(table_string)
    input('continue')