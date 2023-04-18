#Set up
import pandas as pd
import os
from datetime import datetime
from random import choice

accounts = ['285916830885','667663686041']

#Set up 
if not os.path.isfile('sample.csv'):
    df = pd.read_csv('data.csv',dtype=str)
    df = df.replace('nan', None)
    df = df[['identity/LineItemId','lineItem/UsageEndDate','lineItem/UsageAccountId', 'resourceTags/user:map-migrated','bill/InvoiceId','lineItem/UnblendedCost']].head(10)
    df['lineItem/UsageEndDate'] = df['lineItem/UsageEndDate'].replace('02/06/2022', '2022-07-02T00:00:00Z')
    df['lineItem/UsageAccountId'] = df['lineItem/UsageAccountId'].apply(lambda x: choice(accounts))
    df.to_csv('sample.csv', index=False)
else:
    df = pd.read_csv('sample.csv')

print(df['lineItem/UnblendedCost'].astype(float).sum())

column_dict = {i: i.split('/')[0] for i in df.columns}
columns_grouped = {}
for key, value in column_dict.items():
    if value in columns_grouped:
        columns_grouped[value] = columns_grouped[value] + [key.split('/')[1]]

    else: 
        columns_grouped[value] = [key.split('/')[1]]




