import pandas as pd
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).astype(str)
    data['lpep_pickup_date_formatted'] = pd.to_datetime(data['lpep_pickup_date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%m-%d-%Y"))).dt.date
    data['lpep_pickup_timestamp'] = pd.Timestamp(data['lpep_pickup_date'])
    data.columns = (data.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower()) 
    print(data.dtypes)
    return data
