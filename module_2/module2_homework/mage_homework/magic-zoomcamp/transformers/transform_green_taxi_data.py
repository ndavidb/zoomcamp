import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    print(data['lpep_pickup_date'].nunique())
    
    data.columns = (data.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower())

    return data


@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero distance'
    assert "vendor_id" in output.columns, 'The vendor_id columns has not been defined properly'
    