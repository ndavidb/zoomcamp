import io
import pyarrow
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

init_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"
year = "2022"
file_format = ".parquet"

@data_loader
def load_data_from_api(*args, **kwargs):
    urls = []
    dfs = []

    for i in range(12):
        month = f"0{i+1}"
        month = month[-2:]
        
        urls.append(f"{init_url}{year}-{month}{file_format}")

    for url in urls:
        dfs.append(pd.read_parquet(url, engine="pyarrow"))
    
    return pd.concat(dfs, ignore_index=True)
        


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
