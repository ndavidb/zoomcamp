# Import modules
import pandas as pd
import requests
import argparse
from sqlalchemy import create_engine
from time import time


def preprocess_date_time_columns(dataframe):
    dataframe.lpep_dropoff_datetime = pd.to_datetime(dataframe.lpep_dropoff_datetime)
    dataframe.lpep_pickup_datetime = pd.to_datetime(dataframe.lpep_pickup_datetime)


def create_empty_table(df_columns, table_name: str, engine):
    df_columns.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")


def process_insert_green_taxi_data(engine, url: str, table_name: str, chunk_size: int = 100000):
    output: str = "output.gz"
    response = requests.get(url, allow_redirects=True)
    with open(output, 'wb') as handle:
        handle.write(response.content)

    df_iter = pd.read_csv(output, compression='gzip', iterator=True, chunksize=chunk_size, low_memory=False)
    df = next(df_iter)

    preprocess_date_time_columns(df)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            preprocess_date_time_columns(df)
            df.to_sql(table_name, con=engine, if_exists="append")
            t_end = time()

            print(f"Inserted another chunk...., it took {round(t_end - t_start, 3)}")

        except StopIteration:
            print("Data ingestion completed.")
            break


def process_insert_taxi_zone_data(engine, url: str, table_name: str):
    output: str = "output.csv"

    response = requests.get(url, allow_redirects=True)
    with open(output, 'wb') as handle:
        handle.write(response.content)

    taxi_zone_df = pd.read_csv(output)
    taxi_zone_df.to_sql(name=table_name, con=engine, if_exists='replace')


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    taxi_zone_table = params.taxi_zone_table
    green_taxi_table = params.green_taxi_table
    url_taxi_zone = params.url_taxi_zone
    url_green_taxi = params.url_green_taxi

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    process_insert_green_taxi_data(engine, url_green_taxi, green_taxi_table)
    process_insert_taxi_zone_data(engine, url_taxi_zone, taxi_zone_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='Username for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--db', help='Database name for postgres')
    parser.add_argument('--green_taxi_table', help='Name of the green taxi table where we will the results to')
    parser.add_argument('--taxi_zone_table', help='Name of the taxi zone table where we will the results to')
    parser.add_argument('--url_green_taxi', help='URL of the CSV file')
    parser.add_argument('--url_taxi_zone', help='URL of the CSV file')

    args = parser.parse_args()
    main(args)
