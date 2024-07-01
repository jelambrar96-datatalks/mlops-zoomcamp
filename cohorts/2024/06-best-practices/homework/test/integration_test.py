import os

from datetime import datetime

import pandas as pd

from batch import prepare_data, storage_options


"""
PARQUET_FILE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'

def integration_test():
    # download parquet file
    df_input = pd.read_parquet(PARQUET_FILE_URL)
    df_input.to_parquet(
        's3://bucket/file.parquet',
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=storage_options
    )
"""

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_storage():

    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)
    # df = prepare_data(df)

    df.to_parquet(
        's3://nyc-duration/in/2023-01.parquet',
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=storage_options
    )
