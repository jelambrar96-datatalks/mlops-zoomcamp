
import argparse
import pickle

import numpy as np
import pandas as pd


categorical = ['PULocationID', 'DOLocationID']


def read_data(filename):
    df = pd.read_parquet(filename)
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


if __name__ == '__main__':

    # reading inputs

    parser = argparse.ArgumentParser(description="Process year and month.")

    parser.add_argument('--year', type=int, required=True, help='Year in YYYY format')
    parser.add_argument('--month', type=int, required=True, choices=range(1, 13), help='Month as an integer (1-12)')

    args = parser.parse_args()

    year = args.year
    month = args.month


    # opening model
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)

    # downloading dataframe
    df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')

    # transform data
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    print(np.mean(y_pred))

    # output_file = f"results_{year:04d}-{month:02d}.parquet"
    # 
    # df_result = df.copy()
    # 
    # df_result['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    # df_result['y_pred'] = pd.Series(y_pred)
    # df_result = df_result[['ride_id', 'y_pred']]
    # 
    # df_result.to_parquet(
    #     output_file,
    #     engine='pyarrow',
    #     compression=None,
    #     index=False
    # )
