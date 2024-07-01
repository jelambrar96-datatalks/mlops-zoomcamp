from datetime import datetime

import pandas as pd
from deepdiff import DeepDiff

from batch import prepare_data


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():

    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]
    df = pd.DataFrame(data, columns=columns)

    prepared_df = prepare_data(df)
    print(prepared_df.shape)
    print(prepared_df)

    assert prepared_df.shape == (2, 5)

    expected_output = [
        ("-1", "-1", dt(1, 1), dt(1, 10)),
        ("1", "1", dt(1, 2), dt(1, 10)),
    ]

    expected_df = pd.DataFrame(expected_output, columns=columns)
    expected_df["duration"] = (df.tpep_dropoff_datetime - df.tpep_pickup_datetime).dt.total_seconds() / 60
    print("comparation df")
    comparation = DeepDiff(expected_df.to_json(orient='records'), 
                       prepared_df.to_json(orient='records'))

    print("expected df")
    print(expected_df)

    print("prepared_df")
    print(prepared_df)

    print("comparation")
    print(comparation)
    assert "values_changed" not in comparation

