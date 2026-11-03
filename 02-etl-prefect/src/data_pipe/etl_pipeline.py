from pathlib import Path
from time import time
from typing import cast
from urllib.request import urlretrieve

import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

PIPE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PIPE_ROOT.parents[2]


@task(name="Extract Task", retries=3, retry_delay_seconds=60, log_prints=True)
def extract(url: str, file_path: str):
    """This function reads a Parquet file from a given URL and writes it to a local file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)
    df_taxi = pd.read_parquet(f"{file_path}")
    return df_taxi


@task(name="Transform Task", retries=3, retry_delay_seconds=60, log_prints=True)
def transform(df_taxi: pd.DataFrame):
    """This function transforms the taxi data. It will remove the taxi rides with 0 passengers."""
    print(f"Before transformation: {df_taxi.shape}")
    df_taxi = df_taxi[df_taxi.passenger_count > 0]
    print(f"After transformation: {df_taxi.shape}")
    return df_taxi


@task(name="Load Task", retries=3, retry_delay_seconds=60, log_prints=True)
def load(df_taxi: pd.DataFrame, file_path: str, table_name: str):
    """This function loads the transformed data into a PostgreSQL database."""
    del file_path
    database_block = cast(
        SqlAlchemyConnector, SqlAlchemyConnector.load("ny-taxi-connector")
    )
    with database_block.get_connection(begin=True) as engine:
        df_taxi.head(n=0).to_sql(
            name=table_name, con=engine, if_exists="replace", index=False
        )

        chunksize = 100000

        for batch in range(0, len(df_taxi), chunksize):
            start_time = time()
            batch_df = df_taxi[batch : batch + chunksize]
            batch_df.to_sql(f"{table_name}", engine, if_exists="append", index=False)
            end_time = time()
            print("Batch time: ", end_time - start_time)
        print("Finished writing to database!")


@flow(name="Data Ingestion Flow")
def main_flow(color: str, month: int):
    url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2025-{month:02}.parquet"
    file_path = str(REPO_ROOT / "data" / f"{color}_tripdata_2025-{month:02}.parquet")
    table_name = f"{color}_taxi"
    raw_data = extract(url_source, file_path)
    transformed_data = transform(raw_data)
    load(transformed_data, file_path, table_name)


if __name__ == "__main__":
    color = "yellow"
    month = 1
    main_flow(color, month)
