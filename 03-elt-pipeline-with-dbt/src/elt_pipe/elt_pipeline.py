from pathlib import Path
from time import time
from typing import Any, cast
from urllib.request import urlretrieve

import pandas as pd
import pyarrow.parquet as pq
from prefect import flow, task
from prefect_dbt.core.runner import PrefectDbtRunner
from prefect_dbt.core.settings import PrefectDbtSettings
from prefect_sqlalchemy import SqlAlchemyConnector

ELT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = ELT_ROOT.parents[2]
DBT_PROJECT_DIR = ELT_ROOT / "dbt" / "project" / "dbt_intro"
DBT_PROFILES_DIR = ELT_ROOT / "dbt" / "profiles"


@task(name="ELT Extract Task", retries=3, retry_delay_seconds=60, log_prints=True)
def extract(url: str, file_path: str) -> pd.DataFrame:
    """This function reads a Parquet file from a given URL and writes it to a local file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)
    df_taxi = pd.read_parquet(file_path)
    return df_taxi


@task(name="ELT Load Task", retries=3, retry_delay_seconds=60, log_prints=True)
def load(df_taxi: pd.DataFrame, file_path: str, table_name: str) -> None:
    """This function loads the raw data into a PostgreSQL database."""
    database_block = cast(
        SqlAlchemyConnector, SqlAlchemyConnector.load("ny-taxi-connector")
    )
    with database_block.get_connection(begin=True) as engine:
        df_taxi.head(n=0).to_sql(
            name=table_name, con=engine, if_exists="replace", index=False
        )

        parquet_file = pq.ParquetFile(file_path)
        for batch in parquet_file.iter_batches(batch_size=100000):
            start_time = time()
            batch_df = batch.to_pandas()
            batch_df.to_sql(f"{table_name}", engine, if_exists="append", index=False)
            end_time = time()
            print("Batch time: ", end_time - start_time)
        print("Finished writing to database!")


@task(name="dbt Transform Task", retries=3, retry_delay_seconds=60, log_prints=True)
def dbt_task() -> Any:
    settings = PrefectDbtSettings(
        profiles_dir=DBT_PROFILES_DIR,
        project_dir=DBT_PROJECT_DIR,
    )
    runner = PrefectDbtRunner(settings=settings)
    return [
        runner.invoke(["debug"]),
        runner.invoke(["seed"]),
        runner.invoke(["run"]),
    ]


@flow(name="ELT Flow")
def elt_flow(color: str, month: int) -> None:
    if color != "yellow":
        raise ValueError(
            "This tutorial dbt project currently supports only color='yellow'."
        )

    url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2025-{month:02}.parquet"
    file_path = str(REPO_ROOT / "data" / f"{color}_tripdata_2025-{month:02}.parquet")
    table_name = f"{color}_taxi"
    raw_data = extract(url_source, file_path)
    load(raw_data, file_path, table_name)
    dbt_task()


if __name__ == "__main__":
    color = "yellow"
    month = 1
    elt_flow(color, month)
