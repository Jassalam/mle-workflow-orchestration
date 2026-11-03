import os
from pathlib import Path
from time import time
from urllib.request import urlretrieve

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

PIPE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PIPE_ROOT.parents[2]


def _database_url() -> str:
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DATABASE", "ny_taxi")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def data_ingestion(table_name: str, url: str, file_path: str) -> None:
    """This script reads a Parquet file from a given URL and writes it to a PostgreSQL database."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)

    engine = create_engine(_database_url())

    df_taxi = pd.read_parquet(f"{file_path}")
    df_taxi.head(n=0).to_sql(
        name=table_name, con=engine, if_exists="replace", index=False
    )

    parquet_file = pq.ParquetFile(f"{file_path}")
    for batch in parquet_file.iter_batches(batch_size=100000):
        start_time = time()
        batch_df = batch.to_pandas()
        batch_df.to_sql(f"{table_name}", engine, if_exists="append", index=False)
        end_time = time()
        print("Batch time: ", end_time - start_time)
    print("Finished writing to database!")


if __name__ == "__main__":
    data_ingestion(
        table_name="yellow_taxi",
        url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
        file_path=str(REPO_ROOT / "data" / "yellow_tripdata_2025-01.parquet"),
    )
