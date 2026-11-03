# Orchestrate a Data Pipeline with Prefect

Now that you have seen the basics of flows and tasks, this chapter turns a plain Python ingestion script into a small ETL workflow with Prefect.

The code for this chapter lives in [src/data_pipe](src/data_pipe) inside [02-etl-prefect](.). By the end of this walkthrough, you will be able to:

1. Start a local PostgreSQL database from this repository.
2. Create the Prefect block the ETL flow expects.
3. Run the ETL flow end-to-end.
4. Register and serve a deployment on a schedule.

## Before you start

As you work through this chapter:

- Run commands from the **project root**.
- You will use two terminals:
  - **Terminal 1** starts the Prefect server in step 1 and stays busy with it for the whole chapter.
  - **Terminal 2** runs every other command.
  - A third terminal is optional, and only at the very end.
- Make sure Docker Desktop is running.

This chapter uses a repo-owned local PostgreSQL setup through [docker-compose.yaml](../docker-compose.yaml), so you do not need to install and configure PostgreSQL manually to follow the tutorial.

The local defaults used throughout this chapter are:

- database: `ny_taxi`
- username: `postgres`
- password: `postgres`
- host: `localhost`
- port: `5432`

> **Note:** The ETL flow uses a Prefect `SqlAlchemyConnector` block named `ny-taxi-connector`. This chapter includes a bootstrap script that creates that block for you.

## What we are building

The ETL workflow in this repository is defined in [src/data_pipe/etl_pipeline.py](src/data_pipe/etl_pipeline.py).

It is built from three tasks:

- `extract(...)`
- `transform(...)`
- `load(...)`

and one flow:

- `main_flow(color, month)`

```mermaid
flowchart LR
    A["Extract<br/>download Parquet + read into DataFrame"] --> B["Transform<br/>filter rows with passenger_count > 0"]
    B --> C["Load<br/>write DataFrame to PostgreSQL"]
```

> **Why this matters:** Prefect gives you visibility into each ETL stage, which means you can tell whether the pipeline failed while downloading data, transforming it, or writing it to the database.

## Step 1 — Start the local Prefect server

Use **Terminal 1** and start Prefect (if the server from chapter 1 is not running anymore):

```bash
uv run prefect server start
```

What to expect:

- The command keeps running.
- Prefect starts a local API and UI.
- The UI is available at <http://127.0.0.1:4200>.

If you already walked through chapter 1, this should feel familiar.

## Step 2 — Point Prefect at that server

Use **Terminal 2** and configure your active Prefect profile:

```bash
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

To confirm the setting, run:

```bash
uv run prefect config view
```

You should see `PREFECT_API_URL='http://127.0.0.1:4200/api'` in the output.

## Step 3 — Start local PostgreSQL from this repo

Use **Terminal 2** and start PostgreSQL with Docker Compose:

```bash
docker compose up -d
```

What to expect:

- Docker starts a PostgreSQL container named
  `workflow-orchestration-postgres`.
- The database is exposed on `localhost:5432`.
- The database data is stored in a named Docker volume so it survives restarts.

To verify that PostgreSQL is healthy, run:

```bash
docker compose ps
```

You should see the `postgres` service in a healthy or running state.

If you want more detail, run:

```bash
docker compose logs postgres
```

```mermaid
flowchart LR
    T2["Terminal 2"] --> DC["docker compose up -d"]
    DC --> PG["PostgreSQL container<br/>localhost:5432"]
    PG --> VOL["Named Docker volume"]
```

> **What to notice:** This is the self-contained replacement for installing PostgreSQL manually. If PostgreSQL is not running, the ETL flow will fail in the `Load Task`.

## Step 4 — Create the Prefect SQLAlchemy block

The ETL code uses a Prefect block named `ny-taxi-connector`.

This repository includes a small bootstrap script for it in [src/data_pipe/create_sqlalchemy_block.py](src/data_pipe/create_sqlalchemy_block.py)

```python
from prefect_sqlalchemy import SqlAlchemyConnector

BLOCK_NAME = "ny-taxi-connector"
CONNECTION_INFO = "postgresql+psycopg2://postgres:postgres@localhost:5432/ny_taxi"


def main() -> None:
    block = SqlAlchemyConnector(connection_info=CONNECTION_INFO)
    block.save(BLOCK_NAME, overwrite=True)
    print(f"Created or updated Prefect block: {BLOCK_NAME}")


if __name__ == "__main__":
    main()
```

Use **Terminal 2** and run:

```bash
uv run python 02-etl-prefect/src/data_pipe/create_sqlalchemy_block.py
```

What to expect:

- The block is created or overwritten safely.
- The script prints:

```text
Created or updated Prefect block: ny-taxi-connector
```

You can confirm it exists in the Prefect UI under **Blocks**.

```mermaid
flowchart LR
    SCRIPT["create_sqlalchemy_block.py"] --> BLOCK["Prefect block<br/>ny-taxi-connector"]
    BLOCK --> DB["PostgreSQL<br/>postgresql+psycopg2://.../ny_taxi"]
```

> **What to notice:** The ETL flow now has a reproducible setup path. You don't need to click through the UI manually to create the block.

## Step 5 — Understand the plain ingestion script

The original ingestion logic is in [src/data_pipe/data_ingestion.py](src/data_pipe/data_ingestion.py).

That script:

- downloads a Parquet file,
- reads it into pandas,
- connects to PostgreSQL,
- writes the data in chunks.

Here is the important shape:

```python
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
```

This script works, but it is not yet a well-structured Prefect workflow:

- Everything happens inside one function.
- There is no explicit transform step.
- Prefect cannot show ETL stages separately in the UI.

## Step 6 — Split the logic into ETL tasks

The Prefect ETL version is implemented in [src/data_pipe/etl_pipeline.py](src/data_pipe/etl_pipeline.py).

### Extract

```python
@task(name="Extract Task", retries=3, retry_delay_seconds=60, log_prints=True)
def extract(url: str, file_path: str):
    """This function reads a Parquet file from a given URL and writes it to a local file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)
    df_taxi = pd.read_parquet(f"{file_path}")
    return df_taxi
```

### Transform

```python
@task(name="Transform Task", retries=3, retry_delay_seconds=60, log_prints=True)
def transform(df_taxi: pd.DataFrame):
    """This function transforms the taxi data. It will remove the taxi rides with 0 passengers."""
    print(f"Before transformation: {df_taxi.shape}")
    df_taxi = df_taxi[df_taxi.passenger_count > 0]
    print(f"After transformation: {df_taxi.shape}")
    return df_taxi
```

### Load

```python
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
```

### Flow

```python
PIPE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PIPE_ROOT.parents[2]


@flow(name="Data Ingestion Flow")
def main_flow(color: str, month: int):
    url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2025-{month:02}.parquet"
    file_path = str(REPO_ROOT / "data" / f"{color}_tripdata_2025-{month:02}.parquet")
    table_name = f"{color}_taxi"
    raw_data = extract(url_source, file_path)
    transformed_data = transform(raw_data)
    load(transformed_data, file_path, table_name)
```

```mermaid
flowchart TD
    F["main_flow(color, month)"] --> E["Extract Task"]
    E --> T["Transform Task"]
    T --> L["Load Task"]
    L --> DB["PostgreSQL table<br/>yellow_taxi / green_taxi / ..."]
```

> **What to notice:** This is now a real ETL workflow, not just an ingestion script. Each stage has a name, retry policy, logs, and a visible boundary in Prefect.

## Step 7 — Run the ETL flow

Use **Terminal 2** and run:

```bash
uv run python 02-etl-prefect/src/data_pipe/etl_pipeline.py
```

What to expect:

- Prefect starts a flow run named `Data Ingestion Flow`.
- The flow creates three task runs:
  - `Extract Task`
  - `Transform Task`
  - `Load Task`
- The Parquet file is downloaded into `data/`.
- The transform task logs the DataFrame shape before and after filtering.
- The load task logs chunk write timings.

If everything is set up correctly, the flow should finish in a `Completed()` state and appear in the Prefect UI.

Most likely failure modes:

- **Postgres container not running**
  - The flow fails during `Load Task`.
- **Prefect server not running**
  - Runs may not appear in the expected local UI.
- **`ny-taxi-connector` block missing**
  - The flow fails with:

```text
Unable to find block document named ny-taxi-connector for block type sqlalchemy-connector
```

![prefect ui](../assets/02-ETL.png)

## Step 8 — Create a deployment

Running the ETL flow directly is useful for development. A **deployment** makes it repeatable and schedulable.

The deployment script in this repo is [etl_deploy.py](src/data_pipe/etl_deploy.py).

```python
from datetime import UTC, datetime, timedelta

from etl_pipeline import main_flow
from prefect.client.schemas.schedules import RRuleSchedule

# Start the first run 2 minutes from now, then every 2 months on the same day and time
now = datetime.now(UTC) + timedelta(minutes=2)
rrule_string = (
    f"DTSTART:{now.strftime('%Y%m%dT%H%M%SZ')}\n"
    f"FREQ=MONTHLY;INTERVAL=2;BYMONTHDAY={now.day};BYHOUR={now.hour};BYMINUTE={now.minute}"
)


schedule_rule = RRuleSchedule(rrule=rrule_string, timezone="UTC")

if __name__ == "__main__":
    # Start the flow with a schedule
    main_flow.serve(
        name="ETL Pipeline",
        schedules=[
            schedule_rule,
        ],
        parameters={"color": "yellow", "month": 1},
        tags=["etl"],
    )
```

What this script does:

- Registers the deployment.
- Serves it locally.
- Schedules the first run about 2 minutes in the future.
- Repeats every 2 months using an RRule.

```mermaid
flowchart TD
    D["etl_deploy.py"] --> S["Create RRuleSchedule"]
    S --> SERVE["main_flow.serve(...)"]
    SERVE --> REG["Register deployment"]
    REG --> NEXT["Next scheduled run appears in UI"]
```

> **Why this matters:** A flow defines the workflow. A deployment defines how that workflow is run operationally.

## Step 9 — Run the deployment

Use **Terminal 2** and run:

```bash
uv run python 02-etl-prefect/src/data_pipe/etl_deploy.py
```

What to expect:

- The deployment is registered in Prefect.
- The terminal stays open and polls for scheduled runs.
- The first scheduled run should appear in the UI within about 2 minutes.

You should see output similar to:

```text
Your flow 'Data Ingestion Flow' is being served and polling for scheduled runs!
```

### Optional: Trigger a run immediately

If you simply wait, the scheduled run starts on its own.

To see a run right away instead, open a third terminal (Terminal 2 is busy serving) and run:

```bash
uv run prefect deployment run 'Data Ingestion Flow/ETL Pipeline'
```

> **Note:** Trigger it soon after the deployment starts serving. Otherwise your manual run may overlap with the scheduled one about 2 minutes in, and you will see two runs of the same flow.

Useful UI pages while this is running:

- <http://127.0.0.1:4200/flows>
- <http://127.0.0.1:4200/deployments>
- <http://127.0.0.1:4200/dashboard>

> **Note:** Keep the deployment terminal open. That process is what polls for scheduled runs.

## Common commands

| Command | Purpose |
| --- | --- |
| `docker compose up -d` | Start the local PostgreSQL dependency |
| `docker compose ps` | Check container status |
| `docker compose logs postgres` | Inspect PostgreSQL logs |
| `uv run prefect server start` | Start local Prefect API and UI |
| `uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api` | Point Prefect at your local server |
| `uv run python 02-etl-prefect/src/data_pipe/create_sqlalchemy_block.py` | Create the `ny-taxi-connector` block |
| `uv run python 02-etl-prefect/src/data_pipe/etl_pipeline.py` | Run the ETL flow directly |
| `uv run python 02-etl-prefect/src/data_pipe/etl_deploy.py` | Register and serve the ETL deployment |
| `uv run prefect deployment run 'Data Ingestion Flow/ETL Pipeline'` | Trigger the deployment manually |

## Useful local UI URLs

| URL | Purpose |
| --- | --- |
| <http://127.0.0.1:4200/dashboard> | View overall activity and recent runs |
| <http://127.0.0.1:4200/flows> | View registered flows and their runs |
| <http://127.0.0.1:4200/deployments> | View deployments and schedules |
| <http://127.0.0.1:4200/docs> | Open the Prefect API docs |

> **Warning:** These URLs work only while your local Prefect server is running.

## When you are done

Make sure you stop the Prefect processes and the local PostgreSQL container before starting the next chapter.

First, return to the terminals running `uv run prefect server start` and `uv run python 02-etl-prefect/src/data_pipe/etl_deploy.py`, then press `Ctrl + C` in each one.

If you already closed those terminals and need a fallback, stop Prefect processes:

```bash
# macOS, Linux, Windows Git-Bash
pkill -f prefect
```

```PowerShell
# Windows PowerShell
Get-Process prefect -ErrorAction SilentlyContinue | Stop-Process
```

> **Warning:** This stops **all** Prefect processes running on your machine.

Then stop the local PostgreSQL container:

```bash
docker compose down
```

If you also want to delete the local PostgreSQL data volume and start with an empty database next time, run:

```bash
docker compose down -v
```

Feel free to close the remaining terminals to have a fresh start.
