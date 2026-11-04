# Process Walkthrough: Prefect ETL/ELT Workflows

A record of working through all three chapters of this repository end to end, including the results observed and the problems that had to be fixed along the way.

- **Date:** 2026-11-04
- **Platform:** macOS (Apple Silicon), Python 3.13, `uv`
- **Key versions:** Prefect 3.9.0, prefect-sqlalchemy 0.6.1, prefect-dbt 0.7.25, dbt 1.12.6 (postgres adapter 1.11.0), PostgreSQL 17, SQLAlchemy 2.0.57 (pinned, see [Issue 2](#issue-2--scheduled-runs-never-fire-sqlalchemy-21-incompatibility))

All commands are run from the project root.

---

## Chapter 01: Intro to Prefect

### Setup

```bash
# Terminal 1: local Prefect server (API + UI at http://127.0.0.1:4200)
uv run prefect server start

# Terminal 2: point the client at it
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uv run prefect config view   # shows PREFECT_API_URL='http://127.0.0.1:4200/api'
```

### Examples

| Script | Concept | Result |
|---|---|---|
| `example01-flow.py` | A single `@flow` | Flow `say-hello` reached `Completed()` and printed `Hello, world!` / `Hello ML Engineers!` |
| `example02-task.py` | `@task`s inside a flow | Flow `api-flow` ran `call_api` → `parse_fact`, printing `200` and a cat fact |
| `example03-subflow.py` | Flow calling a flow | Parent `main-flow` created a separately tracked child run `common-flow` |
| `example04-type-conversion.py` | Parameter validation | **Failed on purpose** with `y: Input should be a valid string` before any task ran |
| `example05-type-conversion-pydantic.py` | Explicit coercion with Pydantic | Succeeded: `a=42 b='0' c=55.0` |
| `example06-task-runner.py` | `.submit()` + `ThreadPoolTaskRunner` | Two tasks ran concurrently (`AAAA`/`BBBB` interleaved); total ~10.5 s instead of 15 s |

Run with `uv run python 01-intro-to-prefect/src/examples/<script>`.

**Takeaways:** `@flow` and `@task` turn ordinary functions into observable, retryable units. Subflows get their own run records. Type hints on flow parameters are enforced. `.submit()` is what enables concurrency.

---

## Chapter 02: ETL Pipeline with Prefect

### Steps

```bash
docker compose up -d                       # PostgreSQL 17 on localhost:5432, db ny_taxi
docker compose ps                          # wait for "healthy"
uv run python 02-etl-prefect/src/data_pipe/create_sqlalchemy_block.py   # block: ny-taxi-connector
uv run python 02-etl-prefect/src/data_pipe/etl_pipeline.py              # run ETL directly
uv run python 02-etl-prefect/src/data_pipe/etl_deploy.py                # register + serve deployment
```

### Results

- **Extract:** downloaded `yellow_tripdata_2025-01.parquet` (56 MB) into `data/`.
- **Transform:** filtered `passenger_count > 0`, going from 3,475,226 to **2,910,421** rows.
- **Load:** wrote to `public.yellow_taxi` in 100k-row chunks. The run took about 2 minutes, and `SELECT count(*)` returned 2,910,421, matching the transform output.
- **Deployment `Data Ingestion Flow/ETL Pipeline`:** an RRule schedule with the first run about 2 minutes after registration, then every 2 months. After the fix in Issue 2, the scheduled run fired on time and completed.

---

## Chapter 03: ELT Pipeline with Prefect + dbt

### Steps

Chapter 03 reuses the same server, PostgreSQL container and `ny-taxi-connector` block as Chapter 02.

```bash
uv run python 02-etl-prefect/src/data_pipe/create_sqlalchemy_block.py   # safe to re-run
uv run python 03-elt-pipeline-with-dbt/src/elt_pipe/elt_pipeline.py     # run ELT directly
uv run python 03-elt-pipeline-with-dbt/src/elt_pipe/elt_deploy.py       # register + serve
uv run prefect deployment run 'ELT Flow/ELT Pipeline'                   # optional: trigger now
```

### Results

- **Extract + Load:** loaded all **3,475,226** raw rows into `public.yellow_taxi`. In ELT the load step doesn't filter anything; filtering happens later in dbt.
- **dbt Transform Task:** `dbt debug` passed (connection OK), `dbt seed` loaded `prd.payment_type` (6 rows), and `dbt run` built 4 models, all passing:
  - `prd_stg.stg_yellow_taxi` (3,475,226 rows)
  - `prd_int.int_avg_tips`
  - `prd_int.int_avg_tips_per_payment_type`
  - `prd_driver.fct_best_tips`
- The direct run took about 2¼ minutes.

Final mart `prd_driver.fct_best_tips`:

| payment_type | avg_tip_amount | tip_status |
|---|---|---|
| Credit Card | 4.11 | good tip |
| Cash | 0.0027 | bad tip |
| No Charge | 0.0169 | bad tip |
| Dispute | 0.0440 | bad tip |
| Unknown | 0 | bad tip |

- **Deployment `ELT Flow/ELT Pipeline`:** after resolving Issue 3, both the manual trigger and the scheduled run completed. The next run is queued 2 months out.

---

## Issues encountered and how they were fixed

### Issue 1: Port 5432 already in use

**Symptom:** a different container (`ny-taxi-db`, postgres:17, from another project) was already bound to `localhost:5432`. That meant `docker compose up -d` couldn't start, and that container held its own `yellow_taxi` table, which the ETL flow would have overwritten.

**Fix:** stopped the other container. Its data is preserved, and the repo's own PostgreSQL was started in its place:

```bash
docker stop ny-taxi-db
docker compose up -d --wait
```

To switch back later: `docker compose down && docker start ny-taxi-db`.

### Issue 2: Scheduled runs never fire (SQLAlchemy 2.1 incompatibility)

**Symptom:** the deployment registered and served, but no scheduled run was ever created. The Prefect server log repeated this error on every scheduler tick:

```text
sqlalchemy.exc.InvalidRequestError: Can't evaluate bulk DML statement; please supply a bulk_dml decorated function
```

**Root cause:** `uv` had resolved SQLAlchemy 2.1.6, which is allowed because Prefect only requires `sqlalchemy<3`. Prefect 3.9.0's scheduler bulk-inserts flow runs as dicts containing a `"state"` key, and `FlowRun.state` is a `hybrid_property`. SQLAlchemy 2.1 now routes hybrid keys in bulk INSERTs through a `bulk_dml` setter that Prefect doesn't define, so every insert raises. Running a flow directly doesn't touch this code path, which is why Chapter 02's step 7 worked and only the scheduled deployment failed.

**Fix:** constrain SQLAlchemy in `pyproject.toml`, then re-lock:

```toml
[tool.uv]
constraint-dependencies = [..., "sqlalchemy<2.1"]
```

```bash
uv lock && uv sync      # sqlalchemy 2.1.6 -> 2.0.57
```

Restart the Prefect server and any serving processes afterwards so they load the new version.

### Issue 3: PostgreSQL crashed mid-load (host disk full)

**Symptom:** the first scheduled ELT run hung in `RUNNING`. The `ELT Load Task` failed with `server closed the connection unexpectedly`, followed by `Connection refused` on every retry and `500 Internal Server Error` responses from the Prefect API.

**Root cause:** the Mac's data volume was **99% full (2.4 GB free)**. Each full load of 3.4M rows writes several GB of table data and WAL into Docker's virtual disk, so PostgreSQL ran out of space and the container exited. Prefect's SQLite database lives on the same disk, which caused the 500s.

**Fix:** stopped the serving process, cleared Docker's build cache, and restarted PostgreSQL. It recovered cleanly from its WAL:

```bash
docker builder prune -af        # freed ~3.3 GB inside Docker's disk
docker compose up -d --wait
```

Free space on the host rose to about 4.4 GB, and the re-run of the deployment completed. **Recommendation:** keep at least 10 GB free before running these pipelines repeatedly.

---

## Useful UI pages (while the server runs)

| URL | What's there |
|---|---|
| http://127.0.0.1:4200/dashboard | Overall activity |
| http://127.0.0.1:4200/runs | Every flow run, with task timelines and logs |
| http://127.0.0.1:4200/deployments | `ETL Pipeline` and `ELT Pipeline` with their schedules |
| http://127.0.0.1:4200/blocks | The `ny-taxi-connector` block |

## Cleanup

```bash
pkill -f prefect                         # stop server + serving deployments
docker compose down                      # stop PostgreSQL (keeps data volume)
docker compose down -v                   # ...or also delete the data volume
uv run prefect server database reset -y  # optional: wipe local Prefect run history
docker start ny-taxi-db                  # optional: bring back the other project's DB
```
