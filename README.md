# Workflow Orchestration

In this repository, you will explore how to orchestrate a workflow using [Prefect](https://docs.prefect.io/). Workflow orchestration is the process of automating the flow of data, tasks, and events across systems and services. You will start a local Prefect server, learn the building blocks of flows and tasks, and then build both an ETL and an ELT pipeline that load the [NYC Yellow Taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) into a local PostgreSQL database.

## Learning Objectives

By the end of this repository, you should be able to:

- Explain what workflow orchestration is and when to use Prefect.
- Define flows, tasks and subflows in Python and understand how Prefect tracks them.
- Run a local Prefect server and inspect flow runs in the UI.
- Validate flow parameters and explain the difference between strict validation and Pydantic-based coercion.
- Build an ETL pipeline that extracts data, transforms it, and loads it into PostgreSQL through a shared `SqlAlchemyConnector` block.
- Build an ELT pipeline where Prefect loads raw data and dbt handles the warehouse transformations.
- Register Prefect deployments and understand how scheduled and manual runs behave.

## Learning Path

### 01 - Intro to Prefect

Start a local Prefect server and learn the building blocks: flows, tasks, subflows, parameter validation and task runners.

| File | Description |
|---|---|
| [**01 - Intro to Prefect**](01-intro-to-prefect/01-intro-to-prefect.md) | Set up a local Prefect server, then work through flows, tasks, subflows, parameter validation and concurrency. |
| [**src**](01-intro-to-prefect/src/) | The example scripts you run throughout the lesson. |

### 02 - ETL Pipeline with Prefect

Orchestrate an ETL pipeline that loads taxi data into PostgreSQL, then serve it as a scheduled deployment.

| File | Description |
|---|---|
| [**02 - Data Pipeline with Prefect**](02-etl-prefect/02-data-pipeline-prefect.md) | Build the ETL flow step by step, from a plain script to a scheduled Prefect deployment. |
| [**src**](02-etl-prefect/src/) | The ingestion script, the ETL flow, the block bootstrap, and the deployment. |

### 03 - ELT Pipeline with dbt

Swap the transform step for dbt: Prefect loads the raw data, dbt models it inside PostgreSQL.

| File | Description |
|---|---|
| [**03 - ELT Pipeline with dbt**](03-elt-pipeline-with-dbt/03-elt-pipeline-with-dbt.md) | Build the ELT flow, run dbt models from Prefect, and serve the workflow as a deployment. |
| [**src**](03-elt-pipeline-with-dbt/src/) | The ELT flow, the deployment, and the dbt project. |

### Additional Folders and Files

| File / Folder | Description |
|---|---|
| [**data**](data/) | Local storage for the downloaded Parquet files. |
| [**assets**](assets/) | Visual aids referenced in the repository. |
| [**docker-compose.yaml**](docker-compose.yaml) | The local PostgreSQL service used in modules 02 and 03. |
| [**pyproject.toml**](pyproject.toml) | Project configuration and dependencies. |
| [**uv.lock**](uv.lock) | Dependency lock file. |

## Prerequisites

- **Docker Desktop**: required to run the local PostgreSQL database used throughout this course. Follow the [installation instructions](https://docs.docker.com/get-docker/) if you do not have it yet. Make sure it is **installed and running** before you start with the exercises.

## Setup

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) 0.13.5 or newer
and Python 3.13.17 or a newer 3.13 patch release, as defined in
[`.python-version`](.python-version).

> [!NOTE]
> Throughout these steps, text in angle brackets like `<repo-name>` is a **placeholder**. Replace it, including the `< >` brackets, with your own value. For example, `cd <repo-name>` becomes `cd mle-workflow-orchestration`.

### 1. Use This Repository as a Template

Click **Use this template** on GitHub.

When creating the repository:

- Set yourself as the **Owner**
- Choose a repository name
- Disable **Include all branches**
- Click **Create repository**

> [!IMPORTANT]
> If you are working in pairs or groups, only **one person** should complete this step.

---

### 2. Add Collaborators (Pairs/Groups Only)

If working with teammates:

1. Open the repository on GitHub
2. Go to **Settings → Collaborators**
3. Add your teammates as collaborators
4. Share the repository link with your team

Teammates should accept the invitation before continuing.

---

### 3. Clone the Repository

Copy the SSH URL from the **Code** button on GitHub, then run:

```bash
git clone <copied-ssh-url>
```

The copied SSH URL will look like `git@github.com:<your-username>/<repo-name>.git`.

---

### 4. Move into the Project Folder and Install Dependencies

This installs all dependencies and creates a virtual environment in `.venv/`.

```bash
cd <repo-name>
uv sync
```

---

### 5. Open the Repository in VS Code

> [!NOTE]
> Make sure you open VS Code from the project root so it automatically detects the environment created by `uv sync`.

Launch VS Code in the project root folder:

```bash
code .
```

Every command in the walkthrough runs through `uv run`, so this environment is used automatically.

## References & Further Reading

- [Prefect Docs](https://docs.prefect.io/): The official Prefect documentation.
- [Flows](https://docs.prefect.io/v3/concepts/flows): What a flow is, how it is tracked, and how parameters are handled.
- [Tasks](https://docs.prefect.io/v3/concepts/tasks): Task caching, retries and concurrency.
- [Blocks](https://docs.prefect.io/v3/concepts/blocks): How Prefect stores reusable configuration such as the SQLAlchemy connector.
- [Deployments](https://docs.prefect.io/v3/concepts/deployments): Turning a flow into something that runs on a schedule.
