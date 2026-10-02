# aioradio

aioradio gives Encoura Python services one shared set of helpers for the external systems they talk to, so each service does not write its own S3, SQS, DynamoDB, Redis, database, Jira, and file-share clients. Enrollment file ingestion and data-science jobs use it to move files, run queries, and send notification email. The library is built on `asyncio`, `aiobotocore`/`aioboto3`, `httpx`, and `polars`/`pandas`.

## Architecture

- `aioradio/aws/` holds async wrappers for S3, SQS, DynamoDB, and Secrets Manager. `AwsServiceManager` in `aws/utils.py` creates one long-lived client per service and region when the module is imported, and recreates it every 300 seconds.
- `redis.py`, `pyodbc.py`, and `psycopg2.py` wrap Redis, MSSQL, and Postgres connections. The calling code supplies every host and credential.
- `file_ingestion.py` handles zip, Excel, and TSV conversion, SMB file-share operations, and Mandrill email.
- `ds_utils.py` holds Databricks Unity Catalog, Spark, MLflow, and S3 helpers for data-science work.
- `long_running_jobs.py` is an SQS-driven worker that caches job results in Redis.
- `logger.py` provides `JsonLogger` for JSON console logging.

## Development

Install Python 3.12 and the [Microsoft ODBC driver](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development), which `pyodbc` requires.

1. Create and activate a virtual environment:

    ```bash
    python3.12 -m venv env
    source env/bin/activate
    ```

2. Install dependencies:

    ```bash
    pip install cython
    pip install -r aioradio/requirements.txt
    ```

3. Run pre-commit, lint, tests, and an editable install in one step:

    ```bash
    make all
    ```

    To run a single stage, use `make lint`, `make test`, or `make setup`. `make test` sets `AWS_PROFILE=efi` and fails below 40% coverage.

## Deployment

aioradio ships as a package on PyPI. There is no automated release pipeline. To release:

1. Bump `version` in `setup.py` and add an entry to `HISTORY.rst`.
2. Merge to `main`.
3. Run `make twine` with PyPI credentials configured. It builds the sdist and wheel and uploads them.

Consuming services pick up the change when they raise their pinned `aioradio` version.

Two GitHub workflows run on `main`, and neither publishes the package:

- `.github/workflows/codeql-analysis.yml` runs CodeQL scanning on pushes and pull requests.
- `.github/workflows/techdocs.yml` publishes this documentation to Backstage when `docs/`, `mkdocs.yml`, or `catalog-info.yaml` changes.

## Operating this

- **Importing `aioradio.ds_utils` connects to Spark.** The module calls `SparkSession.builder.getOrCreate()` at import time, so the import fails anywhere Databricks Connect is not configured. Import it only in code that runs against Databricks.
- **SQS and DynamoDB clients exist only for `us-east-1` and `us-east-2`.** These clients are created at import. Call `add_regions()` before using any other region. Before v0.21.11, DynamoDB calls to `us-east-2` hung on the first scan or query because no client had been created yet.
- **`get_efi_excel_sheet_filter()` hides failures.** It catches every exception from the EFI `/filter/excel-sheet` call and returns an empty dict. If EFI is unreachable, Excel files are processed with no sheet filter, and nothing is logged.
- **Several drivers are not declared dependencies.** `setup.py` does not list `pyodbc`, `psycopg2`, `pyspark`, or `databricks-connect` in `install_requires`. A consuming service that uses those modules must install the drivers itself.

## Additional information

- [Module reference](modules.md): public functions and classes in each module.
- [AWS client lifecycle](aws-clients.md): how the shared AWS clients are created, reused, and refreshed.
- [Testing](testing.md): test fixtures, the moto server, and which tests are skipped.
