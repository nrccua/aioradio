# Module reference

This page lists the public functions and classes in each module. Signatures and full argument descriptions are in each function's docstring.

## aioradio.aws.s3

Async S3 operations through a shared aiobotocore client. See [AWS client lifecycle](aws-clients.md).

- `create_bucket`, `upload_file`, `download_file`, `get_object`, `delete_s3_object`
- `list_s3_objects` returns keys under a prefix, or key attributes when `with_attributes=True`.
- `get_s3_file_attributes` returns an object's metadata.
- Multipart uploads: `create_multipart_upload`, `upload_part`, `list_parts`, `complete_multipart_upload`, `abort_multipart_upload`

## aioradio.aws.sqs

Async SQS operations, one client per region.

- `create_queue`, `send_messages`, `get_messages`, `delete_messages`, `purge_messages`
- `get_messages` long-polls for up to 20 seconds and returns up to 10 messages by default.
- Every function accepts an optional `account_id` for queues in another account.
- `add_regions` adds regions beyond `us-east-1` and `us-east-2`.

## aioradio.aws.dynamodb

Async DynamoDB operations through aioboto3, with a client and a resource per region.

- `create_dynamo_table`, `get_list_of_dynamo_tables`
- `put_item_in_dynamo`, `update_item_in_dynamo`, `batch_write_to_dynamo`
- `scan_dynamo`, `query_dynamo`, `batch_get_items_from_dynamo`
- `add_regions`

## aioradio.aws.secrets

- `get_secret(secret_name, region, aws_creds=None)` returns a Secrets Manager secret as a string, or as decoded bytes for binary secrets.

## aioradio.redis

The `Redis` dataclass wraps a `redis.Redis` connection to `config['redis_primary_endpoint']`. With `fake=True` it uses `fakeredis` instead.

- Keys: `get`, `mget`, `set`, `delete`, `delete_many` (deletes by pattern in batches of 500)
- Hashes: `hget`, `hmget`, `hmget_many`, `hgetall`, `hgetall_many`, `hset`, `hmset`, `hdel`, `hexists`
- `build_cache_key` joins a payload's keys and values into one key, and hashes it when `use_hashkey=True`.

Defaults: `expire=60` seconds, `use_json=True` (values are serialized with `orjson`), and `hash_algorithm='SHA3_256'`.

## aioradio.pyodbc

- `establish_pyodbc_connection` opens an MSSQL connection. On Linux and macOS it looks for a FreeTDS or Microsoft ODBC driver in a fixed list of paths (`UNIXODBC_DRIVER_PATHS`) and raises `FileNotFoundError` if none exists. It accepts `tds_version`, `trusted_connection`, `trust_server_certificate`, `multi_subnet_failover`, `application_intent`, and `autocommit`.
- `pyodbc_query_fetchone`, `pyodbc_query_fetchall`

## aioradio.psycopg2

- `establish_psycopg2_connection` opens a Postgres connection.

## aioradio.jira

Jira REST calls over `httpx` with basic auth.

- `post_jira_issue`, `get_jira_issue`, `add_comment_to_jira`

## aioradio.file_ingestion

Helpers for file processing.

- Decorators: `async_wrapper` runs an async function from sync code, such as a DAG task. `async_wrapper_using_new_loop` does the same on a fresh event loop. `async_db_wrapper` opens database connections from Secrets Manager credentials, passes them to the wrapped function, and closes them afterward.
- Archives: `unzip_file`, `unzip_file_get_filepaths`
- Conversion: `xlsx_to_tsv`, `zipfile_to_tsv`, `tsv_to_records`, `xlsx_to_records`, `tsv_to_s3`
- Detection: `detect_encoding` (uses `cchardet`), `detect_delimiter`
- SMB file shares: `establish_ftp_connection`, `list_ftp_objects`, `get_ftp_file_attributes`, `write_file_to_ftp`, `delete_ftp_file`
- Email: `send_emails_via_mandrill`
- `get_efi_excel_sheet_filter` fetches Excel sheet filters and returns an empty dict on any error.
- `get_current_datetime_from_timestamp`

## aioradio.ds_utils

Synchronous helpers for data-science jobs that run against Databricks. Importing the module starts a Spark session.

- Unity Catalog: `scan_db_table`, `merge_polars_in_db`, `merge_spark_df_in_db`, `merge_pandas_df_in_db`, `does_db_table_exists`, `alter_db_table_column`
- Catalog names: `db_catalog(env)` and `ese_db_catalog(env)` select a catalog for the requested environment.
- Conversion: `sql_to_polars`, `sql_to_polars_df`, `polars_to_spark`, `convert_pyspark_dtypes_to_pandas`, `remove_pyarrow_dtypes`
- Constants tables: `write_constants_to_db`, `read_constants_from_db`
- MLflow: `promote_model_to_production`
- Sync S3 through `boto3`: `get_boto3_session`, `get_aws_creds`, `file_to_s3`, `bytes_to_s3`, `list_s3_objects`, `delete_s3_object`, `df_to_s3_as_csv`, `df_to_s3_as_parquet`, `list_of_dict_to_s3_as_csv`, `get_large_s3_csv_to_df`, `get_large_s3_parquet_to_df`, `get_s3_pickle_to_object`
- Secrets: `get_secret`, `update_secret`
- Geometry and modeling: `bearing`, `apply_bearing`, `logit`, `apply_logit`
- `get_fice_institutions_map`, `get_ftp_connection`
- `DB_CONNECT` and `DbInfo` are context managers that open a database connection. On exit they roll back or commit depending on `config['rollback']`. `DB_CONNECT` reads credentials from environment variables. `DbInfo` reads them from the Secrets Manager secret named in `config['secret']`.

`get_aws_creds(env)` reads the AWS credentials for the requested environment from environment variables.

## aioradio.long_running_jobs

`LongRunningJobs` is a worker that pulls job requests from an SQS queue, runs the matching async function, and stores the result in Redis.

- Configure `jobs` as `{"job_name": (async_func, timeout_seconds)}`. Timeouts must be between 10 seconds and 5 hours.
- `send_message` queues a job and returns its UUID. If `cache_key` matches a job that already finished, the worker returns the cached result and does not rerun the job.
- `check_job_status` returns the job's state and result.
- `start_worker` and `stop_worker` control the polling loop.
- Cached results expire after `expire_cached_result` (default 86400 seconds), and job data after `expire_job_data` (default 3600 seconds).

## aioradio.utils

- `manage_async_tasks(items, concurrency)` runs `(coroutine, name)` pairs with at most `concurrency` running at once and returns results keyed by name.
- `manage_async_to_thread_tasks(func, items, concurrency)` runs a sync function in threads once per kwargs dict in `items`, with the same concurrency limit.

## aioradio.logger

- `JsonLogger(main_logger, logger_names)` attaches JSON stdout handlers to the named loggers. Use its `.logger` attribute.
- `DatadogLogger` is a compatibility alias for existing callers. It does not send anything to Datadog.
