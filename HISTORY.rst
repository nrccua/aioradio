=======
History
=======


v0.21.10 (2025-03-20)

* Fix issue with param name exclude_cols, should be except_cols.


v0.21.9 (2025-03-20)

* Replace when_matched_update with when_matched_update_all adding exclude_cols parameter.


v0.21.8 (2025-03-19)

* Add quotes in match clause for merge_polars_in_db function, fixes an issue with merging when column names start with numbers.
* Add convenience function scan_db_table that breaks down the catalog, schema and table.
* Update aioboto3==14.1.0.
* Update boto3==1.37.1.
* Update botocore==1.37.1.
* Update ddtrace==3.2.1.
* Update polars==1.25.2.


v0.21.7 (2025-03-03)

* Add new ds utils function merge_polars_in_db to directly merge polars df into databricks unity catalog tables.


v0.21.6 (2025-02-12)

* Add configurable polling_interval parameter to pass into manage_async_tasks function.


v0.21.5 (2025-02-10)

* Update flask-cors==4.0.2.


v0.21.4 (2025-02-10)

* Update numpy==2.0.2.
* Update aioboto3==13.4.0.
* Update boto3==1.36.1.
* Update botocore==1.36.1.
* Update ddtrace==2.20.0.
* Update numpy==2.0.2.
* Update polars==1.22.0.
* Update pylint==3.3.4.
* Update setuptools==75.8.0.
* Update twine==6.1.0.
* Update typing_extensions==4.12.2.


v0.21.3 (2025-01-13)

* Update spark/polars functions to use arrow for better memory performance with databricks runtime 16.1+.


v0.21.2 (2025-01-06)

* Update allowable python version to be greater than or equal to 3.10.
* Update aioboto3==13.3.0.
* Update boto3==1.35.81.
* Update botocore==1.35.81.
* Update ddtrace==2.18.1.
* Update fakeredis==2.26.2.
* Update flask==3.1.0.
* Update httpx==0.28.1.
* Update mlflow==2.19.0.
* Update polars==1.17.1.
* Update psycopg2-binary==2.9.10.
* Update pylint==3.3.3.
* Update pyodbc==5.2.0.
* Update pyspark==3.4.4.
* Update python-json-logger==3.2.1.
* Update redis==5.2.1.
* Update twine==6.0.1.
* Update werkzeug==3.1.3.


v0.21.1 (2024-10-10)

* Add convenience funtion alter_db_table_column to add/drop databricks table columns.
* No longer require installing haversine.
* Update fakeredis==2.25.1.
* Update mlflow==2.16.2.
* Update pandas==2.2.3.
* Update polars==1.9.0.
* Update pylint==3.3.1.
* Update pysmb==1.2.10.
* Update redis==5.1.1.


v0.21.0 (2024-09-17)

* Update cython==3.0.11.
* Update fakeredis==2.24.1.
* Update httpx==0.27.2.
* Update numpy==1.26.4.
* Update polars==1.7.1.
* Update pre-commit==3.8.0.
* Update pylint==3.2.7.
* Update redis==5.0.8.
* Update werkzeug==3.0.4.
* Update wheel==0.44.0.


v0.20.24 (2024-07-26)

* Update install require numpy==1.23.5.


v0.20.23 (2024-07-25)

* Update install requires grpcio==1.62.2 & grpcio-status==1.62.2.


v0.20.22 (2024-07-25)

* Update install require numpy==1.26.4.


v0.20.21 (2024-07-25)

* Fix broken dependencies.


v0.20.20 (2024-07-25)

* Update mlflow, flask, flask-cors & werkzeug to remove vulnerability alert.
* Update python libs.
* Remove python lib dominodatalab.


v0.20.19 (2024-05-06)

* Add option to create polars LazyFrame from databricks query via pyarrow.


v0.20.18 (2024-04-06)

* Add all variations of env names to ds_utils databricks catalog functions.


v0.20.17 (2024-04-05)

* Improve logging spark merge query results using logger instead of show.


v0.20.16 (2024-03-19)

* Remove databricks session spark object and use original method to convert spark to polars df.


v0.20.15 (2024-03-16)

* Explicitly add databricks config to DatabricksSession.


v0.20.14 (2024-03-15)

* Fix bug with spark not being instantiated.


v0.20.13 (2024-03-15)

* Use spark df.toPandas() function instead of private function converted spark df -> pandas -> polars.


v0.20.12 (2024-03-12)

* Avoid instantiating spark if databricks-connect installed.


v0.20.11 (2024-03-12)

* Add pyspark back in as install requirement.


v0.20.10 (2024-03-12)

* Remove pyspark as an install requirement.


v0.20.9 (2024-02-29)

* Add new function promote_model_to_production in ds_utils.
* Update ddtrace==2.6.5.
* Update httpx==0.27.0.
* Update orjson==3.9.15.
* Update pandas==2.2.1.
* Update pyodbc==5.1.0.
* Update polars==0.20.13.
* Update redis==5.0.2.
* Update python to 3.12.2.
* Add .show() to end of spark.sql(...) to display merge statistics.


v0.20.8 (2024-02-19)

* Update aioboto3==12.3.0.
* Update boto3==1.34.34.
* Update botocore==1.34.34.
* Update dominodatalab==1.3.0.
* Update fakeredis==2.21.1.
* Update httpx==0.26.0.
* Update polars==0.20.10.
* Update pre-commit==3.6.2.
* Update pytest==8.0.1.
* Update twine==5.0.0.


v0.20.7 (2023-12-14)

* In merge_spark_df_in_db make separation explicit in the operation condition to avoid ConcurrentAppendException.


v0.20.6 (2023-12-14)

* Add stage_table optional argument to merge_spark_df_in_db ds.utils function.
* Update aioboto3==12.1.0.
* Update boto3==1.33.1.
* Update botocore==1.33.1.


v0.20.5 (2023-12-08)

* Add function ese_db_catalog in ds_utils.
* Update pandas==2.1.4.
* Update polars==0.19.19.


v0.20.4 (2023-12-01)

* Fix bug with sql statement in function ds_utils.read_constants_from_db.
* Update aiojobs==1.2.1.
* Update polars==0.19.18.


v0.20.3 (2023-11-29)

* Add Databricks functions to read/write constants for Data Science to use across projects.


v0.20.2 (2023-11-16)

* Hard-code 'us-east-1' and 'us-east-2' as regions in SQS AwsServiceManager.


v0.20.1 (2023-11-16)

* Add sqs.sync_add_regions for setting region besides us-east-1 during instantiation of LongRunningJobs.


v0.20.0 (2023-11-06)

* Update redis==5.0.1.
* Update fakeredis==2.20.0.


v0.19.4 (2023-11-03)

* Add common data science functions to ds_utils.py.
* Update cython==3.0.5.
* Update httpx==0.25.1.
* Update pandas==2.1.2.
* Update pylint==3.0.2.
* Update pytest==7.4.3.
* Update wheel==0.41.3.
* Add python library haversine==2.8.0.
* Add python library polars==0.19.12.
* Add python library pyarrow==13.0.0.
* Add python library pyspark==3.4.1.


v0.19.3 (2023-10-20)

* Add TrustServerCertificate option in sqlserver connection string, enabling use of driver {ODBC Driver 18 for SQL Server}.


v0.19.2 (2023-10-19)

* Add new DB_CONNECT class that uses env vars instead of AWS secret manager for sensitive creds.
* Update aioboto3==11.3.1.
* Update cython==3.0.4.
* Update httpx==0.25.0.
* Update pandas==2.1.1.
* Update pre-commit==3.5.0.
* Update psycopg2-binary==2.9.9.
* Update pylint==3.0.1.
* Update pytest==7.4.2.


v0.19.1 (2023-09-07)

* Set logging level in ds_utils.


v0.19.0 (2023-09-07)

* Only import domino within domino specific functions so it doesn't need to be installed when using ds_utils.
* Improve logging in ds_utils to log with timestamp before message.


v0.18.5 (2023-08-31)

* Remove dominodatalab as an install dependency.
* Update aioboto3==11.3.0.
* Update aiojobs==1.2.0.
* Update boto3==1.28.17.
* Update botocore==1.31.17.
* Update cython==3.0.2.
* Update faust-cchardet==2.1.19.


v0.18.4 (2023-07-21)

* Replace cchardet with faust-cchardet.
* Update cython==0.29.36.
* Update dominodatalab==1.2.4.

v0.18.3 (2023-06-07)

* Add excel sheet filter to file ingestion efi functions.


v0.18.2 (2023-04-18)

* Add psycopg2 imports inside functions and not at the top of the file.


v0.18.1 (2023-04-13)

* Remove psycopg2 as a required install with aioradio.


v0.18.0 (2023-03-22)

* Add support for python3.11.
* Make sure to pip install cython before installing the requirements file when using python3.11.
* Update ddtrace==1.9.4.
* Update dominodatalab==1.2.3.
* Update pandas==1.5.3.
* Update python-json-logger==2.0.7.
* Update wheel==0.40.0.


v0.17.36 (2023-03-13)

* Raise error instead of printing in file_ingestion function xlsx_to_tsv.


v0.17.35 (2023-02-23)

* Create function check_phone_number in efi parsing.
* Update bytes read in detect_delimiter function from 4096 to 8192.


v0.17.34 (2023-02-16)

* Revise ethnicity check in EFI and increase truncation limit.


v0.17.33 (2023-02-14)

* Hardcode openpyxl version to 3.0.10.


v0.17.32 (2023-02-13)

* Update csv sniffer to read 8192 bytes instea of 4096.


v0.17.31 (2023-02-03)

* Remove results from callback when error in LongRunningJobs.


v0.17.30 (2023-02-03)

* Update LongRunningJobs to gracefully deal with exceptions during narwhal execution.
* Update aioboto3==10.4.0.
* Update openpyxl==3.1.0.


v0.17.29 (2023-01-31)

* Update sleep time from 30 to 5 seconds in long running jobs after exception.
* Refactor long running jobs to no longer use redis queue.


v0.17.28 (2023-01-26)

* Add support to filter secrets in async_db_wrapper using the argument secret_json_key.


v0.17.27 (2023-01-20)

* Raise errors instead of printing in file_ingestion function zipfile_to_tsv.
* Update dominodatalab==1.2.2.
* Update pysmb==1.2.9.1.


v0.17.26 (2023-01-12)

* Add option to pass aws_creds argument in get_secret function.
* Update httpx==0.23.3.
* Update orjson==3.8.5.


v0.17.25 (2023-01-03)

* Update httpx==0.23.2.
* Update aioboto3==10.2.0.
* Update orjson==3.8.3.
* Update psycopg2-binary==2.9.5.


v0.17.24 (2022-12-01)

* Fix overwrite of delimiter parameter value in zipfile_to_tsv function.
* Update httpx==0.23.1.
* Update orjson==3.8.2.
* Update pylint==2.15.7.
* Update twine==4.0.2.


v0.17.23 (2022-11-30)

* Add account_id argument to SQS functions for cross account functionality.


v0.17.22 (2022-11-17)

* Fix issue with writing excel data to s3 for data ingestion project.
* Update dominodatalab==1.2.1.
* Update fakeredis==1.10.1.
* Update pyodbc==4.0.35.
* Update pytest-asyncio==0.20.2.
* Update wheel==0.38.4.


v0.17.21 (2022-11-08)

* Add zipfile to tsv/csv file function in file_ingestion.py.
* Add new unixodbc driver path: /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.1.1.


v0.17.20 (2022-11-02)

* Allow xlsx_to_tsv function to work on python3.7+ by removing python3.10 specific typing using |.
* Add openpyxl as a required dependency for aioradio.
* Update aiojobs==1.1.0.
* Update backoff==2.2.1.
* Update dominodatalab==1.2.0.
* Update orjson==3.8.1.
* Update pylint==2.15.5.
* Update pytest==7.2.0.
* Update pytest-asyncio==0.20.1.
* Update pytest-cov==4.0.0.


v0.17.19 (2022-10-28)

* Update xlsx_to_tsv function to use s3 instead of local directories for origin/destination files.


v0.17.18 (2022-10-21)

* Add a function in file_ingestion to convert an xlsx file to csv/tsv.
* Update fakeredis==1.9.4.
* Update pylint==2.15.4.


v0.17.17 (2022-10-01)

* Fixed botocore version to 1.27.59.


v0.17.16 (2022-09-28)

* Fix werkzeug version to 2.1.2.
* Update aioboto3==10.1.0.
* Update aiobotocore==2.4.0.
* Update boto3==1.24.59.
* Update dominodatalab==1.1.1.
* Update fakeredis==1.9.3.
* Update pylint==2.15.3.
* Update moto==3.1.18.


v0.17.15 (2022-09-15)

* Add python backoff library to install_requires in setup.


v0.17.14 (2022-09-15)

* Add backoff algorithm in aws/utils.py establish_client_resource function.
* Update ddtrace==1.3.6.
* Update fakeredis==1.9.1.
* Update orjson==3.8.0.
* Update pandas==1.4.4.
* Update pylint==2.15.2.
* Update pytest==7.1.3.


v0.17.13 (2022-08-30)

* Add 'env' key-value for dict object passed into Dbinfo.


v0.17.12 (2022-08-29)

* Add missing EL3 fields to file_injestion.py.


v0.17.11 (2022-08-17)

* Update aioboto3==10.0.0.
* Update aiobotocore==2.3.4.


v0.17.10 (2022-08-08)

* Update aiobotocore==2.3.0.
* Update aiojobs==1.0.0.
* Update ddtrace==1.3.2.


v0.17.9 (2022-08-01)

* Update httpx==0.23.0 to remove security vulnerability issue in github.
* Update aioboto3==9.6.0.
* Update ddtrace==1.3.0.
* Update dominodatalab==1.0.8.
* Update fakeredis==1.9.0.
* Update flask==2.1.3.
* Update orjson==3.7.11.
* Update pandas==1.4.3.
* Update pre-commit==2.20.0.
* Update pylint==2.14.5.
* Update pyodbc==4.0.34.
* Update pysmb==1.2.8.
* Update pytest-asyncio==0.19.0.
* Update python-json-logger==2.0.4.
* Update twine==4.0.1.

v0.17.8 (2022-05-05)

* Update get_ftp_connection function in ds_utils to allow configuration of is_direct_tcp.
* Update ddtrace==1.1.1.
* Update flask==2.1.2.
* Update orjson==3.6.8.
* Update pre-commit==2.19.0.
* Update pylint==2.13.8.
* Update pytest==7.1.2.


v0.17.7 (2022-05-03)

* Add update_secret function to ds_utils.


v0.17.6 (2022-04-14)

* Remove ip parameters from ds_utils function get_ftp_connection.
* Update ddtrace==1.0.0.
* Update pandas==1.4.2.


v0.17.5 (2022-03-31)

* Update aioboto3==9.5.0.
* Update aiobotocore==2.2.0.


v0.17.4 (2022-03-25)

* Update aioboto3==9.4.0.
* Update aiobotocore==2.1.2.


v0.17.3 (2022-02-18)

* Use dominodatalab installed from pypi instead of github.


v0.17.2 (2022-02-18)

* Adding converted async to sync functions to ds_utils.py.


v0.17.1 (2022-02-17)

* Adding a data science utils script that can be shared across project easily.


v0.17.0 (2022-02-11)

* Modify pyobdc & psycopg2 functions to no longer be async
* Update aiobotocore==2.1.0.
* Update httpx==0.22.0.
* Update moto==3.0.3.
* Update numpy==1.22.2.
* Update orjson==3.6.6.
* Update twine==3.8.0.
* Update pre-commit==2.17.0.
* Update pytest==7.0.0.

v0.16.2 (2022-01-13)

* Update EL3 field lengths.


v0.16.1 (2022-01-13)

* Adding UniqueID as new el3 field, in place of StudentID
* Removed logic for parsing phones numbers and text message opt in.


v0.16.0 (2022-01-11)

* Update ddtrace==0.57.0.
* Update moto==2.3.1.
* Update numpy==1.22.0.
* Update orjson==3.6.5.
* Update pre-commit==2.16.0.
* Update psycopg2-binary==2.9.3.
* Update pylint==2.12.2.
* Update pytest-asyncio==0.16.0.
* Update twine==3.7.1.
* Update wheel==0.37.1
* Add support for EL3 field parsing.


v0.15.6 (2021-11-30)

* Hard-code redis==3.5.3.
* Update ddtrace==0.56.0.
* Update fakeredis==1.7.0.
* Update moto==2.2.17.
* Update numpy==1.21.4.
* Update psycopg2-binary==2.9.2.
* Update pylint==2.12.1.
* Update twine==3.6.0.


v0.15.5 (2021-11-02)

* Fix import of establish_pyodbc_connection to not check sys.modules for pyodbc.


v0.15.4 (2021-11-01)

* Dates and years checks now use values from constants and we do not need to pass min/max into the corresponding functions:
 * check_date()
 * check_year()


v0.15.3 (2021-10-27)

* Move the import of pyodbc within the function that uses it since it requires additional dependencies.


v0.15.2 (2021-10-13)

* Fix error in FICE enrolled logic by setting enrolled field instead of confirmed.


v0.15.1 (2021-10-13)

* Improve shared EFI code by creating base functions for Datalab File Upload to use without using a list of one item.


v0.15.0 (2021-10-06)

* Shift common EFI logic into file_ingestion to be used by EFI and Datalab File Upload systems.


v0.14.5 (2021-10-06)

* Updating python modules and loosening versioning of sub-dependencies.


v0.14.4 (2021-09-27)

* Add function delete_many to Redis.


v0.14.3 (2021-07-30)

* Add SentTimestamp attribute to SQS get_messages function.


v0.14.2 (2021-07-22)

* Update aioboto3==9.2.0.


v0.14.1 (2021-07-22)

* Relaxed aioboto3, aiobotocore & boto3 version requirements to work with python3.7 since new versions of aioboto3 are limited to python3.8+.


v0.14.0 (2021-07-22)

* Update aioboto3==9.1.0, aiobotocore==1.3.3, boto3==1.17.106 & moto==2.1.0.


v0.13.18 (2021-07-21)

* Use json instead of data for callback.


v0.13.17 (2021-07-21)

* Update long running jobs callback to use data instead of params in post.


v0.13.16 (2021-07-21)

* Update boto3==1.16.52.


v0.13.15 (2021-07-21)

* Update aiobotocore==1.2.2.


v0.13.14 (2021-07-21)

* Assign fixed version for python packages.


v0.13.13 (2021-06-10)

* Add callback_url functionality in long_running_jobs.


v0.13.12 (2021-06-09)

* Remove logging in aioradio/aws/utils.py.
* Skip all tests interacting with FTP.


v0.13.11 (2021-06-03)

* Add SQL Server 2017 driver path for Debian operating systems.
* Moving pyodbc.connect port reference within the SERVER term (ex. "<server>,5123")


v0.13.8 (2021-06-02)

* Add application_intent & tds_version as pyodbc connection options.


v0.13.7 (2021-06-01)
-----------------------

* Add attr_before parameter in pyodbc.connect.


v0.13.5 (2021-04-27)
-----------------------

* Add functions (create_multipart_upload, upload_part, complete_multipart_upload, abort_multipart_upload, list_parts) to s3 multipart upload.

v0.13.4 (2021-04-22)
-----------------------

* Remove sensitive company email addresses along with database, redis and secret manager info.


v0.13.3 (2021-04-13)
-----------------------

* Fix issue with pulling more than one message in LongRunningJobs.


v0.13.2 (2021-04-13)
-----------------------

* Add ability for LongRunningJobs to run one to many jobs.


v0.13.1 (2021-04-13)
-----------------------

* Updating LongRunningJob to use either 'sqs' or 'redis' as the queue mechanism.


v0.13.0 (2021-04-12)
-----------------------

* Adding Long Running Job worker class to work asynchronously with client.


v0.12.5 (2021-03-23)
-----------------------

* Add Trusted_Connection to pyodbc options.


v0.12.4 (2021-03-17)
-----------------------

* Add ability to set encoding on Redis client.


v0.12.3 (2021-03-12)
-----------------------

* Use redis instead of aioredis because it is maintained much better by developers.
* Removed aioredis examples from README.md since using aioradio for redis has no benefit over simply using redis.


v0.12.0 (2021-03-08)
-----------------------

* Use aioredis transactions performance fixed branch (sean/aioredis-redis-py-compliance) instead of version 1.3.1.


v0.11.7 (2021-03-01)
-----------------------

* Fix syntax error in manage_async_tasks where append should be equal symbol.


v0.11.6 (2021-03-01)
-----------------------

* Simplify manage_async_tasks args to include list of coroutines.


v0.11.5 (2021-03-01)
-----------------------

* Add manage_async_tasks & manage_async_to_thread_tasks async functions in aioradio/utils.py.


v0.11.4 (2021-02-22)
-----------------------

* Use redis transactions via pipelining with hash set & expire commands.


v0.11.3 (2021-02-18)
-----------------------

* Fix pydoc errors in redis.py file.


v0.11.2 (2021-02-18)
-----------------------

* Add custom hmget_many & hmgetall_many redis commands to get many hashed keys data.


v0.11.1 (2021-02-18)
-----------------------

* Fix issue with sending None values in redis func hmget.


v0.11.0 (2021-02-18)
-----------------------

* Add initial support in redis for the hashes data structure.


v0.10.4 (2021-02-11)
-----------------------

* Add pyodbc driver string for windows OS.


v0.10.3 (2021-02-08)
-----------------------

* Modify async_wrapper to not directly use await within wrapper.


v0.10.2 (2021-02-08)
-----------------------

* Use await in async_db_wrapper instead of using asyncio.get_event_loop.run_until_complete.


v0.10.1 (2021-02-08)
-----------------------

* Add missing comma in install_requires.


v0.10.0 (2021-02-08)
-----------------------

* Add decorator to manage DB connections and using SQL transactions.


v0.9.8 (2021-02-01)
-----------------------

* Add ability to add more regions besides us-east-1 & us-east-2.


v0.9.7 (2021-01-06)
-----------------------

* Give async_wrapper decorator wrapper parent function name.


v0.9.6 (2020-12-22)
-----------------------

* Apply pydoc to repository.
* Add isort and docformatter to pre-commit.


v0.9.5 (2020-12-14)
-----------------------

* Fix bug with reseting list during paginate of list_objects.


v0.9.4 (2020-12-11)
-----------------------

* Adding the with_attributes parameter to list_s3_objects function.


v0.9.3 (2020-12-03)
-----------------------

* Add functions (get_ftp_file_attributes & get_s3_file_attributes) to retrieve metadata on files in FTP and S3.


v0.9.2 (2020-12-03)
-----------------------

* Update aioboto3==8.2.0


v0.9.1 (2020-11-17)
-----------------------

* Add ddtrace logger to DatadogLogger by default saving the user having to pass this info on their side.


v0.9.0 (2020-11-17)
-----------------------

* Set logger.propogate to False after adding handler.
* Remove use_ddtrace logic from DatadogLogger.


v0.8.5 (2020-11-11)
-----------------------

* Fix bug with accessing active keyword incorrectly in aws/utils.py aio_server function.


v0.8.4 (2020-10-27)
-----------------------

* Add documentation and usage examples for onboarding new users from pypi and github pages.


v0.8.3 (2020-10-26)
-----------------------

* Set busy flag to true on creation.


v0.8.2 (2020-10-26)
-----------------------

* Fix issue with incorrect busy signal.


v0.8.0 (2020-10-13)
-----------------------

* Replace python-utils repository with new name: aioradio.


v0.7.4 (2020-10-08)
-----------------------

* Add redis class object pool_task to run async task in fastapi startup function to create redis class pool object.


v0.7.3 (2020-10-08)
-----------------------

* Add async event loop logic when instantiating redis pool.


v0.7.2 (2020-10-07)
-----------------------

* Add boto3 to install_requires within setup.py.


v0.7.1 (2020-10-07)
-----------------------

* Fix missing None values in result from redis get_many_items function.
* Update aiobotocore==1.1.2.


v0.7.0 (2020-10-05)
-----------------------

* Remove fice institution mapping logic as it is too NRCCUA specific for an open source project.
* Refactored tests to no longer use AWS secrets manager for creds but use environment variables instead.


v0.6.10 (2020-10-01)
-----------------------

* Remove None values from redis build_cache_key function.
* Use fakeredis instead of real elasticache resource.


v0.6.7 (2020-09-29)
-----------------------

* Removed closing AioSession as it is unnecessary.


v0.6.6 (2020-09-29)
-----------------------

* Add ability to refresh aioboto3 client/resource every sleep interval.


v0.6.5 (2020-09-29)
-----------------------

* No longer closing the AioSession in utils.py.


v0.6.3 (2020-09-28)
-----------------------

* Converted using real AWS resources to implementing mock moto server with aiobotocore and aioboto3.


v0.6.2 (2020-09-24)
-----------------------

* Redis SET using orjson no longer decoding the cache value, but instead write the value as bytes.


v0.6.1 (2020-09-24)
-----------------------

* Replace ujson with orjson for faster serialization/deserialization.


v0.6.0 (2020-09-22)
-----------------------

* Add redis to python-utils.


v0.5.7 (2020-09-18)
-----------------------

* Fix bug with not passing in region to sqs client.


v0.5.6 (2020-09-18)
-----------------------

* Improved the implimentation of the aiojobs scheduler and active decorator by using a class in utils.py.


v0.5.5 (2020-09-17)
-----------------------

* Replace print statements with logger in sqs.py and s3.py.


v0.5.4 (2020-09-16)
-----------------------

* Improved AioSession refresh logic by setting client to None after exiting context manager.
* Adding logging in sqs.py and s3.py.


v0.5.3 (2020-09-15)
-----------------------

* Use asyncio.create_task instead of loop.run_until_complete.


v0.5.2 (2020-09-10)
-----------------------

* Fix bug with issuing raise out of scope.


v0.5.1 (2020-09-10)
-----------------------

* Fix bug with the active decorator counter not decrementing.
* Removed setting level of root logger.


v0.5.0 (2020-09-09)
-----------------------

* Add logging during reacquiring the s3 or sqs sessions.
* Refactor the Logger to DatadogLogger making it specific to use with Datadog.


v0.4.10 (2020-09-08)
-----------------------

* Improve get event logic in s3.py and sqs.py by always attempting to instantiate the using get_event_loop before using new_event_loop.


v0.4.9 (2020-09-08)
-----------------------

* Add missing await to asyncio.sleep


v0.4.8 (2020-09-08)
-----------------------

* Use asyncio.new_event_loop() in s3.py and sqs.py else use asyncio.get_event_loop() when running pytest.


v0.4.7 (2020-09-08)
-----------------------

* Removed uvloop from python-utils since it was causing issues with streamlit.


v0.4.6 (2020-09-08)
-----------------------

* Changed timeout value from 0.1 to 300 seconds in function establish_s3_client.


v0.4.5 (2020-09-08)
-----------------------

* Add waiting mechanism in active decorator until the client key is set.


v0.4.4 (2020-09-08)
-----------------------

* Add uvloop and aiojobs to install_requires in setup.py.


v0.4.3 (2020-09-08)
-----------------------

* Adding uvloop==0.14.0 to speed up the event loop.


v0.4.2 (2020-09-04)
-----------------------

* Replace aioboto3 with aiobotocore when appropriate.
* Adding a longer lasting AioSession client (5 minutes) for sqs and s3 for better performance.


v0.4.1 (2020-09-01)
-----------------------

* Update ddtrace from 0.40.0 to 0.41.2 to allow support for asgi integration.
* Adding a file_ingestion function delete_ftp_file.


v0.4.0 (2020-08-17)
-----------------------

* Separate pyodbc logic into its own module.


v0.3.10 (2020-07-30)
-----------------------

* Obtain complete objects streamed bytes from s3 get_object function.


v0.3.9 (2020-07-30)
-----------------------

* Add s3 function get_object to download contents of an s3 file directly.


v0.3.8 (2020-07-29)
-----------------------

* Add missing library httpx to python-utils package.


v0.3.7 (2020-07-28)
-----------------------

* Add generic jira functions post_jira_issue, get_jira_issue and add_comment_to_jira.
* Add s3 function upload_fileobj to basically upload a file using the file descriptor.
* Add dynamo function batch_get_items_from_dynamo to batch GET items.
* Add file ingestion function list_ftp_objects to list files & directory at an FTP path.


v0.3.6 (2020-07-24)
-----------------------

* Add async process manager using either threads or asyncio.gather that can manage a fix number of async processes.


v0.3.5 (2020-07-23)
-----------------------

* Add options use_ntlm_v2 & is_direct_tcp to establish_ftp_connection.


v0.3.4 (2020-07-22)
-----------------------

* Switch to using DNS instead of IP for FTP connection.
* Add async_wrapper function in file_ingestion that can be used as a decorator for DAG tasks to enable await usage.


v0.3.3 (2020-07-20)
-----------------------

* Using new secret names in tests.


v0.3.2 (2020-07-20)
-----------------------

* Use consistent AWS Secret Manager secret names across accounts and environments.


v0.3.1 (2020-07-17)
-----------------------

* Whenever importing package files prepend with python_utils.


v0.3.0 (2020-07-17)
-----------------------

* Add getting secrets from AWS Secrets Manager in python_utils/aws/secrets.py
* Removed pyodbc from install_required


v0.2.8 (2020-07-16)
-----------------------

* Hard-code version for each python package in requirements.txt.
* Fix missing comma between aioboto3 and ddtrace in setup.py.
* Adding a check of installing setup.py with the cmd: make all.


v0.2.7 (2020-07-16)
-----------------------

* Adding to install_requires in setup.py: aioboto3.


v0.2.6 (2020-07-16)
-----------------------

* Add the data folder and its contents to the package, currently to use fice_institution_mapping.xlsx.


v0.2.5 (2020-07-16)
-----------------------

* Fix spelling from pyobdc to pyodbc in setup.py


v0.2.4 (2020-07-16)
-----------------------

* Adding to install_requires in setup.py: mandrill, pyobdc, pysmb, & xlrd.


v0.2.3 (2020-07-15)
-----------------------

* Adding python package pytest-cov==2.10.0 with minimum coverage of 95% allowed.
* Extracting generic functions from EFI that appear to be appropriate for use across python projects.


v0.2.2 (2020-07-13)
-----------------------

* Added "python_utils/aws" directory to the packages in setup.py


v0.2.1 (2020-07-13)
-----------------------

* Generate v0.2.1 for initial release of python-utils


v0.2.0 (2020-07-13)
-----------------------

* Add async AWS library with initial support for common SQS, S3 & DynamoDB functions.


v0.1.8 (2020-07-07)
-----------------------

* Fix comparing console_logger with all_loggers list.


v0.1.7 (2020-07-07)
-----------------------

* Adjusting console logger to only add handler if the logger doesn't initially exist.


v0.1.6 (2020-07-07)
-----------------------

* Fill in readme
* Add pre-commit github action


v0.1.5 (2020-07-07)
-----------------------

* Creating release v0.1.5
* Adding tests to repository and pre-commit
* Allow for dynamic formatting of message.
* Add ddtrace==0.39.0 integrating with running via docker
* Add ability to install via setup.py.
* Add generic logger for either local or docker environment, which includes improved Datadog logging.
