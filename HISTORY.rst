=======
History
=======


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
