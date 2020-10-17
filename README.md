# aioradio
Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more.

## INSTALLING FOR DIRECT DEVELOPMENT OF AIORADIO

Install [python 3.8.X](https://www.python.org/downloads/)

Make sure you've installed [ODBC drivers](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15), required for using the python package pyodbc.

Clone aioradio locally and navigate to the root directory

Install and activate python VirtualEnv
```bash
python3.8 -m venv env
source env/bin/activate
```

Install python modules included in requirements.txt
```bash
pip install -r aioradio/requirements.txt
```

Run Makefile command from the root directory to test all is good before issuing push to master
```
make all
```

## AUTHORS

* **Tim Reichard** - [aioradio](https://github.com/nrccua/aioradio)

See also the list of [contributors](https://github.com/nrccua/aioradio/graphs/contributors) who participated in this project.

## ACKNOWLEDGEMENTS

* **Bryan Cusatis** - Architect contributing to aioradio.
* **Kyle Edwards** - Developer contributing to aioradio.
* **Pedro Artiga** - Developer contributing to aioradio.
