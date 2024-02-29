"""utils.py."""

# pylint: disable=broad-except
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name
# pylint: disable=logging-fstring-interpolation
# pylint: disable=no-member
# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-boolean-expressions
# pylint: disable=unnecessary-comprehension
# pylint: disable=unused-argument

import base64
import csv
import json
import logging
import os
import pickle
import warnings
from math import cos, degrees, radians, sin
from platform import system
from tempfile import NamedTemporaryFile
from time import sleep, time

import boto3
import numpy as np
import pyarrow as pa
import pandas as pd
import polars as pl
from haversine import haversine, Unit
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus
from mlflow.tracking.client import MlflowClient
from pyspark.sql import SparkSession
from smb.SMBConnection import SMBConnection

warnings.simplefilter(action='ignore', category=UserWarning)
MAX_SSL_CONTENT_LENGTH = (2 ** 31) - 1

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)
c_format = logging.Formatter('%(asctime)s:   %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

spark = SparkSession.builder.getOrCreate()


############################### Databricks functions ################################


def db_catalog(env):
    """Return the DataBricks catalog based on the passed in environment."""

    catalog = ''
    if env == 'sandbox':
        catalog = 'dsc_sbx'
    elif env == 'prod':
        catalog = 'dsc_prd'

    return catalog


def ese_db_catalog(env):
    """Return the ESE DataBricks catalog based on the passed in environment."""

    catalog = ''
    if env == 'sandbox':
        catalog = 'ese_dev'
    elif env == 'stage':
        catalog = 'ese_stg'
    elif env == 'prod':
        catalog = 'ese_prd'

    return catalog


def sql_to_polars_df(sql):
    """Get polars DataFrame from SQL query results."""

    return pl.from_arrow(pa.Table.from_batches(spark.sql(sql)._collect_as_arrow()))


def does_db_table_exists(name):
    """Check if delta table exists in databricks."""

    exists = False
    try:
        spark.sql(f"describe formatted {name}")
        exists = True
    except Exception:
        pass

    return exists


def merge_spark_df_in_db(df, target, on, partition_by=None, stage_table=None, partition_by_values=None):
    """Convert spark DF to staging table than merge with target table in
    Databricks."""

    stage = f"{target}_stage" if stage_table is None else stage_table

    if not does_db_table_exists(target):
        if partition_by is None:
            df.write.option("delta.columnMapping.mode", "name").saveAsTable(target)
        else:
            df.write.option("delta.columnMapping.mode", "name").partitionBy(partition_by).saveAsTable(target)
    else:
        if partition_by is None:
            df.write.option("delta.columnMapping.mode", "name").mode('overwrite').saveAsTable(stage)
        else:
            df.write.option("delta.columnMapping.mode", "name").mode('overwrite').partitionBy(partition_by).saveAsTable(stage)

        on_clause = ' AND '.join(f'{target}.{col} = {stage}.{col}' for col in on)
        if partition_by_values is not None:
            explicit_separation = ' AND '.join(f'{target}.{col} IN ({str(values)[1:-1]})' for col, values in partition_by_values.items())
            on_clause += f' AND {explicit_separation}'
        match_clause = ', '.join(f'{target}.{col} = {stage}.{col}' for col in df.columns if col != 'CREATED_DATETIME')

        try:
            spark.sql(f'MERGE INTO {target} USING {stage} ON {on_clause} WHEN MATCHED THEN UPDATE SET {match_clause} WHEN NOT MATCHED THEN INSERT *').show()
            spark.sql(f'DROP TABLE {stage}')
        except Exception:
            spark.sql(f'DROP TABLE {stage}')
            raise


def merge_pandas_df_in_db(df, target, on, partition_by=None, stage_table=None):
    """Convert pandas DF to staging table than merge with target table in
    Databricks."""

    stage = f"{target}_stage" if stage_table is None else stage_table

    for col, dtype in df.dtypes.apply(lambda x: x.name).to_dict().items():
        if dtype == 'object':
            df[col] = df[col].astype('string[pyarrow]')
            df[col].mask(df[col].isna(), '', inplace=True)
        elif dtype == 'string':
            # pyspark will throw an exception if strings are set to <NA> so convert to empty string
            df[col].mask(df[col].isna(), '', inplace=True)

    if not does_db_table_exists(target):
        if partition_by is None:
            spark.createDataFrame(df).write.option("delta.columnMapping.mode", "name").saveAsTable(target)
        else:
            spark.createDataFrame(df).write.option("delta.columnMapping.mode", "name").partitionBy(partition_by).saveAsTable(target)
    else:
        if partition_by is None:
            spark.createDataFrame(df).write.option("delta.columnMapping.mode", "name").mode('overwrite').saveAsTable(stage)
        else:
            spark.createDataFrame(df).write.option("delta.columnMapping.mode", "name").mode('overwrite').partitionBy(partition_by).saveAsTable(stage)

        on_clause = ' AND '.join(f'{target}.{col} = {stage}.{col}' for col in on)
        match_clause = ', '.join(f'{target}.{col} = {stage}.{col}' for col in df.columns if col != 'CREATED_DATETIME')

        try:
            spark.sql(f'MERGE INTO {target} USING {stage} ON {on_clause} WHEN MATCHED THEN UPDATE SET {match_clause} WHEN NOT MATCHED THEN INSERT *').show()
            spark.sql(f'DROP TABLE {stage}')
        except Exception:
            spark.sql(f'DROP TABLE {stage}')
            raise


def write_constants_to_db(df, df_library='pandas'):
    """Write constants defined in a dataframe to databricks.

    Each constant value is defined as a JSON string. See schema of
    dsc_prd.student_data.constants.
    """

    table = f"{db_catalog('prod')}.student_data.constants"

    if df_library.lower() == 'pandas':
        merge_spark_df_in_db(spark.createDataFrame(df), table, on=['key'])
    elif df_library.lower() == 'polars':
        merge_spark_df_in_db(spark.createDataFrame(df.to_pandas()), table, on=['key'])
    elif df_library.lower() == 'spark':
        merge_spark_df_in_db(df, table, on=['key'])
    else:
        logger.info(f'Unknown dataframe library {df_library}')


def read_constants_from_db(constants_list=None):
    """Read all constants or pass in a list to filter constants."""

    table = f"{db_catalog('prod')}.student_data.constants"
    where_clause = f'WHERE key in ({str(constants_list)[1:-1]})' if constants_list is not None else ''
    mapping = {i['key']: json.loads(i['value']) for i in sql_to_polars_df(f'SELECT * FROM {table} {where_clause}').to_dicts()}

    return mapping


def promote_model_to_production(model_name, tags):
    """Transition new model to production in Databricks."""

    client = MlflowClient()

    # current registered version
    new_model = client.get_latest_versions(name=model_name, stages=["None"])
    logger.info(f"new_model: {new_model}")
    new_version = new_model[0].version
    logger.info(f"new_version: {new_version}")

    # Add tags to registered model
    for key in tags:
        value = client.get_run(new_model[0].run_id).data.tags[key]
        client.set_model_version_tag(model_name, new_version, key, value)

    # current production version
    current_production = client.get_latest_versions(name=model_name, stages=["Production"])
    if len(current_production) > 0:
        current_production_version = current_production[0].version
        logger.info(f"current_production_version: {current_production_version}")
    else:
        current_production_version = None

    # ensure current version is ready
    for _ in range(10):
        model_version_details = client.get_model_version(name=model_name, version=new_version)
        status = ModelVersionStatus.from_string(model_version_details.status)
        logger.info(f"Model status: {ModelVersionStatus.to_string(status)}")
        if status == ModelVersionStatus.READY:
            break
        time.sleep(1)

    registered_model = client.get_registered_model(model_name)
    logger.info(f"registered_model: {registered_model}")

    # transition to production
    client.transition_model_version_stage(name=model_name, version=new_version, stage="Production")

    # archive previous model (can also delete, but that is permanent)
    if current_production_version is not None and new_version != current_production_version:
        client.transition_model_version_stage(name=model_name, version=current_production_version, stage="Archived")


################################## DataFrame functions ####################################


def convert_pyspark_dtypes_to_pandas(df):
    """The pyspark toPandas function converts strings to objects.

    This function takes the resulting df and converts the object dtypes
    to string[pyarrow], then it converts empty strings to pd.NA.
    """

    for col, dtype in df.dtypes.apply(lambda x: x.name).to_dict().items():

        if dtype == 'object':
            df[col] = df[col].astype('string[pyarrow]')
            df[col] = df[col].mask(df[col] == '', pd.NA)
        elif (dtype.startswith('int') or dtype.startswith('float')) and not dtype.endswith('[pyarrow]'):
            df[col] = df[col].astype(f'{dtype}[pyarrow]')
        elif 'string' in dtype:
            df[col] = df[col].astype('string[pyarrow]')
            df[col] = df[col].mask(df[col] == '', pd.NA)

    return df


def remove_pyarrow_dtypes(df):
    """Switch pyarrow dtype to non pyarrow dtype (int8['pyarrow'] to int8)"""

    df = df.astype({k: v.replace('[pyarrow]', '') for k, v in df.dtypes.apply(lambda x: x.name).to_dict().items()})
    return df


################################## AWS functions ####################################


def get_boto3_session(env):
    """Get Boto3 Session."""

    aws_profile = os.getenv('AWS_PROFILE')

    try:
        if aws_profile is not None:
            del os.environ['AWS_PROFILE']
        aws_creds = get_aws_creds(env)
        boto3_session = boto3.Session(**aws_creds)
    except ValueError:
        if aws_profile is not None:
            os.environ["AWS_PROFILE"] = aws_profile
        boto3_session = boto3.Session()

    return boto3_session


def file_to_s3(s3_client, local_filepath, s3_bucket, key):
    """Write file to s3."""

    start = time()
    s3_client.upload_file(Filename=local_filepath, Bucket=s3_bucket, Key=key)
    logger.info(f"Uploading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")


def get_secret(s3_client, secret_name):
    """Get secret from AWS Secrets Manager."""

    secret = ''
    response = s3_client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString'] if 'SecretString' in response else base64.b64decode(response['SecretBinary'])

    return secret


def update_secret(s3_client, secret_name, secret_value):
    """Update secret value in AWS Secrets Manager."""

    return s3_client.put_secret_value(SecretId=secret_name, SecretString=secret_value)


def list_s3_objects(s3_client, s3_bucket, s3_prefix, with_attributes=False):
    """List all objects within s3 folder."""

    arr = []
    paginator = s3_client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        for item in result.get('Contents', []):
            if with_attributes:
                arr.append(item)
            else:
                arr.append(item['Key'])

    return arr


def delete_s3_object(s3_client, bucket, s3_prefix):
    """Delete object(s) from s3."""

    return s3_client.delete_object(Bucket=bucket, Key=s3_prefix)


def bytes_to_s3(s3_client, s3_bucket, key, body):
    """Write data in bytes to s3."""

    start = time()
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=body)
    logger.info(f"Uploading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")


def df_to_s3_as_csv(s3_client, s3_bucket, s3_config, df, keys):
    """Write dataframe to s3 as CSV."""

    with NamedTemporaryFile(delete=system() != 'Windows') as tmp:
        df.to_csv(tmp.name, index=False, encoding='utf-8')
        for key in keys:
            start = time()
            s3_client.upload_file(Filename=tmp.name, Bucket=s3_bucket, Key=key, Config=s3_config)
            logger.info(f"Uploading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")
        tmp.close()


def df_to_s3_as_parquet(s3_client, s3_bucket, s3_config, df, keys):
    """Write dataframe to s3 as Parquet."""

    with NamedTemporaryFile(delete=system() != 'Windows') as tmp:
        df.to_parquet(tmp.name, index=False)
        for key in keys:
            start = time()
            s3_client.upload_file(Filename=tmp.name, Bucket=s3_bucket, Key=key, Config=s3_config)
            logger.info(f"Uploading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")
        tmp.close()


def list_of_dict_to_s3_as_csv(s3_client, s3_bucket, s3_config, list_of_dict, keys):
    """Write list of dict to s3 as CSV."""

    with NamedTemporaryFile(delete=system() != 'Windows') as tmp:
        writer = csv.DictWriter(tmp, fieldnames=list_of_dict[0].keys())
        writer.writeheader()
        writer.writerows(list_of_dict)
        for key in keys:
            start = time()
            s3_client.upload_file(Filename=tmp.name, Bucket=s3_bucket, Key=key, Config=s3_config)
            logger.info(f"Uploading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")
        tmp.close()


def get_large_s3_csv_to_df(s3_client, s3_bucket, key, usecols=None, na_values=None, dtype=None, encoding='utf-8'):
    """Convert a large CSV file in s3 to a pandas dataframe."""

    df = pd.DataFrame()
    with NamedTemporaryFile(delete=system() != 'Windows') as tmp:
        start = time()
        for chunk in s3_client.get_object(Bucket=s3_bucket, Key=key)["Body"].iter_chunks(MAX_SSL_CONTENT_LENGTH):
            tmp.write(chunk)
        tmp.seek(0)
        df = pd.read_csv(tmp.name, usecols=usecols, na_values=na_values, encoding=encoding, dtype=dtype, engine='pyarrow')
        tmp.close()
        logger.info(f"Downloading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")

    return df


def get_large_s3_parquet_to_df(s3_client, s3_bucket, key, usecols=None):
    """Convert a large CSV file in s3 to a pandas dataframe."""

    df = pd.DataFrame()
    with NamedTemporaryFile(delete=system() != 'Windows') as tmp:
        start = time()
        for chunk in s3_client.get_object(Bucket=s3_bucket, Key=key)["Body"].iter_chunks(MAX_SSL_CONTENT_LENGTH):
            tmp.write(chunk)
        tmp.seek(0)
        df = pd.read_parquet(tmp.name, columns=usecols)
        tmp.close()
        logger.info(f"Downloading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")

    return df


def get_s3_pickle_to_object(s3_client, s3_bucket, key):
    """Convert a pickle file in s3 to a pandas dataframe."""

    start = time()
    obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
    data = pickle.loads(obj["Body"].read())
    logger.info(f"Downloading s3://{s3_bucket}/{key} took {round(time() - start, 2)} seconds")

    return data


def get_aws_creds(env):
    """Get AWS credentials from environment variables."""

    suffix = 'PROD' if env.lower() == 'prod' else 'SAND'
    aws_creds = {
        'aws_access_key_id': os.environ.get(f"AWS_ACCESS_KEY_ID_{suffix}", None),
        'aws_secret_access_key': os.environ.get(f"AWS_SECRET_ACCESS_KEY_{suffix}", None),
        'aws_session_token': os.environ.get(f"AWS_SESSION_TOKEN_{suffix}", None)
    }
    if aws_creds['aws_session_token'] is None:
        del aws_creds['aws_session_token']

    if aws_creds['aws_access_key_id'] is None:
        raise ValueError(f"AWS_ACCESS_KEY_ID_{suffix} environment variable missing!")
    if aws_creds['aws_secret_access_key'] is None:
        raise ValueError(f"AWS_SECRET_ACCESS_KEY_{suffix} environment variable missing!")

    return aws_creds


get_s3_csv_to_df = get_large_s3_csv_to_df
get_s3_parquet_to_df = get_large_s3_parquet_to_df


################################# Misc functions ####################################


def bearing(slat, elat, slon, elon):
    """Bearing function."""

    slat, elat, slon, elon = radians(slat), radians(elat), radians(slon), radians(elon)
    var_dl = elon - slon
    var_x = cos(elat) * sin(var_dl)
    var_y = cos(slat) * sin(elat) - sin(slat) * cos(elat) * cos(var_dl)
    return (degrees(np.arctan2(var_x, var_y)) + 360) % 360


def apply_bearing(dataframe, latitude, longitude):
    """Apply bearing function on split dataframe."""

    return dataframe.apply(lambda x: bearing(x.LATITUDE, latitude, x.LONGITUDE, longitude), axis=1)


def apply_haversine(dataframe, latitude, longitude):
    """Apply haversine function on split dataframe."""

    return dataframe.apply(lambda x: haversine((x.LATITUDE, x.LONGITUDE), (latitude, longitude), unit=Unit.MILES), axis=1)


def logit(x, a, b, c, d):
    """Logit function."""

    return a / (1 + np.exp(-c * (x - d))) + b


def apply_logit(dataframe, a, b, c, d):
    """Apply logit function on split dataframe."""

    return dataframe.apply(lambda x: logit(x, a, b, c, d))


def get_fice_institutions_map(db_config):
    """Get mapping of fice to college from mssql table."""

    from aioradio.pyodbc import pyodbc_query_fetchall

    result = {}
    with DbInfo(db_config) as target_db:
        query = "SELECT FICE, Institution FROM EESFileuploadAssignments WHERE FileCategory = 'EnrollmentLens'"
        rows = pyodbc_query_fetchall(conn=target_db.conn, query=query)
        result = {fice: institution for fice, institution in rows}

    return result


def get_ftp_connection(secret_id, port=139, is_direct_tcp=False, env='sandbox'):
    """Get SMB Connection."""

    secret_client = get_boto3_session(env).client("secretsmanager", region_name='us-east-1')
    creds = json.loads(secret_client.get_secret_value(SecretId=secret_id)['SecretString'])
    conn = SMBConnection(
        creds['user'],
        creds['password'],
        secret_id,
        creds['server'],
        use_ntlm_v2=True,
        is_direct_tcp=is_direct_tcp
    )
    conn.connect(creds['server'], port)

    return conn


def monitor_domino_run(domino, run_id, sleep_time=10):
    """Monitor domino job run and return True/False depending if job was
    successful."""

    status = None
    while status is None:
        sleep(sleep_time)
        result = domino.runs_status(run_id)
        if result['status'] in ["Finished", "Succeeded"]:
            status = True
            break
        if result['status'] in ["Failed", "Error"]:
            status = False
            break

    return status


def get_domino_connection(secret_id, project, host, env='sandbox'):
    """Get domino connection."""

    from domino import Domino
    secret_client = get_boto3_session(env).client("secretsmanager", region_name='us-east-1')
    api_key = secret_client.get_secret_value(SecretId=secret_id)['SecretString']
    return Domino(project=project, api_key=api_key, host=host)


######################## Postgres or MSSQL Connection Classes #######################


class DB_CONNECT():
    """[Class for database connection]

    DB_CONNECT can be used to return a connection object, and will work
    within the "with" context to appropriately commit or rollback
    transactions based on the current environment.  Uses env vars
    instead of AWS secret manager for sensitive creds.
    """
    config = None
    conn = None

    def __init__(self, config: dict):
        self.config = config

    def __enter__(self):
        self.conn = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            if self.config.get('rollback', True):
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

    def connect(self):
        """[Setup database connection.]

        Create and return a connection using the config info. Autocommit
        will be on by default unless the rollback setting is set to true
        in db_info Autocommit can be changed if needed once the
        connection is returned
        """

        if self.config['db'] == 'psycopg2':

            from aioradio.psycopg2 import establish_psycopg2_connection

            self.conn = establish_psycopg2_connection(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config.get('database', ''),
                port=self.config['port']
            )
            self.conn.autocommit = not self.config['rollback']

        elif self.config['db'] == 'pyodbc':

            from aioradio.pyodbc import establish_pyodbc_connection

            self.conn = establish_pyodbc_connection(
                host=self.config['host'],
                user=self.config['user'],
                pwd=self.config['pwd'],
                port=self.config.get('port', None),
                multi_subnet_failover=self.config.get('multi_subnet_failover', None),
                database=self.config.get('database', ''),
                driver=self.config.get('driver', '{ODBC Driver 17 for SQL Server}'),
                autocommit = not self.config.get('rollback', True),
                trusted_connection=self.config.get('trusted_connection', 'no'),
                tds_version=self.config.get('tds_version', '7.4'),
                application_intent=self.config.get('application_intent', '')
            )

        return self.conn


class DbInfo():
    """[Class for database connection]

    db_info can be used to return a connection object, and will work
    within the "with" context to appropriately commit or rollback
    transactions based on the current environment.
    """
    config = None
    conn = None

    def __init__(self, config: dict):
        self.config = config

    def __enter__(self):
        self.conn = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            if self.config['rollback']:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

    def connect(self):
        """[Setup database connection.]

        Create and return a connection using the config info. Autocommit
        will be on by default unless the rollback setting is set to true
        in db_info Autocommit can be changed if needed once the
        connection is returned
        """

        secret_client = get_boto3_session(self.config['env']).client("secretsmanager", region_name='us-east-1')
        creds = json.loads(secret_client.get_secret_value(SecretId=self.config['secret'])['SecretString'])
        if self.config['db'] == 'psycopg2':
            from aioradio.psycopg2 import establish_psycopg2_connection
            self.conn = establish_psycopg2_connection(
                host = creds['host'],
                user = creds['user'],
                password = creds['password'],
                database = self.config.get('database', ''),
                port = creds['port']
            )
            self.conn.autocommit = not self.config['rollback']
        elif self.config['db'] == 'pyodbc':
            from aioradio.pyodbc import establish_pyodbc_connection
            self.conn = establish_pyodbc_connection(
                host = creds['host'],
                user = creds['user'],
                pwd = creds['pwd'],
                port = creds['port'] if 'port' in creds else None,
                multi_subnet_failover = creds.get('multi_subnet_failover', None),
                database = self.config.get('database', ''),
                driver = creds['driver'],
                autocommit = not self.config['rollback'],
                trusted_connection = self.config['trusted_connection'],
                tds_version=self.config['tds_version'],
                application_intent = self.config.get('application_intent', '')
            )

        return self.conn
