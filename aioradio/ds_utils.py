"""utils.py."""

# pylint: disable=invalid-name
# pylint: disable=logging-fstring-interpolation
# pylint: disable=no-member
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
from platform import system
from tempfile import NamedTemporaryFile
from time import sleep, time

import boto3
import pandas as pd
from domino import Domino
from smb.SMBConnection import SMBConnection

from aioradio.psycopg2 import establish_psycopg2_connection
from aioradio.pyodbc import establish_pyodbc_connection, pyodbc_query_fetchall

warnings.simplefilter(action='ignore', category=UserWarning)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
MAX_SSL_CONTENT_LENGTH = (2 ** 31) - 1


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


def get_fice_institutions_map(db_config):
    """Get mapping of fice to college from mssql table."""

    result = {}
    with DbInfo(db_config) as target_db:
        query = "SELECT FICE, Institution FROM EESFileuploadAssignments WHERE FileCategory = 'EnrollmentLens'"
        rows = pyodbc_query_fetchall(conn=target_db.conn, query=query)
        result = {fice: institution for fice, institution in rows}

    return result


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


def get_domino_connection(secret_id, project, host, env='sandbox'):
    """Get domino connection."""

    secret_client = get_boto3_session(env).client("secretsmanager", region_name='us-east-1')
    api_key = secret_client.get_secret_value(SecretId=secret_id)['SecretString']
    return Domino(project=project, api_key=api_key, host=host)


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
            self.conn = establish_psycopg2_connection(
                host = creds['host'],
                user = creds['user'],
                password = creds['password'],
                database = self.config.get('database', ''),
                port = creds['port']
            )
            self.conn.autocommit = not self.config['rollback']
        elif self.config['db'] == 'pyodbc':
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
