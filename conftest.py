"""pytest configuration."""

import asyncio
import os
from itertools import chain

import aioboto3
import aiobotocore
import pytest
from aiobotocore.config import AioConfig

from aioradio.aws.dynamodb import DYNAMO
from aioradio.aws.moto_server import MotoService
from aioradio.aws.s3 import S3
from aioradio.aws.secrets import SECRETS
from aioradio.aws.sqs import SQS
from aioradio.redis import Redis


@pytest.fixture(scope='session')
def event_loop():
    """Redefine event_loop with scope set to session instead of function."""

    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def user():
    """Get the current USER environment variable value.

    Some tests need to be skipped if the user doesn't have access to an
    AWS service.
    """

    return os.getenv('USER')


@pytest.fixture(scope='module')
def payload():
    """Test payload to reuse."""

    return {
        'tool': 'pytest',
        'version': 'python3',
        'opinion': ['redis', 'rocks'],
        'none': None,
        'empty': []
    }

@pytest.fixture(scope='module')
def cache(github_action):
    """Redefine event_loop with scope set to session instead of function."""

    if github_action:
        pytest.skip('Skip test_set_one_item when running via Github Action')

    cache_object = Redis(config={
        'redis_primary_endpoint': 'prod-race2.gbngr1.ng.0001.use1.cache.amazonaws.com'
    })
    yield cache_object


def pytest_addoption(parser):
    """Command line argument --cleanse=false can be used to turn off address
    cleansing."""

    parser.addoption(
        '--github', action='store', default='false', help='pytest running from github action')


@pytest.fixture(scope='session')
def github_action(pytestconfig):
    """Return True/False depending on the --cleanse command line argument."""

    return pytestconfig.getoption("github").lower() == "true"


######### aiobotocore common async moto fixtures #########

def moto_config(endpoint_url):
    kw = dict(endpoint_url=endpoint_url,
              aws_secret_access_key="xxx",
              aws_access_key_id="xxx")

    return kw


def assert_status_code(response, status_code):
    assert response['ResponseMetadata']['HTTPStatusCode'] == status_code


@pytest.fixture(scope='session')
def session():
    session = aiobotocore.session.AioSession()
    return session


@pytest.fixture(scope='session')
def region():
    return 'us-east-2'


######### aiobotocore s3 async moto fixtures #########

@pytest.fixture(scope='module')
async def s3_client(session, region, s3_config, s3_server):
    kw = moto_config(s3_server)
    async with session.create_client('s3', region_name=region, config=s3_config, **kw) as client:
        real_client = S3['client']
        S3['client']['obj'] = client
        yield client
        S3['client'] = real_client


@pytest.fixture(scope='module')
def signature_version():
    return 's3'


@pytest.fixture(scope='module')
def s3_config(region, signature_version):
    return AioConfig(region_name=region, signature_version=signature_version, read_timeout=5, connect_timeout=5)

@pytest.fixture(scope='module')
async def s3_server():
    async with MotoService('s3', port=5001) as svc:
        yield svc.endpoint_url


@pytest.fixture(scope='module')
async def create_bucket(s3_client):
    _bucket_name = None

    async def _f(region_name, bucket_name):
        nonlocal _bucket_name
        _bucket_name = bucket_name
        bucket_kwargs = {'Bucket': bucket_name}
        if region_name != 'us-east-1':
            bucket_kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region_name,}
        response = await s3_client.create_bucket(**bucket_kwargs)
        assert_status_code(response, 200)
        await s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
        return bucket_name

    try:
        yield _f
    finally:
        await recursive_delete(s3_client, _bucket_name)


async def recursive_delete(s3_client, bucket_name):
    # Recursively deletes a bucket and all of its contents.
    paginator = s3_client.get_paginator('list_object_versions')
    async for n in paginator.paginate(
            Bucket=bucket_name, Prefix=''):
        for obj in chain(
                n.get('Versions', []),
                n.get('DeleteMarkers', []),
                n.get('Contents', []),
                n.get('CommonPrefixes', [])):
            kwargs = dict(Bucket=bucket_name, Key=obj['Key'])
            if 'VersionId' in obj:
                kwargs['VersionId'] = obj['VersionId']
            resp = await s3_client.delete_object(**kwargs)
            assert_status_code(resp, 204)

    resp = await s3_client.delete_bucket(Bucket=bucket_name)
    assert_status_code(resp, 204)


######### aiobotocore sqs async moto fixtures #########

@pytest.fixture(scope='module')
def sqs_config(region):
    return AioConfig(region_name=region, read_timeout=5, connect_timeout=5)


@pytest.fixture(scope='module')
async def sqs_server():
    async with MotoService('sqs', port=5002) as svc:
        yield svc.endpoint_url


@pytest.fixture(scope='module')
async def sqs_client(session, region, sqs_config, sqs_server):
    kw = moto_config(sqs_server)
    async with session.create_client('sqs', region_name=region, config=sqs_config, **kw) as client:
        real_client = SQS[region]['client']
        SQS[region]['client']['obj'] = client
        yield client
        SQS[region]['client'] = real_client


@pytest.fixture(scope='module')
async def sqs_queue_url(sqs_client):
    _queue_name = None

    async def _f(region_name, queue_name):
        nonlocal _queue_name
        _queue_name = queue_name
        response = await sqs_client.create_queue(QueueName=queue_name)
        queue_url = response['QueueUrl']
        assert_status_code(response, 200)
        return queue_url

    try:
        yield _f
    finally:
        await delete_sqs_queue(sqs_client, _queue_name)


async def delete_sqs_queue(sqs_client, queue_url):
    response = await sqs_client.delete_queue(
        QueueUrl=queue_url
    )
    assert_status_code(response, 200)


######### aiobotocore secretsmanager async moto fixtures #########

@pytest.fixture(scope='module')
def secrets_manager_config(region):
    return AioConfig(region_name=region, read_timeout=5, connect_timeout=5)


@pytest.fixture(scope='module')
async def secrets_manager_server():
    async with MotoService('secretsmanager', port=5003) as svc:
        yield svc.endpoint_url


@pytest.fixture(scope='module')
async def secrets_manager_client(session, region, secrets_manager_config, secrets_manager_server):
    kw = moto_config(secrets_manager_server)
    async with session.create_client('secretsmanager', region_name=region, config=secrets_manager_config, **kw) as client:
        real_client = SECRETS[region]['client']
        SECRETS[region]['client']['obj'] = client
        yield client
        SECRETS[region]['client'] = real_client


@pytest.fixture(scope='module')
async def create_secret(secrets_manager_client):
    result = await secrets_manager_client.create_secret(Name="test-secret", SecretString="abc123")
    assert result["ARN"]


######### aioboto3 dynamodb async moto fixtures  #########

@pytest.fixture(scope='module')
def dynamodb_config(region):
    return AioConfig(region_name=region, read_timeout=5, connect_timeout=5)


@pytest.fixture(scope='module')
async def dynamodb2_server():
    async with MotoService('dynamodb2', port=5004) as svc:
        yield svc.endpoint_url


@pytest.fixture(scope='module')
async def dynamodb_kw(dynamodb2_server):
    return moto_config(dynamodb2_server)

@pytest.fixture(scope='module')
async def dynamodb_session(dynamodb2_server):
    return aioboto3._get_default_session(region_name=region)


@pytest.fixture(scope='module')
async def dynamodb_client(dynamodb_session, dynamodb_kw, region, dynamodb_config):
    async with dynamodb_session.client('dynamodb', config=dynamodb_config, **dynamodb_kw) as client:
        real_client = DYNAMO[region]['client']
        DYNAMO[region]['client']['obj'] = client
        yield client
        DYNAMO[region]['client'] = real_client


@pytest.fixture(scope='module')
async def dynamodb_resource(dynamodb_session, dynamodb_kw, region, dynamodb_config):
    async with dynamodb_session.resource('dynamodb', config=dynamodb_config, **dynamodb_kw) as resource:
        real_resource = DYNAMO[region]['resource']
        DYNAMO[region]['resource']['obj'] = resource
        yield resource
        DYNAMO[region]['resource'] = real_resource


@pytest.fixture(scope='module')
async def create_table(dynamodb_client, dynamodb_resource):
    _table_name = None

    async def _is_table_ready(table_name):
        response = await dynamodb_client.describe_table(TableName=table_name)
        return response['Table']['TableStatus'] == 'ACTIVE'

    async def _f(table_name):
        nonlocal _table_name
        _table_name = table_name
        table_kwargs = {
            'TableName': table_name,
            'AttributeDefinitions': [
                {
                    'AttributeName': 'fice',
                    'AttributeType': 'S'
                },
            ],
            'KeySchema': [
                {
                    'AttributeName': 'fice',
                    'KeyType': 'HASH'
                },
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 1
            },
        }

        response = await dynamodb_client.create_table(**table_kwargs)
        while not (await _is_table_ready(table_name)):
            pass

        assert_status_code(response, 200)
        return response['TableDescription']['TableName']

    try:
        yield _f
    finally:
        await delete_table(dynamodb_client, _table_name)


async def delete_table(dynamodb_client, table_name):
    response = await dynamodb_client.delete_table(
        TableName=table_name
    )
    assert_status_code(response, 200)
