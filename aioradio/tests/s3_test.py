'''pytest s3'''

# pylint: disable=logging-fstring-interpolation

import logging

import pytest

from aioradio.aws.s3 import delete_s3_object
from aioradio.aws.s3 import download_file
from aioradio.aws.s3 import get_object
from aioradio.aws.s3 import get_s3_file_attributes
from aioradio.aws.s3 import list_s3_objects
from aioradio.aws.s3 import upload_file


LOG = logging.getLogger(__name__)

S3_BUCKET = 'pytest-nrccua'
S3_PREFIX = 'pytest-objects'

FILE_CONTENT = 'hello world of pytest!'
pytestmark = pytest.mark.asyncio


async def test_s3_creating_bucket(create_bucket):
    '''Create the mock S3 bucket.'''

    result = await create_bucket(region_name='us-east-1', bucket_name=S3_BUCKET)
    assert result == S3_BUCKET


async def test_s3_upload_file(tmpdir_factory):
    '''Test uploading file to s3. In addition will test deleting a file and listing objects.'''

    filename = 'hello_world.txt'
    path = str(tmpdir_factory.mktemp('upload').join(filename))
    with open(path, 'w') as file_handle:
        file_handle.write(FILE_CONTENT)

    s3_key = f'{S3_PREFIX}/{filename}'

    # First delete the file from s3 if it exists and verify it is gone from s3
    await delete_s3_object(bucket=S3_BUCKET, s3_prefix=s3_key)
    assert s3_key not in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    # Next upload the file from s3 and confirm it now exists in s3
    await upload_file(bucket=S3_BUCKET, filepath=path, s3_key=s3_key)
    assert s3_key in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)


async def test_s3_download_file(tmpdir_factory):
    '''Test uploading file to s3.'''

    filename = 'hello_world.txt'
    path = str(tmpdir_factory.mktemp('download').join(filename))

    s3_key = f'{S3_PREFIX}/{filename}'
    await download_file(bucket=S3_BUCKET, filepath=path, s3_key=s3_key)
    with open(path, 'r') as file_handle:
        data = file_handle.read()
        assert data == FILE_CONTENT


async def test_get_object():
    '''Test get_object from s3.'''

    result = await get_object(bucket=S3_BUCKET, s3_key=f'{S3_PREFIX}/hello_world.txt')
    assert result is not None


async def test_get_file_attributes():
    '''Test retrieving s3 object attributes.'''

    result = await get_s3_file_attributes(bucket=S3_BUCKET, s3_key=f'{S3_PREFIX}/hello_world.txt')
    assert result['ContentLength'] == 22
