"""pytest s3."""

# pylint: disable=logging-fstring-interpolation

import logging

import pytest

from aioradio.aws.s3 import (abort_multipart_upload, complete_multipart_upload,
                             create_multipart_upload, delete_s3_object,
                             download_file, get_object, get_s3_file_attributes,
                             list_parts, list_s3_objects, upload_file,
                             upload_part)

LOG = logging.getLogger(__name__)

S3_BUCKET = 'pytest-nrccua'
S3_PREFIX = 'pytest-objects'

FILE_CONTENT = 'hello world of pytest!'
pytestmark = pytest.mark.asyncio


async def test_s3_creating_bucket(create_bucket):
    """Create the mock S3 bucket."""

    result = await create_bucket(region_name='us-east-1', bucket_name=S3_BUCKET)
    assert result == S3_BUCKET


async def test_s3_upload_file(tmpdir_factory):
    """Test uploading file to s3.

    In addition will test deleting a file and listing objects.
    """

    filename = 'hello_world.txt'
    path = str(tmpdir_factory.mktemp('upload').join(filename))
    with open(path, 'w', encoding='utf-8') as file_handle:
        file_handle.write(FILE_CONTENT)

    s3_key = f'{S3_PREFIX}/{filename}'

    # First delete the file from s3 if it exists and verify it is gone from s3
    await delete_s3_object(bucket=S3_BUCKET, s3_prefix=s3_key)
    assert s3_key not in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    # Next upload the file from s3 and confirm it now exists in s3
    await upload_file(bucket=S3_BUCKET, filepath=path, s3_key=s3_key)
    assert s3_key in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    result = await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX, with_attributes=True)
    assert 'LastModified' in result[0]


async def test_s3_download_file(tmpdir_factory):
    """Test uploading file to s3."""

    filename = 'hello_world.txt'
    path = str(tmpdir_factory.mktemp('download').join(filename))

    s3_key = f'{S3_PREFIX}/{filename}'
    await download_file(bucket=S3_BUCKET, filepath=path, s3_key=s3_key)
    with open(path, 'r', encoding='utf-8') as file_handle:
        data = file_handle.read()
        assert data == FILE_CONTENT


async def test_get_object():
    """Test get_object from s3."""

    result = await get_object(bucket=S3_BUCKET, s3_key=f'{S3_PREFIX}/hello_world.txt')
    assert result is not None


async def test_get_file_attributes():
    """Test retrieving s3 object attributes."""

    result = await get_s3_file_attributes(bucket=S3_BUCKET, s3_key=f'{S3_PREFIX}/hello_world.txt')
    assert result['ContentLength'] == 22

async def test_multipart_upload():
    """"Test a success case of multipart upload."""

    filename = 'multipart_upload_hello_world.txt'
    s3_key = f'{S3_PREFIX}/{filename}'

    # First delete the file from s3 if it exists and verify it is gone from s3
    await delete_s3_object(bucket=S3_BUCKET, s3_prefix=s3_key)
    assert s3_key not in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    # Create the multipart upload
    multipart_upload = await create_multipart_upload(bucket=S3_BUCKET, s3_key=s3_key)
    upload_id = multipart_upload["UploadId"]
    assert upload_id is not None

    # Upload a part
    part_number=1
    part_result = await upload_part(
        bucket=S3_BUCKET,
        s3_key=s3_key,
        part='Hello World/n',
        part_number=part_number,
        upload_id=upload_id)
    parts = [{'ETag': part_result['ETag'], 'PartNumber': part_number}]

    # Verify if the part was listed
    uploaded_parts = await list_parts(
        bucket=S3_BUCKET,
        s3_key=s3_key,
        upload_id=upload_id)
    assert len(uploaded_parts['Parts'])==part_number

    # If everythin looks fine, complete the multipart upload
    await complete_multipart_upload(
        bucket=S3_BUCKET,
        s3_key=s3_key,
        parts=parts,
        upload_id=upload_id
    )
    # Confirm the file now exists in s3
    assert s3_key in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

async def test_abort_multipart_upload():
    """"Test aborting a multipart upload."""

    filename = 'multipart_upload_hello_world.txt'
    s3_key = f'{S3_PREFIX}/{filename}'

    # First delete the file from s3 if it exists and verify it is gone from s3
    await delete_s3_object(bucket=S3_BUCKET, s3_prefix=s3_key)
    assert s3_key not in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    # Create multipart upload, so we can abort it
    multipart_upload = await create_multipart_upload(bucket=S3_BUCKET, s3_key=s3_key)
    upload_id = multipart_upload["UploadId"]
    assert upload_id is not None

    await abort_multipart_upload(
        bucket=S3_BUCKET,
        s3_key=s3_key,
        upload_id=upload_id
    )

    # Verify if the proccess was successfully aborted
    assert s3_key not in await list_s3_objects(bucket=S3_BUCKET, s3_prefix=S3_PREFIX)
