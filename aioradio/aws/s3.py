"""Generic async AWS functions for S3."""

import logging
from typing import Any, Dict, List

from aioradio.aws.utils import AwsServiceManager

LOG = logging.getLogger(__name__)
AWS_SERVICE = AwsServiceManager(service='s3')
S3 = AWS_SERVICE.service_dict


@AWS_SERVICE.active
async def create_bucket(bucket: str) -> Dict[str, str]:
    """Create an s3 bucket.

    Args:
        bucket (str): s3 bucket

    Returns:
        Dict[str, str]: response of operation
    """

    return await S3['client']['obj'].create_bucket(Bucket=bucket)


@AWS_SERVICE.active
async def upload_file(bucket: str, filepath: str, s3_key: str) -> Dict[str, Any]:
    """Upload file to s3.

    Args:
        bucket (str): s3 bucket
        filepath (str): local filepath to upload
        s3_key (str): destination s3 key for uploaded file

    Returns:
        Dict[str, Any]: response of operation
    """

    response = {}
    with open(filepath, 'rb') as fileobj:
        response = await S3['client']['obj'].put_object(Bucket=bucket, Key=s3_key, Body=fileobj.read())

    return response


@AWS_SERVICE.active
async def download_file(bucket: str, filepath: str, s3_key: str):
    """Download file to s3.

    Args:
        bucket (str): s3 bucket
        filepath (str): local filepath for downloaded file
        s3_key (str): s3 key to download
    """

    with open(filepath, 'wb') as fileobj:
        data = await get_object(bucket=bucket, s3_key=s3_key)
        fileobj.write(data)


@AWS_SERVICE.active
async def list_s3_objects(bucket: str, s3_prefix: str, with_attributes: bool=False) -> List[str]:
    """List objects in s3 path.

    Args:
        bucket (str): s3 bucket
        s3_prefix (str): s3 prefix
        with_attributes (bool, optional): return all file attributes in addition to s3 keys. Defaults to False.

    Returns:
        List[str]: [description]
    """

    arr = []
    paginator = S3['client']['obj'].get_paginator('list_objects')
    async for result in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        for item in result.get('Contents', []):
            if with_attributes:
                arr.append(item)
            else:
                arr.append(item['Key'])

    return arr


@AWS_SERVICE.active
async def get_s3_file_attributes(bucket: str, s3_key: str) -> Dict[str, Any]:
    """Get s3 objects metadata attributes.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 key

    Returns:
        Dict[str, Any]: response of operation
    """

    s3_object = await S3['client']['obj'].get_object(Bucket=bucket, Key=s3_key)
    del s3_object['Body']

    return s3_object


@AWS_SERVICE.active
async def get_object(bucket: str, s3_key: str) -> bytes:
    """Directly download contents of s3 object.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 key

    Returns:
        bytes: streaming of s3 key as data bytes
    """

    data = None
    s3_object = await S3['client']['obj'].get_object(Bucket=bucket, Key=s3_key)
    async with s3_object["Body"] as stream:
        data = await stream.read()

    return data


@AWS_SERVICE.active
async def delete_s3_object(bucket: str, s3_prefix: str) -> Dict[str, Any]:
    """Delete object(s) from s3.

    Args:
        bucket (str): s3 bucket
        s3_prefix (str): s3 prefix

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].delete_object(Bucket=bucket, Key=s3_prefix)

    return response

@AWS_SERVICE.active
async def create_multipart_upload(bucket: str, s3_key: str) -> Dict[str, Any]:
    """Create multipart upload from s3.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 prefix

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].create_multipart_upload(Bucket=bucket, Key=s3_key)

    return response

@AWS_SERVICE.active
async def upload_part(bucket: str, s3_key: str, part: str, part_number: int, upload_id: str) -> Dict[str, Any]:
    """Upload a single part to s3.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 prefix
        part (str): the content to be uploaded
        part_number (int): part number
        upload_id (str): multipart upload Id

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].upload_part(
        Bucket=bucket,
        Key=s3_key,
        Body=part,
        PartNumber=part_number,
        UploadId=upload_id)

    return response

@AWS_SERVICE.active
async def list_parts(bucket: str, s3_key: str, upload_id: str) -> Dict[str, Any]:
    """List parts already uploaded to s3.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 prefix
        upload_id (str): multipart upload Id

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].list_parts(
        Bucket=bucket,
        Key=s3_key,
        UploadId=upload_id)

    return response

@AWS_SERVICE.active
async def complete_multipart_upload(bucket: str, s3_key: str, parts: List, upload_id: str) -> Dict[str, Any]:
    """Complete multipart upload to s3.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 prefix
        parts (List): all parts info
        upload_id (str): multipart upload Id

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].complete_multipart_upload(
        Bucket=bucket,
        Key=s3_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts})

    return response

@AWS_SERVICE.active
async def abort_multipart_upload(bucket: str, s3_key: str, upload_id: str) -> Dict[str, Any]:
    """Abort multipart upload.

    Args:
        bucket (str): s3 bucket
        s3_key (str): s3 prefix
        upload_id (str): multipart upload Id

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await S3['client']['obj'].abort_multipart_upload(
        Bucket=bucket,
        Key=s3_key,
        UploadId=upload_id)

    return response
