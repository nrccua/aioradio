'''Generic async AWS functions for S3.'''

import logging
from typing import Any
from typing import Dict
from typing import List

from aioradio.aws.utils import AwsServiceManager

LOG = logging.getLogger(__name__)
AWS_SERVICE = AwsServiceManager(service='s3')
S3 = AWS_SERVICE.service_dict


@AWS_SERVICE.active
async def create_bucket(bucket: str) -> Dict[str, str]:
    '''Create an s3 bucket.'''

    return await S3['client']['obj'].create_bucket(Bucket=bucket)


@AWS_SERVICE.active
async def upload_file(bucket: str, filepath: str, s3_key: str) -> Dict[str, Any]:
    '''Upload file to s3.'''

    response = {}
    with open(filepath, 'rb') as fileobj:
        response = await S3['client']['obj'].put_object(Bucket=bucket, Key=s3_key, Body=fileobj.read())

    return response


@AWS_SERVICE.active
async def download_file(bucket: str, filepath: str, s3_key: str) -> None:
    '''Download file to s3.'''

    with open(filepath, 'wb') as fileobj:
        data = await get_object(bucket=bucket, s3_key=s3_key)
        fileobj.write(data)


@AWS_SERVICE.active
async def list_s3_objects(bucket: str, s3_prefix: str) -> List[str]:
    '''List objects in s3 path.'''

    arr = []
    paginator = S3['client']['obj'].get_paginator('list_objects')
    async for result in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        arr = [item['Key'] for item in result.get('Contents', [])]

    return arr


@AWS_SERVICE.active
async def get_object(bucket: str, s3_key: str) -> bytes:
    '''Directly download contents of s3 object.'''

    data = None
    s3_object = await S3['client']['obj'].get_object(Bucket=bucket, Key=s3_key)
    async with s3_object["Body"] as stream:
        data = await stream.read()

    return data


@AWS_SERVICE.active
async def delete_s3_object(bucket: str, s3_prefix: str) -> Dict[str, Any]:
    '''Delete object(s) from s3.'''

    response = await S3['client']['obj'].delete_object(Bucket=bucket, Key=s3_prefix)

    return response
