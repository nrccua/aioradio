"""Generic async AWS functions for Secrets Manager."""

import base64
from typing import List

from aioradio.aws.utils import AwsServiceManager

AWS_SERVICE = AwsServiceManager(service='secretsmanager', regions=['us-east-1'])
SECRETS = AWS_SERVICE.service_dict


async def add_regions(regions: List[str]):
    """Add regions to Secret Manager AWS service.

    Args:
        regions (List[str]): List of AWS regions
    """

    AWS_SERVICE.add_regions(regions)


@AWS_SERVICE.active
async def get_secret(secret_name: str, region: str) -> str:
    """Get secret from AWS Secrets Manager.

    Args:
        secret_name (str): secret name
        region (str): AWS region

    Returns:
        str: secret value
    """

    secret = ''
    response = await SECRETS[region]['client']['obj'].get_secret_value(SecretId=secret_name)
    if 'SecretString' in response:
        secret = response['SecretString']
    else:
        secret = base64.b64decode(response['SecretBinary'])

    return secret
