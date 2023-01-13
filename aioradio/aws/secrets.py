"""Generic async AWS functions for Secrets Manager."""

from base64 import b64decode
from typing import Dict

import boto3


async def get_secret(secret_name: str, region: str, aws_creds: Dict[str, str]=None) -> str:
    """Get secret from AWS Secrets Manager.

    Args:
        secret_name (str): secret name
        region (str): AWS region
        aws_creds (Dict[str, str], optional): AWS credentials

    Returns:
        str: secret value
    """

    if aws_creds:
        client = boto3.client(service_name='secretsmanager', region_name=region, **aws_creds)
    else:
        client = boto3.client(service_name='secretsmanager', region_name=region)

    resp = client.get_secret_value(SecretId=secret_name)
    return resp['SecretString'] if 'SecretString' in resp else b64decode(resp['SecretBinary'])
