"""Pytest secrets."""

# pylint: disable=unused-argument

import asyncio

import boto3
import pytest
from moto import mock_secretsmanager

from aioradio.aws.secrets import get_secret


@mock_secretsmanager
def test_secrets_get_secret():
    """Test getting secret from Secrets Manager."""

    client = boto3.client('secretsmanager', region_name='us-east-1')
    result = client.create_secret(Name="test-secret-aioradio", SecretString="abc123")
    assert result["ARN"]

    loop = asyncio.get_event_loop()
    secret = loop.run_until_complete(get_secret(secret_name='test-secret-aioradio', region='us-east-1'))
    assert secret == 'abc123'


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_secrets_get_secret_with_bad_key():
    """Test exception raised when using a bad key retrieving from Secrets
    Manager."""

    await get_secret(secret_name='Pytest-Bad-Key', region='us-east-2')
