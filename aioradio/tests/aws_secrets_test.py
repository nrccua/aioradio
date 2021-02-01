"""pytest secrets."""

# pylint: disable=unused-argument

import pytest

from aioradio.aws.secrets import add_regions, get_secret

pytestmark = pytest.mark.asyncio


async def test_add_regions():
    """Add us-east-2 region."""

    await add_regions(['us-east-2'])


async def test_secrets_get_secret(create_secret):
    """Test getting secret from Secrets Manager."""

    secret = await get_secret(secret_name='test-secret', region='us-east-2')
    assert secret == 'abc123'


@pytest.mark.xfail
async def test_secrets_get_secret_with_bad_key():
    """Test exception raised when using a bad key retrieving from Secrets
    Manager."""

    await get_secret(secret_name='Pytest-Bad-Key', region='us-east-2')
