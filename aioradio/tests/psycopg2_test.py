"""Pytest psycopg2 script."""

import pytest

pytestmark = pytest.mark.asyncio


async def test_establish_psycopg2_connection():
    """Test establish_psycopg2_connection."""

    pytest.skip('Skip test_establish_psycopg2_connection since it contains sensitive info')
