"""pytest psycopg2 script."""

import json

import pytest

from aioradio.aws.secrets import get_secret
from aioradio.psycopg2 import establish_psycopg2_connection

pytestmark = pytest.mark.asyncio


async def test_establish_psycopg2_connection(github_action, user):
    """Test establish_psycopg2_connection."""

    if github_action:
        pytest.skip('Skip test_establish_psycopg2_connection when running via Github Action')
    elif user != 'tim.reichard':
        pytest.skip('Skip test_establish_psycopg2_connection since user is not Tim Reichard')

    creds = json.loads(await get_secret('datalab/dev/classplanner_db', 'us-east-1'))
    conn = await establish_psycopg2_connection(**creds, database='student')
    assert conn.closed == 0
    conn.close()
