"""pytest pyodbc script."""

import json

import pytest

from aioradio.aws.secrets import get_secret
from aioradio.pyodbc import establish_pyodbc_connection

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail
async def test_bad_unixodbc_driver(github_action):
    """Test using a bad unixodbc_driver that the proper exception is raised."""

    if github_action:
        pytest.skip('Skip test_bad_unixodbc_driver when running via Github Action')

    creds = json.loads(await get_secret('efi/sandbox/mssql', 'us-east-1'))
    await establish_pyodbc_connection(**creds, driver='/usr/lib/bogus.so')


async def test_pyodbc_query_fetchone_and_fetchall():
    """Test pyodbc_query_fetchone.

    Make sure you have unixodbc and freetds installed;
    see here: http://www.christophers.tips/pages/pyodbc_mac.html.
    """

    pytest.skip('Skip test_pyodbc_query_fetchone_and_fetchall since it contains sensitive info')
