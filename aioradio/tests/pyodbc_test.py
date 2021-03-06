"""pytest pyodbc script."""

import pytest

from aioradio.pyodbc import establish_pyodbc_connection

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail
async def test_bad_unixodbc_driver():
    """Test using a bad unixodbc_driver that the proper exception is raised."""

    await establish_pyodbc_connection(host='unknown', user='psuedo', pwd='no-way-jose', driver='/usr/lib/bogus.so')


async def test_pyodbc_query_fetchone_and_fetchall():
    """Test pyodbc_query_fetchone.

    Make sure you have unixodbc and freetds installed;
    see here: http://www.christophers.tips/pages/pyodbc_mac.html.
    """

    pytest.skip('Skip test_pyodbc_query_fetchone_and_fetchall since it contains sensitive info')
