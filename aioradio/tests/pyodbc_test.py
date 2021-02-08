"""pytest pyodbc script."""

import os

import pytest

from aioradio.pyodbc import (establish_pyodbc_connection,
                             pyodbc_query_fetchall, pyodbc_query_fetchone)

pytestmark = pytest.mark.asyncio

CREDS = {'mssql_host': os.getenv('MSSQL_HOST'), 'mssql_user': os.getenv('MSSQL_USER'), 'mssql_pwd': os.getenv('MSSQL_PW')}


@pytest.mark.xfail
async def test_bad_unixodbc_driver(github_action):
    """Test using a bad unixodbc_driver that the proper exception is raised."""

    if github_action:
        pytest.skip('Skip test_bad_unixodbc_driver when running via Github Action')

    driver = '/usr/lib/bogus.so'
    await establish_pyodbc_connection(host=CREDS['mssql_host'], user=CREDS['mssql_user'], pwd=CREDS['mssql_pwd'], driver=driver)


async def test_pyodbc_query_fetchone_and_fetchall(github_action):
    """Test pyodbc_query_fetchone.

    Make sure you have unixodbc and freetds installed;
    see here: http://www.christophers.tips/pages/pyodbc_mac.html.
    """

    if github_action:
        pytest.skip('Skip test_pyodbc_query_fetchone_and_fetchall when running via Github Action')

    conn = await establish_pyodbc_connection(host=CREDS['mssql_host'], user=CREDS['mssql_user'], pwd=CREDS['mssql_pwd'])
    query = "SELECT EFIemails FROM DataStage.dbo.EESFileuploadAssignments WHERE FICE = '003800' AND FileCategory = 'EnrollmentLens'"

    row = await pyodbc_query_fetchone(conn=conn, query=query)
    emails = [i.strip() for i in row[0].lower().split(';')] if row is not None else []

    expected_emails = [
        'amy.huey@nrccua.org',
        'bridgetk@nrccua.org',
        'kris@nrccua.org',
        'ryan.thompson@nrccua.org',
        'tim.reichard@nrccua.org'
    ]
    assert sorted(emails) == expected_emails

    query = "SELECT * FROM DataStage.dbo.EESFileuploadAssignments WHERE FICE = '003800'"
    rows = await pyodbc_query_fetchall(conn=conn, query=query)
    assert len(rows) == 3

    query = "SELECT Consultants FROM DataStage.dbo.EESFileuploadAssignments WHERE FICE = '003800' AND FileCategory = 'EnrollmentLens'"
    row = await pyodbc_query_fetchone(conn=conn, query=query)
    emails = [i.strip() for i in row[0].lower().split(';')] if row is not None else []

    expected_emails = ['bridgetk@nrccua.org']
    assert emails == expected_emails

    conn.close()
