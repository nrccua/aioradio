"""Pyodbc functions for connecting and send queries."""

# pylint: disable=c-extension-no-member
# pylint: disable=too-many-arguments
# pylint: disable=unsubscriptable-object

import os
import platform
from typing import Any, List, Union

import pyodbc

OPERATING_SYSTEM = platform.system()

# driver location varies based on OS.  add to this list if necessary...
UNIXODBC_DRIVER_PATHS = [
    '/usr/lib/libtdsodbc.so',
    '/usr/local/lib/libtdsodbc.so',
    '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so'
]


async def get_unixodbc_driver_path(paths: List[str]) -> Union[str, None]:
    """Check the file system for the unixodbc driver.

    Args:
        paths (List[str]): List of filepaths

    Returns:
        Union[str, None]: driver path
    """

    driver_path = None
    for path in paths:
        if os.path.exists(path):
            driver_path = path
            break

    return driver_path


async def establish_pyodbc_connection(
        host: str,
        user: str,
        pwd: str,
        port: int=None,
        database: str='',
        trusted_connection: str='',
        multi_subnet_failover: str='',
        driver: str='',
        autocommit: bool=False
) -> pyodbc.Connection:
    """Acquire and return pyodbc.Connection object else raise
    FileNotFoundError.

    Args:
        host (str): hostname
        user (str): username
        pwd (str): password
        port (int, optional): port. Defaults to None.
        database (str, optional): database. Defaults to ''.
        trusted_connection (str, optional): Trusted_Connection. Defaults to ''.
        multi_subnet_failover (str, optional): MultiSubnetFailover. Defaults to ''.
        driver (str, optional): unixodbc driver. Defaults to ''.
        autocommit (bool, optional): autocommit. Defaults to False.

    Raises:
        FileNotFoundError: unable to locate unixodbc driver

    Returns:
        pyodbc.Connection: database connection object
    """

    verified_driver = None
    if OPERATING_SYSTEM == 'Windows':
        verified_driver = driver
    else:
        if driver and not driver.startswith('{'):
            verified_driver = await get_unixodbc_driver_path([driver])
        else:
            verified_driver = await get_unixodbc_driver_path(UNIXODBC_DRIVER_PATHS)
        if verified_driver is None:
            raise FileNotFoundError('Unable to locate unixodbc driver file: libtdsodbc.so')

    conn_string = f'DRIVER={verified_driver};SERVER={host};UID={user};PWD={pwd};TDS_Version=8.0'
    if port is not None:
        conn_string += f';PORT={port}'
    if database:
        conn_string += f';DATABASE={database}'
    if trusted_connection:
        conn_string += f';Trusted_Connection={trusted_connection}'
    if multi_subnet_failover:
        conn_string += f';MultiSubnetFailover={multi_subnet_failover}'

    return pyodbc.connect(conn_string, autocommit=autocommit)


async def pyodbc_query_fetchone(conn: pyodbc.Connection, query: str) -> Union[List[Any], None]:
    """Execute pyodbc query and fetchone, see
    https://github.com/mkleehammer/pyodbc/wiki/Cursor.

    Args:
        conn (pyodbc.Connection): database connection object
        query (str): sql query

    Returns:
        Union[List[Any], None]: list of one result
    """

    cursor = conn.cursor()
    result = cursor.execute(query).fetchone()

    return result


async def pyodbc_query_fetchall(conn: pyodbc.Connection, query: str) -> Union[List[Any], None]:
    """Execute pyodbc query and fetchone, see
    https://github.com/mkleehammer/pyodbc/wiki/Cursor.

    Args:
        conn (pyodbc.Connection): database connection object
        query (str): sql query

    Returns:
        Union[List[Any], None]: list of one to many results
    """

    cursor = conn.cursor()
    result = cursor.execute(query).fetchall()

    return result
