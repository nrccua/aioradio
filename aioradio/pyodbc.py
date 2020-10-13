'''Pyodbc functions for connecting and send queries.'''

# pylint: disable=c-extension-no-member

import os
from typing import Any
from typing import List
from typing import Union

import pyodbc

# driver location varies based on OS.  add to this list if necessary...
UNIXODBC_DRIVER_PATHS = [
    '/usr/lib/libtdsodbc.so',
    '/usr/local/lib/libtdsodbc.so',
    '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so'
]


async def get_unixodbc_driver_path(paths) -> Union[str, None]:
    '''Check the file system for the unixodbc driver.'''

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
        driver: str = None) -> pyodbc.Connection:
    '''Acquire and return pyodbc.Connection object else raise FileNotFoundError.'''

    if driver is None:
        verified_driver = await get_unixodbc_driver_path(UNIXODBC_DRIVER_PATHS)
    else:
        verified_driver = await get_unixodbc_driver_path([driver])

    if verified_driver is None:
        raise FileNotFoundError('Unable to locate unixodbc driver file: libtdsodbc.so')

    return pyodbc.connect(
        f'DRIVER={verified_driver};SERVER={host};PORT=1433;UID={user};PWD={pwd};TDS_Version=8.0')


async def pyodbc_query_fetchone(conn: pyodbc.Connection, query: str) -> List[Any]:
    '''Execute pyodbc query and fetchone, see https://github.com/mkleehammer/pyodbc/wiki/Cursor'''

    cursor = conn.cursor()
    result = cursor.execute(query).fetchone()

    return result


async def pyodbc_query_fetchall(conn: pyodbc.Connection, query: str) -> List[Any]:
    '''Execute pyodbc query and fetchone, see https://github.com/mkleehammer/pyodbc/wiki/Cursor'''

    cursor = conn.cursor()
    result = cursor.execute(query).fetchall()

    return result
