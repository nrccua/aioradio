"""Pyodbc functions for connecting and send queries."""

# pylint: disable=c-extension-no-member
# pylint: disable=too-many-arguments

import psycopg2


def establish_psycopg2_connection(
        host: str,
        user: str,
        password: str,
        database: str,
        port: int=5432,
        is_audit: bool=False
):
    """Acquire the psycopg2 connection object.

    Args:
        host (str): Host
        user (str): User
        password (str): Password
        database (str): Database
        port (int, optional): Port. Defaults to 5432.
        is_audit (bool, optional): Audit queries. Defaults to False.

    Returns:
        pyscopg2 Connection object
    """

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=database)

    if is_audit:
        conn.autocommit=True

    return conn
