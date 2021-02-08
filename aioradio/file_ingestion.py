"""Generic functions related to working with files or the file system."""

# pylint: disable=broad-except
# pylint: disable=invalid-name
# pylint: disable=too-many-arguments
# pylint: disable=too-many-boolean-expressions

import asyncio
import functools
import json
import os
import re
import time
import zipfile
from asyncio import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from types import coroutine
from typing import Any, Dict, List

import mandrill
from smb.base import SharedFile
from smb.smb_structs import OperationFailure
from smb.SMBConnection import SMBConnection

from aioradio.aws.secrets import get_secret
from aioradio.psycopg2 import establish_psycopg2_connection
from aioradio.pyodbc import establish_pyodbc_connection

DIRECTORY = Path(__file__).parent.absolute()


def async_wrapper(func: coroutine) -> Any:
    """Decorator to run functions using async. Found this handy to use with DAG
    tasks.

    Args:
        func (coroutine): async coroutine
    Returns:
        Any: any
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        """Decorator wrapper.

        Returns:
            Any: any
        """

        return asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper


def async_db_wrapper(db_info: List[Dict[str, Any]]) -> Any:
    """Decorator to run functions using async that handles database connection
    creation and closure.  Pulls database creds from AWS secret manager.

    Args:
        db_info (List[Dict[str, str]], optional): Database info {'name', 'db', 'secret', 'region'}. Defaults to [].

    Returns:
        Any: any
    """

    def parent_wrapper(func: coroutine) -> Any:
        """Decorator parent wrapper.

        Args:
            func (coroutine): async coroutine

        Returns:
            Any: any
        """

        @functools.wraps(func)
        async def child_wrapper(*args, **kwargs) -> Any:
            """Decorator child wrapper. All DB established/closed connections
            and commits or rollbacks take place in the decorator and should
            never happen within the inner function.

            Returns:
                Any: any
            """

            conns = {}
            rollback = {}

            # create connections
            for item in db_info:

                if item['db'] in ['pyodbc', 'psycopg2']:
                    creds = {**json.loads(await get_secret(item['secret'], item['region'])), **{'database': item.get('database', '')}}
                    if item['db'] == 'pyodbc':
                        conns[item['name']] = await establish_pyodbc_connection(**creds, autocommit=False)
                    elif item['db'] == 'psycopg2':
                        conns[item['name']] = await establish_psycopg2_connection(**creds)
                    rollback[item['name']] = item['rollback']
                    print(f"ESTABLISHED CONNECTION for {item['name']}")

            result = None
            error = None
            try:
                # run main function
                result = await func(*args, **kwargs, conns=conns) if conns else await func(*args, **kwargs)
            except Exception as err:
                error = err

            # close connections
            for name, conn in conns.items():

                if rollback[name]:
                    conn.rollback()
                elif error is None:
                    conn.commit()

                conn.close()
                print(f"CLOSED CONNECTION for {name}")

            # if we caught an exception raise it again
            if error is not None:
                raise error

            return result

        return child_wrapper

    return parent_wrapper


def async_wrapper_using_new_loop(func: coroutine) -> Any:
    """Decorator to run functions using async. Found this handy to use with DAG
    tasks.

    Args:
        func (coroutine): async coroutine

    Returns:
        Any: any
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        """Decorator wrapper.

        Returns:
            Any: any
        """

        return asyncio.run(func(*args, **kwargs))

    return wrapper


async def async_process_manager(
        function: coroutine,
        list_of_kwargs: List[Dict[str, Any]],
        chunk_size: int,
        use_threads=True) -> List[Any]:
    """Process manager to run fixed number of functions, usually the same
    function expressed as coroutines in an array.  Use case is sending many
    http requests or iterating files.

    Args:
        function (coroutine): async coroutine
        list_of_kwargs (List[Dict[str, Any]]): list of kwargs to pass into function
        chunk_size (int): number of functions to run concurrently
        use_threads (bool, optional): should threads be used. Defaults to True

    Returns:
        List[Any]: List of function results
    """

    results = []
    if use_threads:
        with ThreadPoolExecutor(max_workers=chunk_size) as exe:
            futures = [exe.submit(function, **items) for items in list_of_kwargs]
            for future in as_completed(futures):
                results.append(future.result())
    else:
        for num in range(0, len(list_of_kwargs), chunk_size):
            tasks = [function(**items) for items in list_of_kwargs[num:num+chunk_size]]
            results.extend(await asyncio.gather(*tasks))

    return results


async def unzip_file(filepath: str, directory: str) -> List[str]:
    """Unzip supplied filepath in the supplied directory.

    Args:
        filepath (str): filepath to unzip
        directory (str): directory to write unzipped files

    Returns:
        List[str]: List of filenames
    """

    zipped = zipfile.ZipFile(filepath)

    # exclude __MACOSX directory that could be added when creating zip on macs
    filenames = [i for i in zipped.namelist() if '__MACOSX' not in i]

    zipped.extractall(directory)
    zipped.close()

    return filenames


async def unzip_file_get_filepaths(
        filepath: str,
        directory: str,
        include_extensions: List[str] = None,
        exclude_extensions: List[str] = None) -> List[str]:
    """Get all the filepaths after unzipping supplied filepath in the supplied
    directory. If the zipfile contains zipfiles, those files will also be
    unzipped.

    Args:
        filepath (str): [description]
        directory (str): [description]
        include_extensions (List[str], optional): list of file types to add to result, if None add all. Defaults to None.
        exclude_extensions (List[str], optional): list of file types to exclude from result. Defaults to None.

    Returns:
        List[str]: [description]
    """

    paths = []
    zipfile_filepaths = [filepath]
    while zipfile_filepaths:

        new_zipfile_filepaths = []
        for path in zipfile_filepaths:

            for filename in await unzip_file(filepath=path, directory=directory):
                filepath = os.path.join(directory, filename)
                suffix = Path(filepath).suffix.lower()[1:]
                if suffix in 'zip':
                    new_zipfile_filepaths.append(filepath)
                elif include_extensions:
                    if suffix in include_extensions:
                        paths.append(filepath)
                elif exclude_extensions:
                    if suffix not in exclude_extensions:
                        paths.append(filepath)
                else:
                    paths.append(filepath)

        zipfile_filepaths = new_zipfile_filepaths

    return paths


async def get_current_datetime_from_timestamp(dt_format: str = '%Y-%m-%d %H_%M_%S.%f', time_zone: tzinfo = timezone.utc) -> str:
    """Get the datetime from the timestamp in the format and timezone desired.

    Args:
        dt_format (str, optional): date format desired. Defaults to '%Y-%m-%d %H_%M_%S.%f'.
        time_zone (tzinfo, optional): timezone desired. Defaults to timezone.utc.

    Returns:
        str: current datetime
    """

    return datetime.fromtimestamp(time.time(), time_zone).strftime(dt_format)


async def send_emails_via_mandrill(
        mandrill_api_key: str,
        emails: List[str],
        subject: str,
        global_merge_vars: List[Dict[str, Any]],
        template_name: str,
        template_content: List[Dict[str, Any]] = None
        ) -> Any:
    """Send emails via Mailchimp mandrill API.

    Args:
        mandrill_api_key (str): mandrill API key
        emails (List[str]): receipt emails
        subject (str): email subject
        global_merge_vars (List[Dict[str, Any]]): List of dicts used to dynamically populated email template with data
        template_name (str): mandrill template name
        template_content (List[Dict[str, Any]], optional): mandrill template content. Defaults to None.

    Returns:
        Any: any
    """

    message = {
        'to': [{'email': email} for email in emails],
        'subject': subject,
        'merge_language': 'handlebars',
        'global_merge_vars': global_merge_vars
    }

    return mandrill.Mandrill(mandrill_api_key).messages.send_template(
        template_name=template_name,
        template_content=template_content,
        message=message
    )


async def establish_ftp_connection(
        user: str,
        pwd: str,
        name: str,
        server: str,
        dns: str,
        port: int = 139,
        use_ntlm_v2: bool = True,
        is_direct_tcp: bool = False) -> SMBConnection:
    """Establish FTP connection.

    Args:
        user (str): ftp username
        pwd (str): ftp password
        name (str): connection name
        server (str): ftp server
        dns (str): DNS
        port (int, optional): port. Defaults to 139.
        use_ntlm_v2 (bool, optional): use NTLMv1 (False) or NTLMv2(True) authentication algorithm. Defaults to True.
        is_direct_tcp (bool, optional): if NetBIOS over TCP (False) or SMB over TCP (True) is used for communication. Defaults to False.

    Returns:
        SMBConnection: SMB connection object
    """

    conn = SMBConnection(
        username=user,
        password=pwd,
        my_name=name,
        remote_name=server,
        use_ntlm_v2=use_ntlm_v2,
        is_direct_tcp=is_direct_tcp
    )
    conn.connect(ip=dns, port=port)
    return conn


async def list_ftp_objects(
        conn: SMBConnection,
        service_name: str,
        ftp_path: str,
        exclude_directories: bool = False,
        exclude_files: bool = False,
        regex_pattern: str = None) -> List[SharedFile]:
    """List all files and directories in an FTP directory.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path
        exclude_directories (bool, optional): directories to exclude. Defaults to False.
        exclude_files (bool, optional): files to exclude. Defaults to False.
        regex_pattern (str, optional): regex pattern to use to filter search. Defaults to None.

    Returns:
        List[SharedFile]: List of files with their attribute info
    """

    results = []
    for item in conn.listPath(service_name, ftp_path):
        is_directory = item.isDirectory
        if item.filename == '.' or item.filename == '..' or \
                (exclude_directories and is_directory) or (exclude_files and not is_directory):
            continue
        if regex_pattern is None or re.search(regex_pattern, item.filename) is not None:
            results.append(item)

    return results


async def delete_ftp_file(conn: SMBConnection, service_name: str, ftp_path: str) -> bool:
    """Remove a file from FTP and verify deletion.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path

    Returns:
        bool: deletion status
    """

    status = False
    conn.deleteFiles(service_name, ftp_path)
    try:
        conn.getAttributes(service_name, ftp_path)
    except OperationFailure:
        status = True

    return status


async def write_file_to_ftp(
        conn: SMBConnection,
        service_name: str,
        ftp_path: str,
        local_filepath) -> SharedFile:
    """Write file to FTP creating missing FTP directories if necessary.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path
        local_filepath ([type]): local filepath

    Returns:
        SharedFile: ftp file attribute info
    """

    # steps to create missing directories
    path = ''
    for directory in os.path.dirname(ftp_path).split(os.sep):
        folders = {i.filename for i in conn.listPath(service_name=service_name, path=path)}
        if directory not in folders:
            conn.createDirectory(service_name=service_name, path=f'{path}/{directory}')
            await sleep(1)
        path = directory if not path else f'{path}/{directory}'

    # write local file to FTP server
    with open(local_filepath, 'rb') as file_obj:
        conn.storeFile(service_name=service_name, path=ftp_path, file_obj=file_obj, timeout=300)
        await sleep(1)

    # return file attributes
    return await get_ftp_file_attributes(conn, service_name, ftp_path)


async def get_ftp_file_attributes(conn: SMBConnection, service_name: str, ftp_path: str) -> SharedFile:
    """GET FTP file attributes.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path

    Returns:
        SharedFile: ftp file attribute info
    """

    return conn.getAttributes(service_name=service_name, path=ftp_path)
