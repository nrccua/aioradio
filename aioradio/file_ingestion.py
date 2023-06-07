"""Generic functions related to working with files or the file system."""

# pylint: disable=broad-except
# pylint: disable=consider-using-enumerate
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name
# pylint: disable=logging-fstring-interpolation
# pylint: disable=too-many-arguments
# pylint: disable=too-many-boolean-expressions
# pylint: disable=too-many-branches
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-lines
# pylint: disable=too-many-locals
# pylint: disable=too-many-nested-blocks
# pylint: disable=too-many-public-methods

import asyncio
import csv
import functools
import json
import logging
import os
import re
import time
import zipfile
from asyncio import sleep
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from types import coroutine
from typing import Any, Dict, List, Union

import cchardet as chardet
import httpx
import mandrill
from openpyxl import load_workbook
from smb.base import SharedFile
from smb.smb_structs import OperationFailure
from smb.SMBConnection import SMBConnection

from aioradio.aws.s3 import download_file, upload_file
from aioradio.aws.secrets import get_secret

DIRECTORY = Path(__file__).parent.absolute()
LOG = logging.getLogger('file_ingestion')


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
                    if 'aws_creds' in item:
                        secret = await get_secret(item['secret'], item['region'], item['aws_creds'])
                    else:
                        secret = await get_secret(item['secret'], item['region'])

                    secret = json.loads(secret)
                    if 'secret_json_key' in item:
                        secret = secret[item['secret_json_key']]

                    creds = {**secret, **{'database': item.get('database', '')}}
                    if item['db'] == 'pyodbc':
                        # Add import here because it requires extra dependencies many systems
                        # don't have out of the box so only import when explicitly being used
                        from aioradio.pyodbc import establish_pyodbc_connection
                        conns[item['name']] = establish_pyodbc_connection(**creds, autocommit=False)
                    elif item['db'] == 'psycopg2':
                        from aioradio.psycopg2 import \
                            establish_psycopg2_connection
                        conns[item['name']] = establish_psycopg2_connection(**creds)
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


async def unzip_file(filepath: str, directory: str) -> List[str]:
    """Unzip supplied filepath in the supplied directory.

    Args:
        filepath (str): filepath to unzip
        directory (str): directory to write unzipped files

    Returns:
        List[str]: List of filenames
    """

    filenames = []
    with zipfile.ZipFile(filepath) as zipped:
        # exclude __MACOSX directory that could be added when creating zip on macs
        filenames = [i for i in zipped.namelist() if '__MACOSX' not in i]
        zipped.extractall(directory)

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
        template_content: List[Dict[str, Any]] = None) -> Any:
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


async def xlsx_to_tsv(
        s3_source_bucket: str,
        s3_source_key: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        fice: str='',
        delimiter: str='\t'
) -> Union[str, None]:
    """Convert xlsx file to csv/tsv file.

    Args:
        s3_source_bucket (str): source xlsx file s3 bucket
        s3_source_key (str): source xlsx file s3 key
        s3_destination_bucket (str): destination xlsx file s3 bucket
        s3_destination_key (str): destination xlsx file s3 key
        fice (str): Institution unique identifier
        delimiter (str, optional): Delimiter. Defaults to '\t'.

    Returns:
        Union[str, None]: Error message during process else None
    """

    try:
        with NamedTemporaryFile(suffix='.xlsx') as tmp:
            await download_file(bucket=s3_source_bucket, filepath=tmp.name, s3_key=s3_source_key)
            records, _ = xlsx_to_records(fice, tmp)

        await tsv_to_s3(records, delimiter, s3_destination_bucket, s3_destination_key)
    except Exception as err:
        raise ValueError('xlsx_to_tsv function failed') from err

    return None


async def zipfile_to_tsv(
        s3_source_bucket: str,
        s3_source_key: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        fice: str='',
        delimiter: str='\t'
) -> Union[str, None]:
    """Convert zipfile to csv/tsv file.

    Args:
        s3_source_bucket (str): source zipfile s3 bucket
        s3_source_key (str): source zipfile s3 key
        s3_destination_bucket (str): destination zipfile s3 bucket
        s3_destination_key (str): destination zipfile s3 key
        fice (str): Institution unique identifier
        delimiter (str, optional): Delimiter. Defaults to '\t'.

    Returns:
        Union[str, None]: Error message during process else None
    """


    extensions = ['xlsx', 'txt', 'csv', 'tsv']
    records = []
    header = None

    with NamedTemporaryFile(suffix='.zip') as tmp:
        await download_file(bucket=s3_source_bucket, filepath=tmp.name, s3_key=s3_source_key)
        with TemporaryDirectory() as tmp_directory:
            for path in await unzip_file_get_filepaths(tmp.name, tmp_directory, include_extensions=extensions):
                ext = os.path.splitext(path)[1].lower()
                if ext == '.xlsx':
                    records_from_path, header = xlsx_to_records(fice, path, header)
                    records.extend(records_from_path)
                else:
                    encoding = detect_encoding(path)
                    if encoding is None:
                        raise IOError(f"Failed to detect proper encoding for {path}")
                    encodings = [encoding] + [i for i in ['UTF-8', 'LATIN-1', 'UTF-16'] if i != encoding]
                    for encoding in encodings:
                        try:
                            detected_delimiter = detect_delimiter(path, encoding)
                            if detected_delimiter:
                                try:
                                    records_from_path, header = tsv_to_records(path, encoding, detected_delimiter, header)
                                    records.extend(records_from_path)
                                    break
                                except Exception as err:
                                    if str(err) == 'Every file must contain the exact same header':
                                        raise ValueError('Every file must contain the exact same header') from err
                                    continue
                        except Exception as err:
                            if str(err) == 'Every file must contain the exact same header':
                                raise ValueError('Every file must contain the exact same header') from err
                            continue
                    else:
                        raise IOError(f"Failed to detect proper encoding for {path}")

    await tsv_to_s3(records, delimiter, s3_destination_bucket, s3_destination_key)

    return None


def tsv_to_records(path: str, encoding: str, delimiter: str, header: str) -> tuple:
    """Translate the file data into 2-dimensional list for efficient
    processing.

    Args:
        path (str): Enrollment file path
        encoding (str): File encoding
        delimiter (str): Delimiter
        header (Union[str, None], optional): Header. Defaults to None.

    Returns:
        tuple: Records as list of lists, header
    """

    records = []
    with open(path, newline='', encoding=encoding) as csvfile:

        dialect = csv.Sniffer().sniff(csvfile.read(8192))
        csvfile.seek(0)

        # remove any null characters in the file
        reader = csv.reader((line.replace('\0', '') for line in csvfile), dialect=dialect, delimiter=delimiter, doublequote=True)
        for row in reader:

            if reader.line_num == 1:
                if header is None:
                    header = row
                elif header != row:
                    raise ValueError("Every file must contain the exact same header")
                else:
                    continue

            records.append(row)

    return records, header


def xlsx_to_records(fice: str, filepath: str, header: Union[str, None]=None) -> tuple:
    """Load excel file to records object as list of lists.

    Args:
        fice (str): Institution unique identifier
        filepath (str): Temporary Filepath
        header (Union[str, None], optional): Header. Defaults to None.

    Raises:
        ValueError: Excel sheets must contain the exact same header

    Returns:
        tuple: Records as list of lists, header
    """

    excel_sheet_filter = get_efi_excel_sheet_filter()

    records = []
    workbook = load_workbook(filepath, read_only=True)
    for sheet in workbook:
        # Make sure excel sheet hasn't been marked to skip for particular fice
        if fice not in excel_sheet_filter or sheet.title not in excel_sheet_filter[fice]:

            sheet.calculate_dimension(force=True)

            for idx, row in enumerate(sheet.values):
                items = [str(value) if value is not None else "" for value in row]

                if idx == 0:
                    if header is None:
                        header = items
                    elif header != items:
                        raise ValueError("Excel sheets must contain the exact same header")
                    else:
                        continue

                records.append(items)
    workbook.close()

    return records, header


async def tsv_to_s3(records: str, delimiter: str, s3_bucket: str, s3_key: str):
    """Write records to tsv/csv file then upload to s3.

    Args:
        records (str): list of lists with values as strings
        delimiter (str): File delimiter
        s3_bucket (str): destination s3 bucket
        s3_key (str): destination s3 key
    """

    with NamedTemporaryFile(mode='w') as tmp:
        writer = csv.writer(tmp, delimiter=delimiter)
        writer.writerows(records)
        tmp.seek(0)
        await upload_file(bucket=s3_bucket, filepath=tmp.name, s3_key=s3_key)


def detect_encoding(path: str) -> str:
    """Detect enrollment file encoding.

    Args:
        path (str): Enrollment file path

    Returns:
        str: Enrollment file encoding
    """

    encoding = None
    with open(path, "rb") as handle:
        encoding_dict = chardet.detect(handle.read())
        if 'encoding' in encoding_dict:
            encoding = encoding_dict['encoding'].upper()

    return encoding


def detect_delimiter(path: str, encoding: str) -> str:
    """Detect enrollment file delimiter.

    Args:
        path (str): Enrollment file path
        encoding (str): File encoding

    Returns:
        str: Delimiter
    """

    delimiter = ''
    with open(path, newline='', encoding=encoding) as csvfile:
        data = csvfile.read(8192)
        count = -1
        for item in [',', '\t', '|']:
            char_count = data.count(item)
            if char_count > count:
                delimiter = item
                count = char_count

    return delimiter


def get_efi_excel_sheet_filter() -> dict[str, set]:
    """Get the Excel sheet filter from the EFI api.

    Returns:
        dict[str, set]: Excel sheet filter with fice as key and sheet names as value
    """

    excel_sheet_filter = {}
    try:
        with httpx.Client() as client:
            resp = client.get(url="http://efi.nrccua-app.org/filter/excel-sheet", timeout=30.0)
            for fice, item in resp.json()["excel_sheet_filter"].items():
                excel_sheet_filter[fice] = set()
                for name, value in item.items():
                    if value:
                        excel_sheet_filter[fice].add(name)
    except Exception:
        pass

    return excel_sheet_filter
