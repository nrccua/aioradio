'''Generic functions related to working with files or the file system.'''

# pylint: disable=invalid-name
# pylint: disable=too-many-arguments
# pylint: disable=too-many-boolean-expressions

import asyncio
import os
import re
import time
import zipfile
from asyncio import sleep
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from datetime import tzinfo
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import mandrill
from smb.base import SharedFile
from smb.SMBConnection import SMBConnection
from smb.smb_structs import OperationFailure

DIRECTORY = Path(__file__).parent.absolute()


def async_wrapper(func):
    '''Decorator to run functions using async. Found this handy to use with DAG tasks.'''

    def wrapper(*args, **kwargs):
        '''Decorator wrapper.'''

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(*args, **kwargs))

    return wrapper


def async_wrapper_using_new_loop(func):
    '''Decorator to run functions using async. Found this handy to use with DAG tasks.'''

    def wrapper(*args, **kwargs):
        '''Decorator wrapper.'''

        return asyncio.run(func(*args, **kwargs))

    return wrapper


async def async_process_manager(
        function: asyncio.coroutine,
        list_of_kwargs: List[Dict[str, Any]],
        chunk_size: int,
        use_threads=True) -> List[Any]:
    '''Process manager to run fixed number of functions, usually the same function expressed as
    coroutines in an array.  Use case is sending many http requests or iterating files.'''

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
    '''Unzip supplied filepath in the supplied directory returning list of filenames.'''

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
    '''Get all the filepaths after unzipping supplied filepath in the supplied directory.
    If the zipfile contains zipfiles, those files will also be unzipped. If include_extensions
    is supplied then add those file types to the result. If exclude_extensions is supplied
    then skip adding those filepaths to the result.'''

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


async def get_current_datetime_from_timestamp(
        dt_format: str = '%Y-%m-%d %H_%M_%S.%f',
        time_zone: tzinfo = timezone.utc) -> str:
    '''Get the datetime from the timestamp in the format and timezone desired.'''

    return datetime.fromtimestamp(time.time(), time_zone).strftime(dt_format)


async def send_emails_via_mandrill(
        mandrill_api_key: str,
        emails: List[str],
        subject: str,
        global_merge_vars: List[Dict[str, Any]],
        template_name: str,
        template_content: List[Dict[str, Any]] = None
        ) -> Any:
    '''Send emails via Mailchimp mandrill API.'''

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
    '''Establish FTP connection'''

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
    '''List all files and directories in an FTP directory.'''

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
    '''Remove a file from FTP and verify deletion.'''

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
    '''Write file to FTP creating missing FTP directories if necessary.'''

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
    return conn.getAttributes(service_name=service_name, path=ftp_path)
