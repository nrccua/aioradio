"""pytest file_ingestion script."""

# pylint: disable=broad-except
# pylint: disable=c-extension-no-member
# pylint: disable=too-many-nested-blocks

import logging
import os
import time
from datetime import timedelta, timezone

import pytest

from aioradio.file_ingestion import (EFIParse, async_db_wrapper, async_wrapper,
                                     delete_ftp_file, establish_ftp_connection,
                                     get_current_datetime_from_timestamp,
                                     list_ftp_objects,
                                     send_emails_via_mandrill,
                                     unzip_file_get_filepaths,
                                     write_file_to_ftp)

LOG = logging.getLogger(__name__)

CREDS = {
    'mandrill': os.getenv('MANDRILL_API_KEY'),
    'ftp_user': os.getenv('FTP_USER'),
    'ftp_pwd': os.getenv('FTP_PW'),
    'ftp_server': os.getenv('FTP_SERVER'),
    'ftp_dns': os.getenv('FTP_DNS'),
}


@pytest.mark.asyncio
async def test_unzip_file_get_filepaths(request, tmpdir_factory):
    """Test unzip_file_get_filepaths."""

    filepath = os.path.join(request.fspath.dirname, 'test_data', 'test_file_ingestion.zip')
    temp_directory = str(tmpdir_factory.mktemp("data"))

    result = await unzip_file_get_filepaths(filepath=filepath, directory=temp_directory)
    assert len(result) == 3

    result = await unzip_file_get_filepaths(filepath=filepath, directory=temp_directory, include_extensions=['txt', 'png'])
    assert len(result) == 3

    result = await unzip_file_get_filepaths(filepath=filepath, directory=temp_directory, exclude_extensions=['png'])
    assert len(result) == 1

    result = await unzip_file_get_filepaths(filepath=filepath, directory=temp_directory, exclude_extensions=['txt', 'png'])
    assert not result


@pytest.mark.asyncio
async def test_get_current_datetime_from_timestamp():
    """Test get_current_datetime_from_timestamp."""

    datetime_utc = await get_current_datetime_from_timestamp()
    assert len(datetime_utc) == 26

    # get datetime for CST during Daylight Savings Time
    datetime = await get_current_datetime_from_timestamp(time_zone=timezone(timedelta(hours=-5)))
    assert len(datetime) == 26
    assert datetime_utc > datetime

    date = await get_current_datetime_from_timestamp(dt_format='%Y-%m-%d')
    assert len(date) == 10


@pytest.mark.asyncio
async def test_send_emails_via_mandrill():
    """Test send_emails_via_mandrill."""

    pytest.skip("Skip sending emails via mandrill.")

    fice = 'XXXXXX'
    global_merge_vars = [
        {'name': 'fice', 'content': fice},
        {'name': 'institution', 'content': 'Test College'}
    ]

    result = await send_emails_via_mandrill(
        mandrill_api_key=CREDS['mandrill'],
        emails=['not.real@nrccua.org'],
        subject='Pytest send_emails_via_mandrill',
        global_merge_vars=global_merge_vars,
        template_name='EFI-Internal'
    )

    assert result[0]['status'] in ['sent', 'queued']


@pytest.mark.asyncio
async def test_write_file_to_ftp(request):
    """Test write_file_to_ftp."""

    pytest.skip('Skip test_write_file_to_ftp')

    filepath = os.path.join(request.fspath.dirname, 'test_data', 'test_file_ingestion.zip')

    conn = await establish_ftp_connection(
        user=CREDS['ftp_user'],
        pwd=CREDS['ftp_pwd'],
        name='pytest',
        server=CREDS['ftp_server'],
        dns=CREDS['ftp_dns']
    )

    file_attributes = await write_file_to_ftp(
        conn=conn,
        service_name='EnrollmentFunnel',
        ftp_path='pytest/is/great/test_file_ingestion.zip',
        local_filepath=filepath
    )

    conn.close()
    assert file_attributes.file_size > 100000
    last_write_time = file_attributes.last_write_time
    time.sleep(1)
    now = time.time()
    assert now > last_write_time and now - last_write_time < 10


@pytest.mark.asyncio
async def test_list_ftp_objects():
    """Test test_list_ftp_objects."""

    pytest.skip('Skip test_list_ftp_objects')

    conn = await establish_ftp_connection(
        user=CREDS['ftp_user'],
        pwd=CREDS['ftp_pwd'],
        name='pytest',
        server=CREDS['ftp_server'],
        dns=CREDS['ftp_dns']
    )

    results = await list_ftp_objects(conn=conn, service_name='EnrollmentFunnel', ftp_path='pytest', exclude_files=True)
    assert len(results) == 1
    assert results[0].filename == 'is'

    results = await list_ftp_objects(conn=conn, service_name='EnrollmentFunnel', ftp_path='pytest/is/great', exclude_directories=True)
    assert len(results) == 1
    assert results[0].filename == 'test_file_ingestion.zip'


@pytest.mark.asyncio
async def test_list_ftp_objects_with_regex():
    """Test test_list_ftp_objects with regex."""

    pytest.skip('Skip test_list_ftp_objects_with_regex')

    conn = await establish_ftp_connection(
        user=CREDS['ftp_user'],
        pwd=CREDS['ftp_pwd'],
        name='pytest',
        server=CREDS['ftp_server'],
        dns=CREDS['ftp_dns']
    )

    results = await list_ftp_objects(
        conn=conn,
        service_name='EnrollmentFunnel',
        ftp_path='pytest/is/great',
        regex_pattern='.(([tT][xX][tT])|([cC][sS][vV])|([tT][sS][vV])|'
                      '([xX][lL][sS][xX])|([xX][lL][sS])|([zZ][iI][pP]))$'
    )
    assert len(results) == 1
    assert results[0].filename == 'test_file_ingestion.zip'


@pytest.mark.asyncio
async def test_delete_ftp_file():
    """Test delete_ftp_file."""

    pytest.skip('Skip test_list_ftp_objects_with_regex')

    conn = await establish_ftp_connection(
        user=CREDS['ftp_user'],
        pwd=CREDS['ftp_pwd'],
        name='pytest',
        server=CREDS['ftp_server'],
        dns=CREDS['ftp_dns']
    )

    result = await delete_ftp_file(conn=conn, service_name='EnrollmentFunnel', ftp_path='pytest/is/great/test_file_ingestion.zip')
    assert result is True


def test_async_wrapper(user):
    """Test async_wrapper with database connections."""

    if user != 'tim.reichard':
        pytest.skip('Skip test_async_wrapper since user is not Tim Reichard')

    @async_wrapper
    async def func():
        return 'Hello World'

    result = func()
    assert result == 'Hello World'


@pytest.mark.asyncio
async def test_async_db_wrapper(user):
    """Test async_db_wrapper with database connections."""

    if user != 'tim.reichard':
        pytest.skip('Skip test_async_db_wrapper since user is not Tim Reichard')

    db_info=[{
        'db': 'pyodbc',
        'name': 'test1',
        'database': 'DataStage',
        'secret': 'efi/sandbox/all',
        'secret_json_key': 'mssql',
        'region': 'us-east-1',
        'rollback': True,
        'trusted_connection': 'no',
        'application_intent': 'ReadOnly',
        'tds_version': '7.4'
    }]

    @async_db_wrapper(db_info=db_info)
    async def func(**kwargs):
        conns = kwargs['conns']
        for name, conn in conns.items():
            print(f"Connection name: {name}\tConnection object: {conn}")

    await func()


def test_check_phone_number():
    """Test check_phone_number in EFIParse."""

    efi = EFIParse('')
    number = efi.check_phone_number('+1 (512) 573-5819', 'CellPhoneNumber', 0)
    assert number == '5125735819'

    number = efi.check_phone_number('215â€"863-79', 'CellPhoneNumber', 0)
    assert number == '21586379'
