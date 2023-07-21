"""Pytest sqs."""

# pylint: disable=c-extension-no-member
# pylint: disable=no-member

from uuid import uuid4

import orjson
import pytest

from aioradio.aws.sqs import (add_regions, delete_messages, get_messages,
                              purge_messages, send_messages)

QUEUE = 'pytest'
REGION = 'us-east-2'

RECEIPT_HANDLES = []
pytestmark = pytest.mark.asyncio


async def test_add_regions():
    """Add us-east-2 region."""

    await add_regions([REGION])


async def test_sqs_creating_queue(sqs_queue_url):
    """Create mock SQS queue."""

    queue_url = await sqs_queue_url(region_name=REGION, queue_name=QUEUE)
    assert queue_url


@pytest.mark.xfail
async def test_sqs_non_existing_queue():
    """Test purging all messages from SQS queue that does not exist."""

    await purge_messages(queue='this-queue-does-not-exist', region=REGION)


async def test_sqs_send_messages():
    """Test sending a batch of messages to an SQS queue."""

    entries = [
        {'Id': str(uuid4()), 'MessageBody': orjson.dumps({'data': 'Hello Austin!'}).decode()},
        {'Id': str(uuid4()), 'MessageBody': orjson.dumps({'data': 'Hello Kansas City!'}).decode()},
        {'Id': str(uuid4()), 'MessageBody': orjson.dumps({'data': 'Hello New York City!'}).decode()},
        {'Id': str(uuid4()), 'MessageBody': orjson.dumps({'data': 'Hello Victoria, Canada!'}).decode()}
    ]
    result = await send_messages(queue=QUEUE, region=REGION, entries=entries)
    assert len(result['Successful']) == 4


async def test_sqs_get_messages():
    """Test receiving a batch of messages from an SQS queue."""

    msgs = await get_messages(queue=QUEUE, region=REGION)
    assert len(msgs) > 0

    for msg in msgs:
        RECEIPT_HANDLES.append(msg['ReceiptHandle'])
        body = orjson.loads(msg['Body'])
        assert 'data' in body
        assert 'Hello' in body['data']


async def test_sqs_delete_messages():
    """Test successful deletion of a batch of SQS queue messages."""

    entries = [{'Id': str(uuid4()), 'ReceiptHandle': i} for i in RECEIPT_HANDLES]
    result = await delete_messages(queue=QUEUE, region=REGION, entries=entries)
    assert result['Successful']


async def test_sqs_purge_messages():
    """Test purging all messages from SQS queue."""

    # Iterate twice to exercise the err on issuing PurgeQueue within 60 seconds of previous call
    for _ in range(2):
        err = await purge_messages(queue=QUEUE, region=REGION)
        if err:
            # accept err: "Only one PurgeQueue operation on pytest is allowed every 60 seconds."
            assert 'PurgeQueue' in err
        else:
            assert not await get_messages(queue=QUEUE, region=REGION, wait_time=1)
