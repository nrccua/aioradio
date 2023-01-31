"""pytest Long Running Jobs."""

from asyncio import create_task, sleep
from random import randint
from typing import Any, Dict

import pytest

from aioradio.aws.sqs import add_regions
from aioradio.long_running_jobs import LongRunningJobs

QUEUE = 'pytest.fifo'
REGION = 'us-east-2'

pytestmark = pytest.mark.asyncio

async def test_add_regions():
    """Add us-east-2 region."""

    await add_regions([REGION])


async def test_sqs_creating_queue(sqs_queue_url):
    """Create mock SQS queue."""

    queue_url = await sqs_queue_url(region_name=REGION, queue_name=QUEUE)
    assert queue_url


async def job1(params: Dict[str, Any]) -> int:
    """Long Running Job1."""

    async def delay(delay, result):
        await sleep(delay)
        return result

    return await delay(**params)


async def test_lrj_worker():
    """Test test_lrj_worker."""

    lrj1 = LongRunningJobs(
        fakeredis=True,
        expire_cached_result=5,
        expire_job_data=5,
        sqs_queue=QUEUE,
        sqs_region=REGION,
        jobs={'job1': (job1, 15)}
    )

    create_task(lrj1.start_worker())

    await sleep(1)

    params = {'delay': 1, 'result': randint(0, 100)}
    result1 = await lrj1.send_message(job_name='job1', params=params)
    assert 'uuid' in result1 and 'error' not in result1

    await sleep(3)

    result = await lrj1.check_job_status(result1['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    await sleep(5)
    result = await lrj1.check_job_status(result1['uuid'])
    assert not result['job_done'] and 'error' in result

    await lrj1.stop_worker()
    await sleep(2)
