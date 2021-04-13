"""pytest Long Running Jobs."""

import time
from asyncio import create_task, sleep
from random import randint
from typing import Any, Dict

import pytest

from aioradio.long_running_jobs import LongRunningJobs

pytestmark = pytest.mark.asyncio


async def job1(params: Dict[str, Any]) -> int:
    """Long Running Job1."""

    async def delay(delay, result):
        await sleep(delay)
        return result

    return await delay(**params)

async def job2(params: Dict[str, Any]) -> int:
    """Long Running Job2."""

    def delay(delay, result):
        time.sleep(delay)
        return result

    return delay(**params)


async def test_lrj_worker(github_action):
    """Test test_lrj_worker."""

    if github_action:
        pytest.skip('Skip test_lrj_worker when running via Github Action')

    lrj1 = LongRunningJobs(
        redis_host='prod-race2.gbngr1.ng.0001.use1.cache.amazonaws.com',
        expire_cached_result=5,
        expire_job_data=5,
        sqs_queue='NARWHAL_QUEUE_SANDBOX.fifo',
        sqs_region='us-east-1',
        jobs={'job1': (job1, 10)}
    )

    lrj2 = LongRunningJobs(
        redis_host='prod-race2.gbngr1.ng.0001.use1.cache.amazonaws.com',
        expire_cached_result=5,
        expire_job_data=5,
        queue_service='redis',
        jobs={'job2': (job2, 10)}
    )

    create_task(lrj1.start_worker())
    create_task(lrj2.start_worker())

    params = {'delay': 1, 'result': randint(0, 100)}
    result1 = await lrj1.send_message(job_name='job1', params=params)
    assert 'uuid' in result1 and 'error' not in result1

    cache_key = await lrj2.build_cache_key(params=params)
    result2 = await lrj2.send_message(job_name='job2', params=params, cache_key=cache_key)
    assert 'uuid' in result2 and 'error' not in result2

    await sleep(4)

    result = await lrj1.check_job_status(result1['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    result = await lrj2.check_job_status(result2['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    result3 = await lrj1.send_message(job_name='job1', params=params, cache_key=cache_key)
    await sleep(1)
    assert 'uuid' in result3 and 'error' not in result3
    result = await lrj1.check_job_status(result3['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    await sleep(5)
    result = await lrj2.check_job_status(result2['uuid'])
    assert not result['job_done'] and 'error' in result


async def test_lrj_worker_running_two_jobs(github_action):
    """Test test_lrj_worker_running_two_jobs."""

    if github_action:
        pytest.skip('Skip test_lrj_worker_running_two_jobs when running via Github Action')

    lrj3 = LongRunningJobs(
        redis_host='prod-race2.gbngr1.ng.0001.use1.cache.amazonaws.com',
        expire_cached_result=5,
        expire_job_data=5,
        queue_service='redis',
        jobs={'job1': (job1, 10), 'job2': (job2, 10)}
    )

    create_task(lrj3.start_worker())

    params = {'delay': 0.1, 'result': randint(0, 100)}
    result1 = await lrj3.send_message(job_name='job1', params=params)
    result2 = await lrj3.send_message(job_name='job2', params=params)

    await sleep(2)

    result1 = await lrj3.check_job_status(result1['uuid'])
    result2 = await lrj3.check_job_status(result2['uuid'])
    assert result1['job_done'] and result1['results'] == params['result']
    assert result2['job_done'] and result2['results'] == params['result']
