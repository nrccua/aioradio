"""pytest Long Running Jobs."""

import time
from asyncio import create_task, sleep
from random import randint
from typing import Any, Dict

import pytest

pytestmark = pytest.mark.asyncio


async def test_lrj_worker(github_action, lrj1, lrj2):
    """Test test_lrj_worker."""

    if github_action:
        pytest.skip('Skip test_lrj_worker when running via Github Action')

    async def job1(params: Dict[str, Any]) -> int:

        async def delay(delay, result):
            await sleep(delay)
            return result

        return await delay(**params)

    async def job2(params: Dict[str, Any]) -> int:

        def delay(delay, result):
            time.sleep(delay)
            return result

        return delay(**params)


    worker1 = lrj1.start_worker(job=job1, job_timeout=3)
    create_task(worker1)
    worker2 = lrj2.start_worker(job=job2, job_timeout=3)
    create_task(worker2)

    params = {'delay': 1, 'result': randint(0, 100)}
    result1 = await lrj1.send_message(params=params)
    assert 'uuid' in result1 and 'error' not in result1

    cache_key = await lrj2.build_cache_key(params=params)
    result2 = await lrj2.send_message(params=params, cache_key=cache_key)
    assert 'uuid' in result2 and 'error' not in result2

    await sleep(1.5)

    result = await lrj1.check_job_status(result1['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    result = await lrj2.check_job_status(result2['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    result3 = await lrj1.send_message(params=params, cache_key=cache_key)
    await sleep(0.333)
    assert 'uuid' in result3 and 'error' not in result3
    result = await lrj1.check_job_status(result3['uuid'])
    assert result['job_done'] and result['results'] == params['result']

    await sleep(5)
    result = await lrj2.check_job_status(result1['uuid'])
    assert not result['job_done'] and 'error' in result

    await lrj1.stop_worker()
    await lrj2.stop_worker()
