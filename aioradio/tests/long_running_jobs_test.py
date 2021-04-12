"""pytest Long Running Jobs."""

from asyncio import create_task, sleep
from random import randint

import pytest

pytestmark = pytest.mark.asyncio


async def test_lrj_worker(github_action, lrj):
    """Test test_lrj_worker."""

    if github_action:
        pytest.skip('Skip test_lrj_worker when running via Github Action')

    async def pytest_async(delay, result) -> int:
        await sleep(delay)
        return result

    worker = lrj.start_worker(job_name='pytest_async', job=pytest_async)
    create_task(worker)

    result = await lrj.send_message(job_name='pytest_async', params={'delay': 1, 'result': randint(0, 100)})
    assert 'uuid' in result and 'error' not in result
