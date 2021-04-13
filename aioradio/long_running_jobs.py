"""Long Running Jobs worker script."""

# pylint: disable=broad-except
# pylint: disable=c-extension-no-member
# pylint: disable=logging-fstring-interpolation
# pylint: disable=too-many-instance-attributes

import asyncio
import traceback
from dataclasses import dataclass
from time import time
from typing import Any, Dict
from uuid import uuid4

import orjson

from aioradio.aws import sqs
from aioradio.redis import Redis


@dataclass
class LongRunningJobs:
    """Worker that continually pulls from queue (either SQS or Redis list
    implemented like queue), running a job using request parameters conveyed in
    the message.

    Also has functions to send messages to the queue, and to check if
    job is complete.
    """

    name: str   # Name of the long running job used to identify between multiple jobs running within app.
    redis_host: str
    cache_expiration: int = 3600
    worker_active: bool = False

    # choose between sqs or redis
    queue_service: str = 'sqs'
    # if using sqs than define the queue name and aws region
    sqs_queue: str = None
    sqs_region: str = None

    # job_timeout value should be a factor of 2x or 3x above the max time a job finishes and corresponds
    # to the visibility_timeout when queue_service = 'sqs' and message re-entry into
    # <self.name>-not-started queue when queue_service = 'redis'. Setting job_timeout is optional when
    # instantiating the class as it can also be defined when issuing the start_worker method.
    job_timeout: float = 30

    def __post_init__(self):

        self.queue_service = self.queue_service.lower()
        if self.queue_service not in ['sqs', 'redis']:
            raise ValueError("queue_service must be either 'sqs' or 'redis'.")

        self.name_to_job = {self.name: None}

        self.cache = Redis(
            config={
                'redis_primary_endpoint': self.redis_host,
                'encoding': 'utf-8'
            },
            expire=int(self.cache_expiration)
        )

    async def check_job_status(self, uuid: str) -> Dict[str, Any]:
        """Check if the job is done and if so add results to the returned dict.

        Args:
            uuid (str): Unique identifier

        Returns:
            Dict[str Any]: Check job status results
        """

        result = {"uuid": uuid, "job_done": False}
        data = await self.cache.hgetall(key=uuid)
        if data:
            result["job_done"] = data['job_done']
            if result["job_done"]:
                result["results"] = data['results']
        else:
            result["error"] = f"Cannot find {uuid} in Redis Cache"

        return result

    async def send_message(self, params: Dict[str, Any], cache_key: str=None) -> Dict[str, str]:
        """Send message to queue.

        Args:
            params (Dict[str, Any]): Request parameters needed for job
            cache_key (str, optional): Results cache key. Defaults to None.

        Returns:
            Dict[str, str]: Contains sent message uuid or an error
        """

        identifier = str(uuid4())
        items = {
            "uuid": identifier,
            "job_done": False,
            "params": params,
            "cache_key": cache_key,
            "results": None
        }

        result = {}
        try:
            msg = orjson.dumps(items).decode()
            if self.queue_service == 'sqs':
                entries = [{'Id': str(uuid4()), 'MessageBody': msg, 'MessageGroupId': self.name}]
                await sqs.send_messages(queue=self.sqs_queue, region=self.sqs_region, entries=entries)
            else:
                self.cache.pool.rpush(f'{self.name}-not-started', msg)

            await self.cache.hmset(key=identifier, items=items)
            result['uuid'] = identifier
        except Exception as err:
            result['error'] = str(err)

        return result

    async def start_worker(self, job: Any, job_timeout: float=30):
        """Continually run the worker.

        Args:
            job (Any): Long running job as an async function
            job_timeout (float): Job should finish before given amount of time in seconds
        """

        if self.name_to_job[self.name] is not None and self.name_to_job[self.name] != job:
            raise TypeError('LongRunningJob class can only be assigned to process one job!')

        self.job_timeout = job_timeout
        self.worker_active = True
        while True:

            while self.worker_active:
                try:
                    # run job the majority of the time pulling up to 10 messages to process
                    for _ in range(10):
                        if self.queue_service == 'sqs':
                            await self.__sqs_pull_messages_and_run_jobs__(job)
                        else:
                            await self.__redis_pull_messages_and_run_jobs__(job)

                    # verify processing only a fraction of the time
                    if self.queue_service == 'redis':
                        await self.__verify_processing__()
                except asyncio.CancelledError:
                    print(traceback.format_exc())
                    break
                except Exception:
                    print(traceback.format_exc())
                    await asyncio.sleep(30)

            await asyncio.sleep(1)

    async def stop_worker(self):
        """Stop worker."""

        self.worker_active = False

    async def __sqs_pull_messages_and_run_jobs__(self, job: Any):
        """Pull messages one at a time and run job.

        Args:
            job (Any): Long running job as an async function

        Raises:
            IOError: Redis access failed
        """

        msg = await sqs.get_messages(
            queue=self.sqs_queue,
            region=self.sqs_region,
            wait_time=1,
            visibility_timeout=self.job_timeout
        )
        if not msg:
            await asyncio.sleep(0.1)
        else:
            body = orjson.loads(msg[0]['Body'])
            key = body['cache_key']

            data = None if key is None else await self.cache.get(key)
            if data is None:
                # No results found in cache so run the job
                data = await job(body['params'])

                # Set the cached parameter based key with results
                if key is not None and not await self.cache.set(key, data):
                    raise IOError(f"Setting cache string failed for cache_key: {key}")

            # Update the hashed UUID with processing results
            await self.cache.hmset(key=body['uuid'], items={**body, **{'results': data, 'job_done': True}})
            entries = [{'Id': str(uuid4()), 'ReceiptHandle': msg[0]['ReceiptHandle']}]
            await sqs.delete_messages(queue=self.sqs_queue, region=self.sqs_region, entries=entries)

    async def __redis_pull_messages_and_run_jobs__(self, job: Any):
        """Pull messages one at a time and run job.

        Args:
            job (Any): Long running job as an async function

        Raises:
            IOError: Redis access failed
        """

        # in the future convert lpop to lmove and also look into integrating with async aioredis
        msg = self.cache.pool.lpop(f'{self.name}-not-started')
        if not msg:
            await asyncio.sleep(0.1)
        else:
            body = orjson.loads(msg)
            key = body['cache_key']

            # Add start time and push msg to <self.name>-in-process
            body['start_time'] = time()
            self.cache.pool.rpush(f'{self.name}-in-process', orjson.dumps(body).decode())

            data = None if key is None else await self.cache.get(key)
            if data is None:
                # No results found in cache so run the job
                data = await job(body['params'])

                # Set the cached parameter based key with results
                if key is not None and not await self.cache.set(key, data):
                    raise IOError(f"Setting cache string failed for cache_key: {key}")

            # Update the hashed UUID with processing results
            await self.cache.hmset(key=body['uuid'], items={**body, **{'results': data, 'job_done': True}})

    async def __verify_processing__(self):
        """Verify processing completed fixing issues related to app crashing or
        scaling down servers when using queue_service = 'redis'.
        """

        for _ in range(10):
            msg = self.cache.pool.lpop(f'{self.name}-in-process')
            if not msg:
                break

            body = orjson.loads(msg)
            job_done = await self.cache.hget(key=body['uuid'], field='job_done')
            if job_done is None:
                pass    # if the cache is expired then we can typically ignore doing anything
            elif not job_done:
                if (time() - body['start_time']) > self.job_timeout:
                    print(f'Failed processing uuid: {body["uuid"]} in {self.job_timeout} seconds. \
                            Pushing msg back to {self.name}-not-started.')
                    self.cache.pool.rpush(f'{self.name}-not-started', msg)
                else:
                    self.cache.pool.rpush(f'{self.name}-in-process', msg)

    @staticmethod
    async def build_cache_key(params: Dict[str, Any], separator='|') -> str:
        """Build a cache key from a dictionary object.  Concatenate and
        normalize key-values from an unnested dict, taking care of sorting the
        keys and each of their values (if a list).

        Args:
            params (Dict[str, Any]): dict object to use to build cache key
            separator (str, optional): character to use as a separator in the cache key. Defaults to '|'.
        Returns:
            str: dict object converted to string
        """

        keys = sorted(params.keys())
        concat_key = separator.join([
            f'{k}={orjson.dumps(params[k], option=orjson.OPT_SORT_KEYS).decode()}'.replace('"', '')
            for k in keys if params[k] != [] and params[k] is not None
        ])

        return concat_key
