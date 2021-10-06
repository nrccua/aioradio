"""Long Running Jobs worker script."""

# pylint: disable=broad-except
# pylint: disable=c-extension-no-member
# pylint: disable=logging-fstring-interpolation
# pylint: disable=no-member
# pylint: disable=too-many-instance-attributes

import asyncio
import socket
import traceback
from asyncio import create_task
from dataclasses import dataclass, field
from time import time
from typing import Any, Dict, Tuple
from uuid import uuid4

import httpx
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

    redis_host: str = 'localhost'

    # Expiration of cached result. If a job has the same cache_key of a previously run job in redis
    # then we can skip running the job using the previously obtained result.
    expire_cached_result: int = 86400

    # Expiration of cached job data stored as a hashed field.  Example of an ideal TTL is one hour if
    # we expect the job to run in a few seconds/minutes.  If the long running job takes hours
    # then update this to day(s) in seconds.
    expire_job_data: int = 3600

    # Flexibility to define one to many jobs, ex: {"job1_name": (async_func1, 30), "job2_name": (async_func2, 60)}
    # First item of tuple must be an async function that runs your long running job, and the second item the
    # job timeout, which should be a value ~3x or more above the max time a job finishes corresponding to the
    # visibility_timeout if queue_service = 'sqs' or message re-entry into not-started queue if queue_service = 'redis'.
    jobs: Dict[str, Tuple[Any, float]] = field(default_factory=dict)

    # choose between sqs or redis
    queue_service: str = 'sqs'
    # if using sqs than define the queue name and aws region
    sqs_queue: str = None
    sqs_region: str = None

    # If running test cases use fakeredis
    fakeredis: bool = False

    # Trigger the worker to stop running continually
    stop: bool = False

    httpx_client: httpx.AsyncClient = httpx.AsyncClient()

    def __post_init__(self):

        self.queue_service = self.queue_service.lower()
        if self.queue_service not in ['sqs', 'redis']:
            raise ValueError("queue_service must be either 'sqs' or 'redis'.")

        if self.fakeredis:
            self.cache = Redis(fake=True)
        else:
            self.cache = Redis(config={'redis_primary_endpoint': self.redis_host, 'encoding': 'utf-8'})

        self.job_names = set(self.jobs.keys())
        for job_name, job_info in self.jobs.items():
            if len(job_info) != 2:
                raise ValueError('Job info should be provided as a tuple, ex. (job_function, job_timeout)')
            if not isinstance(job_name, str):
                raise ValueError('Job name must be a string value')
            if not isinstance(job_info[1], (int, float)):
                raise ValueError('Job timeout needs to be an integer or float')
            if job_info[1] < 10:
                raise ValueError('Job timeout needs to be at least 10 seconds')
            if job_info[1] > (3600 * 5):
                raise ValueError('Job timeout needs to be no more than 5 hours')

        self.longest_job_timeout = max([i[1] for i in self.jobs.values()])
        self.host_uuid = f'{socket.gethostname()}|{uuid4()}'

    async def stop_worker(self):
        """Stop the worker from running continually."""

        self.stop = True

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

    async def send_message(self, job_name: str, params: Dict[str, Any], cache_key: str=None) -> Dict[str, str]:
        """Send message to queue.

        Args:
            job_name (str): Name of job corresponding to a key in self.jobs
            params (Dict[str, Any]): Request parameters needed for job
            cache_key (str, optional): Results cache key. Defaults to None.

        Returns:
            Dict[str, str]: Contains sent message uuid or an error
        """

        if job_name not in self.job_names:
            raise ValueError(f"{job_name} not found in {self.job_names}")

        identifier = str(uuid4())
        items = {"uuid": identifier, "params": params, "cache_key": cache_key, "job_name": job_name}

        result = {}
        try:
            msg = orjson.dumps(items).decode()
            if self.queue_service == 'sqs':
                entries = [{'Id': str(uuid4()), 'MessageBody': msg, 'MessageGroupId': self.host_uuid}]
                await sqs.send_messages(queue=self.sqs_queue, region=self.sqs_region, entries=entries)
            else:
                self.cache.pool.rpush(f'{job_name}-not-started', msg)

            await self.cache.hmset(
                key=identifier,
                items={**items, **{'job_done': False, "results": None}},
                expire=self.expire_job_data
            )
            result['uuid'] = identifier
        except Exception as err:
            result['error'] = str(err)

        return result

    async def start_worker(self):
        """Continually run the worker."""

        while not self.stop:
            try:
                # run job the majority of the time pulling up to 10 messages to process
                for _ in range(10):
                    if self.queue_service == 'sqs':
                        await self.__sqs_pull_messages_and_run_jobs__()
                    else:
                        for job_name in self.job_names:
                            await self.__redis_pull_messages_and_run_jobs__(job_name)

                # verify processing only a fraction of the time
                if self.queue_service == 'redis':
                    for job_name in self.job_names:
                        await self.__verify_processing__(job_name)
            except asyncio.CancelledError:
                print(traceback.format_exc())
                break
            except Exception:
                print(traceback.format_exc())
                await asyncio.sleep(30)

        self.stop = False

    async def __sqs_pull_messages_and_run_jobs__(self):
        """Pull messages one at a time and run job.

        Raises:
            IOError: Redis access failed
        """

        # Since we cannot pull messages for specific jobs we use
        # the longest provided job timeout as the visibility_timeout
        msg = await sqs.get_messages(
            queue=self.sqs_queue,
            region=self.sqs_region,
            wait_time=1,
            visibility_timeout=self.longest_job_timeout,
            max_messages=1,
            attribute_names=['SentTimestamp']
        )

        if not msg:
            await asyncio.sleep(0.1)
        else:
            body = orjson.loads(msg[0]['Body'])
            key = body['cache_key']
            job_name = body['job_name']

            callback_url = body['params'].get('callback_url', '')
            body['params'].pop('callback_url', None)

            data = None if key is None else await self.cache.get(key)
            if data is None:
                # No results found in cache so run the job
                data = await self.jobs[job_name][0](body['params'])

                # Set the cached parameter based key with results
                if key is not None and not await self.cache.set(key, data, expire=self.expire_cached_result):
                    raise IOError(f"Setting cache string failed for cache_key: {key}")

            # Send results via POST request if necessary
            if callback_url:
                create_task(self.httpx_client.post(callback_url, json={'results': data, 'uuid': body['uuid']}, timeout=30))

            # Update the hashed UUID with processing results
            await self.cache.hmset(
                key=body['uuid'],
                items={**body, **{'results': data, 'job_done': True}},
                expire=self.expire_job_data
            )

            entries = [{'Id': str(uuid4()), 'ReceiptHandle': msg[0]['ReceiptHandle']}]
            await sqs.delete_messages(queue=self.sqs_queue, region=self.sqs_region, entries=entries)

            total = round(time() - float(msg[0]['Attributes']['SentTimestamp'])/1000, 3)
            print(f"Async college match processing time for UUID {body['uuid']} took {total} seconds")

    async def __redis_pull_messages_and_run_jobs__(self, job_name):
        """Pull messages one at a time and run job.

        Args:
            job_name (str): Name of job corresponding to a key in self.jobs

        Raises:
            IOError: Redis access failed
        """

        # in the future convert lpop to lmove and also look into integrating with async aioredis
        msg = self.cache.pool.lpop(f'{job_name}-not-started')
        if not msg:
            await asyncio.sleep(0.1)
        else:
            body = orjson.loads(msg)
            key = body['cache_key']

            callback_url = body['params'].get('callback_url', '')
            body['params'].pop('callback_url', None)

            # Add start time and push msg to <self.name>-in-process
            body['start_time'] = time()
            self.cache.pool.rpush(f'{job_name}-in-process', orjson.dumps(body).decode())

            data = None if key is None else await self.cache.get(key)
            if data is None:
                # No results found in cache so run the job
                data = await self.jobs[job_name][0](body['params'])

                # Set the cached parameter based key with results
                if key is not None and not await self.cache.set(key, data, expire=self.expire_cached_result):
                    raise IOError(f"Setting cache string failed for cache_key: {key}")

            # Send results via POST request if necessary
            if callback_url:
                create_task(self.httpx_client.post(callback_url, json={'results': data, 'uuid': body['uuid']}, timeout=30))

            # Update the hashed UUID with processing results
            await self.cache.hmset(
                key=body['uuid'],
                items={**body, **{'results': data, 'job_done': True}},
                expire=self.expire_job_data
            )

    async def __verify_processing__(self, job_name):
        """Verify processing completed fixing issues related to app crashing or
        scaling down servers when using queue_service = 'redis'.

        Args:
            job_name (str): Name of job corresponding to a key in self.jobs
        """

        timeout = self.jobs[job_name][1]
        for _ in range(10):
            msg = self.cache.pool.lpop(f'{job_name}-in-process')
            if not msg:
                break

            body = orjson.loads(msg)
            job_done = await self.cache.hget(key=body['uuid'], field='job_done')
            if job_done is None:
                pass    # if the cache is expired then we can typically ignore doing anything
            elif not job_done:
                if (time() - body['start_time']) > timeout:
                    print(f'{job_name} with uuid: {body["uuid"]} failed after {timeout} seconds. \
                            Pushing msg back to {job_name}-not-started.')
                    self.cache.pool.rpush(f'{job_name}-not-started', msg)
                else:
                    self.cache.pool.rpush(f'{job_name}-in-process', msg)

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
