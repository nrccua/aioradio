"""Long Running Jobs worker script."""

# pylint: disable=broad-except
# pylint: disable=c-extension-no-member
# pylint: disable=logging-fstring-interpolation

import asyncio
import traceback
from dataclasses import dataclass, field
from time import time
from typing import Any, Dict
from uuid import uuid4

import orjson

from aioradio.redis import Redis


@dataclass
class LongRunningJobs:
    """Worker that continually pulls from Redis list (implemented like queue),
    running a job using request parameters conveyed in the message.

    Also has a pre-processing function to send messages to the Redis
    list.
    """

    redis_host: str
    cache_expiration: int = 3600
    worker_active: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
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

    async def send_message(self, job_name: str, params: Dict[str, Any], cache_key: str=None) -> Dict[str, str]:
        """Send message to Redis list.

        Args:
            job_name (str): Name of the long running job.  Used to identify between multiple jobs running within app.
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
            self.cache.pool.rpush(f'{job_name}-not-started', orjson.dumps(items).decode())
            await self.cache.hmset(key=identifier, items=items)
            result['uuid'] = identifier
        except Exception as err:
            result['error'] = str(err)

        return result

    async def start_worker(self, job_name: str, job: Any, job_timeout: float=30):
        """Continually run the worker."""

        self.worker_active[job_name] = True
        while True:
            while self.worker_active[job_name]:
                try:
                    # run job the majority of the time pulling up to 10 messages to process
                    for _ in range(10):
                        await self.__pull_messages_and_run_jobs__(job_name, job)

                    # verify processing only a fraction of the time
                    await self.__verify_processing__(job_name, job_timeout)
                except asyncio.CancelledError:
                    print(traceback.format_exc())
                    break
                except Exception:
                    print(traceback.format_exc())
                    await asyncio.sleep(30)
            await asyncio.sleep(1)

    async def stop_worker(self, job_name: str):
        """Stop worker associated with job_name.

        Args:
            job_name (str): Name of the long running job.  Used to identify between multiple jobs running within app.
        """

        self.worker_active[job_name] = False

    async def __pull_messages_and_run_jobs__(self, job_name: str, job: Any):
        """Pull messages one at a time and run job.

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

            # Add start time and push msg to <job_name>-in-process
            body['start_time'] = time()
            self.cache.pool.rpush(f'{job_name}-in-process', orjson.dumps(body).decode())

            data = None if key is None else await self.cache.get(key)
            if data is None:
                # No results found in cache so run the job
                data = await job(body['params'])

                # Set the cached parameter based key with results
                if key is not None and not await self.cache.set(key, data):
                    raise IOError(f"Setting cache string failed for cache_key: {key}")

            # Update the hashed UUID with processing results
            await self.cache.hmset(key=body['uuid'], items={**body, **{'results': data, 'job_done': True}})

    async def __verify_processing__(self, job_name: str, job_timeout: float):
        """Verify processing completed fixing issues related to app crashing or
        scaling down servers."""

        for _ in range(10):
            msg = self.cache.pool.lpop(f'{job_name}-in-process')
            if not msg:
                break

            body = orjson.loads(msg)
            job_done = await self.cache.hget(key=body['uuid'], field='job_done')
            if job_done is None:
                pass    # if the cache is expired then we can typically ignore doing anything
            elif not job_done:
                if (time() - body['start_time']) > job_timeout:
                    print(f'Failed processing uuid: {body["uuid"]} in {job_timeout} seconds.  Pushing msg back to {job_name}-not-started.')
                    self.cache.pool.rpush(f'{job_name}-not-started', msg)
                else:
                    self.cache.pool.rpush(f'{job_name}-in-process', msg)

    @staticmethod
    async def build_cache_key(params: Dict[str, Any], separator='|') -> str:
        """build a cache key from a dictionary object.

        Concatenate and
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
