"""Generic async utility functions."""

# pylint: disable=broad-except
# pylint: disable=logging-fstring-interpolation
# pylint: disable=protected-access
# pylint: disable=unnecessary-dunder-call

import asyncio
from asyncio import sleep
from copy import deepcopy
from dataclasses import dataclass, field
from time import time
from types import coroutine
from typing import Any, Dict, List

import aioboto3
import aiobotocore
import aiojobs
import backoff
import botocore


@dataclass
class AwsServiceManager:
    """AWS Service Manager."""

    service: str
    module: str = 'aiobotocore'
    sleep_interval: int = 300
    regions: List[str] = field(default_factory=list)
    scheduler: aiojobs._scheduler = None

    def __post_init__(self):
        services = ['s3', 'sqs', 'secretsmanager', 'dynamodb']
        if self.service not in services:
            raise ValueError(f'service parameter must be one of the following: {services}')

        modules = ['aiobotocore', 'aioboto3']
        if self.module not in modules:
            raise ValueError(f'module parameter must be one of the following: {modules}')

        data = {'client': {'obj': None, 'session': None, 'busy': True}, 'active': 0}
        if self.service == 'dynamodb':
            data['resource'] = {'obj': None, 'session': None, 'busy': True}
        self.service_dict = {region: deepcopy(data) for region in self.regions} if self.regions else data

        try:
            loop = asyncio.get_event_loop()
            if loop and loop.is_running():
                asyncio.create_task(self.create_scheduler())
            else:
                loop.run_until_complete(self.create_scheduler())
        except RuntimeError:
            loop = asyncio.new_event_loop().run_until_complete(self.create_scheduler())

    def add_regions(self, regions: List[str]):
        """Add regions to AWS service.

        Args:
            regions (List[str]): [description]
        """

        data = {'client': {'obj': None, 'session': None, 'busy': True}, 'active': 0}
        if self.service == 'dynamodb':
            data['resource'] = {'obj': None, 'session': None, 'busy': True}

        for region in regions:
            if region not in self.regions:
                self.regions.append(region)
                self.service_dict[region] = deepcopy(data)
                asyncio.get_event_loop().create_task(self.spawn_aws_service_process(region))

    def __del__(self):
        self.scheduler.close()

    async def create_scheduler(self):
        """Schedule jobs."""

        self.scheduler = await aiojobs.create_scheduler()
        if self.regions:
            for region in self.regions:
                await self.spawn_aws_service_process(region)
        else:
            await self.spawn_aws_service_process()

    async def spawn_aws_service_process(self, region: str=''):
        """Spawn scheduler process on a per region basis.

        Args:
            region (str, optional): AWS region. Defaults to ''.
        """

        if region:
            await self.scheduler.spawn(self.aio_server(item='client', region=region))
            if self.module == 'aioboto3':
                await self.scheduler.spawn(self.aio_server(item='resource', region=region))
        else:
            await self.scheduler.spawn(self.aio_server(item='client'))
            if self.module == 'aioboto3':
                await self.scheduler.spawn(self.aio_server(item='resource'))

    async def aio_server(self, item: str, region: str=''):
        """Begin long running server establishing modules service_dict object.

        Args:
            item (str): either 'client' or 'resource' depending on the aws service and python package
            region (str, optional): AWS region. Defaults to ''.
        """

        service_dict = self.service_dict if region == '' else self.service_dict[region]
        await self.establish_client_resource(service_dict[item], item, region=region)

        while True:
            # sleep for defined interval
            await sleep(self.sleep_interval)

            # if all functions are inactive proceed to re-establishing AioSession client
            start = time()
            while service_dict['active'] and (time() - start) < 300:
                await sleep(0.001)

            await self.establish_client_resource(service_dict[item], item=item, region=region, reestablish=True)

    @backoff.on_exception(backoff.expo, botocore.exceptions.ConnectTimeoutError, max_time=120)
    async def establish_client_resource(self, service_dict: Dict[str, Any], item: str, region: str='', reestablish: bool=False):
        """Establish the AioSession client or resource, then re-establish every
        self.sleep_interval seconds.

        Args:
            service_dict (Dict[str, Any]): dict containing info about the service requested
            item (str): either 'client' or 'resource' depending on the aws service and python package
            region (str, optional): AWS region. Defaults to ''.
            reestablish (bool, optional): should async context manager be reinstantiated. Defaults to False.
        """

        kwargs = {'service_name': self.service, 'verify': False}
        if region:
            kwargs['region_name'] = region

        if reestablish:
            service_dict['busy'] = True
            await service_dict['obj'].__aexit__(None, None, None)

        if self.module == 'aiobotocore':
            service_dict['session'] = aiobotocore.session.get_session()
            service_dict['obj'] = await service_dict['session'].create_client(**kwargs).__aenter__()
        elif self.module == 'aioboto3':
            service_dict['session'] = aioboto3.Session()
            func = service_dict['session'].client if item == 'client' else service_dict['session'].resource
            service_dict['obj'] = await func(**kwargs).__aenter__()

        service_dict['busy'] = False

    def get_region(self, args: List[Any], kwargs: Dict[str, Any]) -> str:
        """Attempt to detect the region from the kwargs or args.

        Args:
            args (List[Any]): list of arguments
            kwargs (Dict[str, Any]): Dict of keyword arguments

        Returns:
            str: AWS region
        """

        region = ''

        if 'region' in kwargs:
            region = kwargs['region']
        else:
            for name in args:
                if name in self.regions:
                    region = name
                    break

        return region

    def active(self, func: coroutine) -> Any:
        """Decorator to keep track of currently running functions, allowing the
        AioSession client to only be re-establish when the count is zero to
        avoid functions using a stale client.

        Args:
            func (coroutine): async coroutine

        Returns:
            Any: any
        """

        async def wrapper(*args, **kwargs) -> Any:
            """Decorator wrapper.

            Raises:
                error: some general error during function execution

            Returns:
                Any: any
            """

            obj = self.service_dict[self.get_region(args, kwargs)] if self.regions else self.service_dict

            # Make sure aiobotocore or aioboto3 isn't busy
            while obj['client']['busy'] or ('resource' in obj and obj['resource']['busy']):
                await sleep(0.001)

            result = None
            error = None
            obj['active'] += 1
            try:
                result = await func(*args, **kwargs)
            except Exception as err:
                error = err
            finally:
                obj['active'] -= 1
                if error is not None:
                    raise error

            return result

        return wrapper
