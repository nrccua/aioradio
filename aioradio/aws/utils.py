
'''Generic async utility functions.'''

# pylint: disable=broad-except
# pylint: disable=logging-fstring-interpolation
# pylint: disable=protected-access

import asyncio
import logging
from asyncio import sleep
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from time import time
from typing import List

import aioboto3
import aiobotocore
import aiojobs

LOG = logging.getLogger(__name__)


@dataclass
class AwsServiceManager:
    '''AWS Service Manager'''

    service: str
    module: str = 'aiobotocore'
    sleep_interval: int = 300
    regions: List[str] = field(default_factory=list)
    scheduler: aiojobs._scheduler = None

    def __post_init__(self):
        '''Post constructor.'''

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

    def __del__(self):
        self.scheduler.close()

    async def create_scheduler(self):
        '''Schedule jobs.'''

        self.scheduler = await aiojobs.create_scheduler()
        if self.regions:
            for region in self.regions:
                await self.scheduler.spawn(self.aio_server(item='client', region=region))
                if self.module == 'aioboto3':
                    await self.scheduler.spawn(self.aio_server(item='resource', region=region))
        else:
            await self.scheduler.spawn(self.aio_server(item='client'))
            if self.module == 'aioboto3':
                await self.scheduler.spawn(self.aio_server(item='resource'))

    async def aio_server(self, item, region=None):
        '''Begin long running server establishing modules service_dict object.'''

        service_dict = self.service_dict if region is None else self.service_dict[region]
        service_dict = service_dict[item]
        await self.establish_client_resource(service_dict, item, region=region)

        while True:
            # sleep for defined interval
            await sleep(self.sleep_interval)

            # if all functions are inactive proceed to re-establishing AioSession client
            start = time()
            while service_dict['active'] and (time() - start) < 300:
                await sleep(0.001)

            await self.establish_client_resource(service_dict, item=item, region=region, reestablish=True)

    async def establish_client_resource(self, service_dict, item, region=None, reestablish=False):
        '''Establish the AioSession client or resource, then re-establish every self.sleep_interval seconds.'''

        kwargs = {'service_name': self.service, 'verify': False}
        if region is None:
            phrase = f're-establish {self.service}' if reestablish else f'establish {self.service}'
        else:
            kwargs['region_name'] = region
            phrase = f're-establish {self.service} {region}' if reestablish else f'establish {self.service} {region}'

        LOG.info(f'Atempting to {phrase} service object...')
        if reestablish:
            service_dict['busy'] = True
            await service_dict['obj'].__aexit__(None, None, None)

        if self.module == 'aiobotocore':
            service_dict['session'] = aiobotocore.get_session()
            service_dict['obj'] = await service_dict['session'].create_client(**kwargs).__aenter__()
        elif self.module == 'aioboto3':
            func = aioboto3.client if item == 'client' else aioboto3.resource
            service_dict['obj'] = await func(**kwargs).__aenter__()

        service_dict['busy'] = False
        LOG.info(f'Successfully {phrase} service object!')

    def get_region(self, args, kwargs):
        '''Attempt to detect the region from the kwargs or args.'''

        region = None

        if 'region' in kwargs:
            region = kwargs['region']
        else:
            for name in args:
                if name in self.regions:
                    region = name
                    break

        return region

    def active(self, func):
        '''Decorator to keep track of currently running functions, allowing the AioSession client
        to only be re-establish when the count is zero to avoid functions using a stale client.'''

        async def wrapper(*args, **kwargs):
            '''Decorator wrapper.'''

            obj = self.service_dict[self.get_region(args, kwargs)] if self.regions else self.service_dict

            # Make sure aiobotocore or aioboto3 isn't busy
            while obj['client']['busy'] or ('resource' in obj and obj['resource']['busy']):
                await sleep(0.01)

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
