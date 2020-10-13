'''pytest redis cache'''

# pylint: disable=c-extension-no-member

from asyncio import sleep

import pytest

pytestmark = pytest.mark.asyncio


async def test_build_cache_key(payload, cache):
    '''Test health check.'''

    key = await cache.build_cache_key(payload)
    assert key == 'opinion=[redis,rocks]|tool=pytest|version=python3'


async def test_set_one_item(payload, cache):
    '''Test set_one_item.'''

    key = await cache.build_cache_key(payload)
    await cache.set_one_item(cache_key=key, cache_value={'name': ['tim', 'walter', 'bryan'], 'app': 'aioradio'})
    await sleep(1)
    result = await cache.get_one_item(key)
    assert result['name'] == ['tim', 'walter', 'bryan']
    assert result['app'] == 'aioradio'

    result = await cache.delete_one_item(key)
    assert result == 1

async def test_set_one_item_with_hashed_key(payload, cache):
    '''Test set_one_item.'''

    key = await cache.build_cache_key(payload, use_hashkey=True)
    assert key == 'bdeb95a5154f7151eecaeadbcea52ed43d80d7338192322a53ef88a50ec7e94a'

    await cache.set_one_item(cache_key=key, cache_value={'name': ['tim', 'walter', 'bryan'], 'app': 'aioradio'})
    await sleep(1)
    result = await cache.get_one_item(key)
    assert result['name'] == ['tim', 'walter', 'bryan']
    assert result['app'] == 'aioradio'

    result = await cache.delete_one_item(key)
    assert result == 1


async def test_get_many_items(cache):
    '''Test get_many_items.'''

    await cache.set_one_item(cache_key='pytest-1', cache_value='one')
    await cache.set_one_item(cache_key='pytest-2', cache_value='two')
    await cache.set_one_item(cache_key='pytest-3', cache_value='three')
    results = await cache.get_many_items(['pytest-1', 'pytest-2', 'pytest-3'])
    assert results == ['one', 'two', 'three']
