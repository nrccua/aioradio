"""pytest redis cache."""

# pylint: disable=c-extension-no-member

from asyncio import sleep

import pytest

pytestmark = pytest.mark.asyncio


async def test_build_cache_key(payload, cache):
    """Test health check."""

    key = await cache.build_cache_key(payload)
    assert key == 'opinion=[redis,rocks]|tool=pytest|version=python3'


async def test_hash_redis_functions(cache):
    """Test setting hash."""

    result = await cache.hset(key='simple_hash', field='aioradio', value='rocks', expire=1)
    assert result == 1

    result = await cache.hget(key='simple_hash', field='aioradio')
    assert result == 'rocks'

    result = await cache.hget(key='simple_hash', field='does not exist')
    assert result is None

    await sleep(2)
    result = await cache.hget(key='simple_hash', field='aioradio')
    assert result is None

    result = await cache.hget(key='fake_hash', field='aioradio')
    assert result is None

    items = {
        'name': 'Tim Reichard',
        'team': 'Architecture',
        'apps': ['EFI', 'RACE', 'Airflow-ETL', 'Narwhal', 'aioradio'],
        'football': {
            'team': [
            {
                'name': 'Tampa Bay Bucs',
                'rank': '1'
            },
            {
                'name': 'Kansas City Chiefs',
                'rank': '2'
            }]
        }
    }

    result = await cache.hmset(key='complex_hash', items=items, expire=1)
    assert result == 1

    result = await cache.hmget(key='complex_hash', fields=['name', 'team', 'apps', 'fake'])
    assert 'fake' not in result
    assert 'aioradio' in result['apps']

    result = await cache.hgetall(key='complex_hash')
    assert result['football']['team'][1]['name'] == 'Kansas City Chiefs'

    result = await cache.hdel(key='complex_hash', fields=['team', 'apps'])
    assert result == 2
    result = await cache.hexists(key='complex_hash', field='team')
    assert result is False

    await sleep(2)
    result = await cache.hgetall(key='complex_hash')
    assert result == {}

    items = {'state': 'TX', 'city': 'Austin', 'zipcode': '78745', 'addr1': '8103 Shiloh Ct.', 'addr2': ''}
    result = await cache.hmset(key='address_hash', items=items, expire=1, use_json=False)
    assert result == 1
    result = await cache.hgetall(key='address_hash', use_json=False)
    assert result == items


async def test_set_one_item(payload, cache):
    """Test set_one_item."""

    key = await cache.build_cache_key(payload)
    await cache.set(key=key, value={'name': ['tim', 'walter', 'bryan'], 'app': 'aioradio'})
    await sleep(1)
    result = await cache.get(key)
    assert result['name'] == ['tim', 'walter', 'bryan']
    assert result['app'] == 'aioradio'

    result = await cache.delete(key)
    assert result == 1

async def test_set_one_item_with_hashed_key(payload, cache):
    """Test set_one_item."""

    key = await cache.build_cache_key(payload, use_hashkey=True)
    assert key == 'bdeb95a5154f7151eecaeadbcea52ed43d80d7338192322a53ef88a50ec7e94a'

    await cache.set(key=key, value={'name': ['tim', 'walter', 'bryan'], 'app': 'aioradio'})
    await sleep(1)
    result = await cache.get(key)
    assert result['name'] == ['tim', 'walter', 'bryan']
    assert result['app'] == 'aioradio'

    result = await cache.delete(key)
    assert result == 1


async def test_get_many_items(cache):
    """Test get_many_items."""

    await cache.set(key='pytest-1', value='one')
    await cache.set(key='pytest-2', value='two')
    await cache.set(key='pytest-3', value='three')
    results = await cache.mget(['pytest-1', 'pytest-2', 'pytest-3'])
    assert results == ['one', 'two', 'three']
