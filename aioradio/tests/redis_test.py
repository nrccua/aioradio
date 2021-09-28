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
    assert result == 4

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
    assert result == 5
    result = await cache.hgetall(key='address_hash', use_json=False)
    assert result == items

    await cache.hmset(key='tim', items={'firstname': 'Tim', 'lastname': 'Reichard', 'avg': '190'}, expire=1, use_json=False)
    await cache.hmset(key='don', items={'firstname': 'Don', 'lastname': 'Mattingly', 'avg': '325'}, expire=1, use_json=False)
    result = await cache.hmget_many(keys=['tim', 'don', 'fake'], fields=['firstname', 'lastname'], use_json=False)
    assert len(result) == 3
    assert result[-1] == {}

    result = await cache.hgetall_many(keys=['tim', 'don', 'fake'], use_json=False)
    assert len(result) == 3
    assert result[0]['avg'] == '190'
    assert result[-1] == {}


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

    await cache.set(key='set_simple_key', value='aioradio is superb', use_json=False)
    result = await cache.get('set_simple_key', use_json=False)
    assert result == 'aioradio is superb'
    result = await cache.mget(items=['set_simple_key'], use_json=False)
    assert result == ['aioradio is superb']


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

async def test_delete_many_items(cache):
    """Test delete_many."""

    await cache.set(key='delete-many-1', value='one')
    await cache.set(key='delete-many-2', value='two')
    await cache.set(key='delete-many-3', value='three')

    results = await cache.mget(['delete-many-1', 'delete-many-2', 'delete-many-3'])
    assert results == ['one', 'two', 'three']

    total = await cache.delete_many('delete-many*')
    assert total == 3

    results = await cache.mget(['delete-many-1', 'delete-many-2', 'delete-many-3'])
    assert results == [None, None, None]
