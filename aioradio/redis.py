"""Aioradio redis cache script."""

# pylint: disable=c-extension-no-member
# pylint: disable=no-member
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes
# pylint: disable=unsubscriptable-object

import hashlib
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, List, Union

import fakeredis
import orjson
import redis

HASH_ALGO_MAP = {
    'SHA1': hashlib.sha1,
    'SHA224': hashlib.sha224,
    'SHA256': hashlib.sha256,
    'SHA384': hashlib.sha384,
    'SHA512': hashlib.sha512,
    'SHA3_224': hashlib.sha3_224,
    'SHA3_256': hashlib.sha3_256,
    'SHA3_384': hashlib.sha3_384,
    'SHA3_512': hashlib.sha3_512
}


@dataclass
class Redis:
    """Class dealing with redis functions."""

    config: Dict[str, Any] = dataclass_field(default_factory=dict)
    pool: redis.Redis = dataclass_field(init=False, repr=False)

    # Cache expiration in seconds
    expire: int = 60

    # If the key contains sensitive info than you can hash the cache key using various hash algorithms
    use_hashkey: bool = False
    hash_algorithm: str = 'SHA3_256'

    # If you want to pass in an object and let this class convert to json or
    # retrieve value letting this class convert from json set use_json = True.
    use_json: bool = True

    # If running test cases use fakeredis
    fake: bool = False

    def __post_init__(self):
        if self.fake:
            self.pool = fakeredis.FakeRedis(encoding='utf-8', decode_responses=True)
        else:
            primary_endpoint = self.config["redis_primary_endpoint"]
            if "encoding" in self.config:
                self.pool = redis.Redis(host=primary_endpoint, encoding=self.config["encoding"], decode_responses=True)
            else:
                self.pool = redis.Redis(host=primary_endpoint)

    async def get(self, key: str, use_json: bool=None, encoding: Union[str, None]=None) -> Any:
        """Check if an item is cached in redis.

        Args:
            key (str): redis cache key
            use_json (bool, optional): convert json value to object. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        value = self.pool.get(key)

        if value is not None:
            if encoding is not None:
                value = value.decode(encoding)
            if use_json:
                value = orjson.loads(value)

        return value

    async def mget(self, items: List[str], use_json: bool=None, encoding: Union[str, None]=None) -> List[Any]:
        """Check if many items are cached in redis.

        Args:
            items (List[str]): list of redis cache keys
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: list of objects
        """

        if use_json is None:
            use_json = self.use_json

        values = self.pool.mget(*items)

        results = []
        for val in values:
            if val is not None:
                if encoding is not None:
                    val = val.decode(encoding)
                if use_json:
                    val = orjson.loads(val)
            results.append(val)

        return results

    async def set(self, key: str, value: str, expire: int=None, use_json: bool=None) -> int:
        """Set one key-value pair in redis.

        Args:
            key (str): redis cache key
            value (str): redis cache value
            expire (int, optional): cache expiration. Defaults to None.
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.

        Returns:
            int: 1 if key is set successfully else 0
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            value = orjson.dumps(value)

        return self.pool.set(key, value, ex=expire)

    async def delete(self, key: str) -> int:
        """Delete key from redis.

        Args:
            key (str): redis cache key

        Returns:
            int: 1 if key is found and deleted else 0
        """

        return self.pool.delete(key)

    async def delete_many(self, pattern: str, max_batch_size: int=500) -> int:
        """Delete all keys it matches the desired pattern.

        Args:
            pattern (str): cache key pattern
            max_batch_size (int): size of pipeline batch, defaults to 500, can be adjusted to increase performance

        Returns:
            int: total of deleted keys
        """
        pipe = self.pool.pipeline()
        total = 0
        batch_size = 0

        for key in self.pool.scan_iter(pattern):
            pipe.delete(key)
            total = total + 1
            batch_size = batch_size + 1

            if batch_size == max_batch_size:
                pipe.execute()
                batch_size = 0

        pipe.execute()

        return total

    async def hget(self, key: str, field: str, use_json: bool=None, encoding: Union[str, None]=None) -> Any:
        """Get the value of a hash field.

        Args:
            key (str): cache key
            field (str): hash field
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        value = self.pool.hget(key, field)

        if value is not None:
            if encoding is not None:
                value = value.decode(encoding)
            if use_json:
                value = orjson.loads(value)

        return value

    async def hmget(self, key: str, fields: List[str], use_json: bool=None, encoding: Union[str, None]=None) -> Any:
        """Get the values of all the given fields.

        Args:
            key (str): cache key
            fields (List[str]): hash fields
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        items = {}
        for index, value in enumerate(self.pool.hmget(key, *fields)):
            if value is not None:
                if encoding is not None:
                    value = value.decode(encoding)
                if use_json:
                    value = orjson.loads(value)
                items[fields[index]] = value

        return items

    async def hmget_many(self, keys: List[str], fields: List[str], use_json: bool=None, encoding: Union[str, None]=None) -> List[Any]:
        """Get the values of all the given fields for many hashed keys.

        Args:
            keys (List[str]): cache keys
            fields (List[str]): hash fields
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: any
        """

        if use_json is None:
            use_json = self.use_json

        pipeline = self.pool.pipeline()
        for key in keys:
            pipeline.hmget(key, *fields)

        results = []
        for values in pipeline.execute():
            items = {}
            for index, value in enumerate(values):
                if value is not None:
                    if encoding is not None:
                        value = value.decode(encoding)
                    if use_json:
                        value = orjson.loads(value)
                    items[fields[index]] = value
            results.append(items)

        return results

    async def hgetall(self, key: str, use_json: bool=None, encoding: Union[str, None]=None) -> Any:
        """Get all the fields and values in a hash.

        Args:
            key (str): cache key
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        items = {}
        for hash_key, value in self.pool.hgetall(key).items():
            if encoding is not None:
                hash_key = hash_key.decode(encoding)
            if value is not None:
                if encoding is not None:
                    value = value.decode(encoding)
                if use_json:
                    value = orjson.loads(value)
                items[hash_key] = value

        return items

    async def hgetall_many(self, keys: List[str], use_json: bool=None, encoding: Union[str, None]=None) -> List[Any]:
        """Get all the fields and values in a hash.

        Args:
            keys (str): cache keys
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: any
        """

        if use_json is None:
            use_json = self.use_json

        pipeline = self.pool.pipeline()
        for key in keys:
            pipeline.hgetall(key)

        results = []
        for item in pipeline.execute():
            items = {}
            for key, value in item.items():
                if encoding is not None:
                    key = key.decode(encoding)
                if value is not None:
                    if encoding is not None:
                        value = value.decode(encoding)
                    if use_json:
                        value = orjson.loads(value)
                items[key] = value
            results.append(items)

        return results

    async def hset(self, key: str, field: str, value: str, use_json: bool=None, expire: int=None) -> int:
        """Set the string value of a hash field.

        Args:
            key (str): cache key
            field (str): hash field
            value (str): hash value
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.
            expire (int, optional): cache expiration. Defaults to None.

        Returns:
            int: 1 if key is set successfully else 0
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            value = orjson.dumps(value)

        pipeline = self.pool.pipeline()
        pipeline.hset(key, field, value)
        pipeline.expire(key, time=expire)
        result, _ = pipeline.execute()

        return result

    async def hmset(self, key: str, items: Dict[str, Any], use_json: bool=None, expire: int=None) -> bool:
        """Set the string value of a hash field.

        Args:
            key (str): cache key
            items (Dict[str, Any]): list of redis hash key-value pairs
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.
            expire (int, optional): cache expiration. Defaults to None.

        Returns:
            bool: True if hash is set successfully else False
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            modified_items = {k: orjson.dumps(v) for k, v in items.items()}
            items = modified_items

        pipeline = self.pool.pipeline()
        pipeline.hset(key, mapping=items)
        pipeline.expire(key, time=expire)
        result, _ = pipeline.execute()
        return  result

    async def hdel(self, key: str, fields: List[str]) -> int:
        """Delete one or more hash fields.

        Args:
            key (str): hash key
            fields (List[str]): hash fields

        Returns:
            int: Number of hash fields deleted
        """

        return self.pool.hdel(key, *fields)

    async def hexists(self, key: str, field: str) -> bool:
        """Determine if hash field exists.

        Args:
            key (str): hash key
            field (str): hash field

        Returns:
            int: True if hash field exists else False
        """

        return self.pool.hexists(key, field)

    async def build_cache_key(self, payload: Dict[str, Any], separator='|', use_hashkey: bool=None) -> str:
        """Build a cache key from a dictionary object. Concatenate and
        normalize key-values from an unnested dict, taking care of sorting the
        keys and each of their values (if a list).

        Args:
            payload (Dict[str, Any]): dict object to use to build cache key
            separator (str, optional): character to use as a separator in the cache key. Defaults to '|'.
            use_hashkey (bool, optional): use a hashkey for the cache key. Defaults to None.

        Returns:
            str: dict object converted to string
        """

        if use_hashkey is None:
            use_hashkey = self.use_hashkey

        keys = sorted(payload.keys())
        concat_key = separator.join([f'{k}={orjson.dumps(payload[k], option=orjson.OPT_SORT_KEYS).decode()}'.replace('"', '')
                                    for k in keys if payload[k] != [] and payload[k] is not None])

        if use_hashkey:
            concat_key = HASH_ALGO_MAP[self.hash_algorithm](concat_key.encode()).hexdigest()

        return concat_key
