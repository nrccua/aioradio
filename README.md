# aioradio
Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more.

## REDIS example code
We build upon aioredis to further abstract some of the gotchas to help make using aioredis even simplier.

```python
import asyncio

from aioradio.redis import Redis

# We use the async main function as an example, but we could have used fastapi's on_startup function similarly
async def main():
    config = {'redis_primary_endpoint': 'your-redis-endpoint'}
    redis = Redis(config=config, use_json=True, expire=60, use_hashkey=False)

    # since aioredis.create_redis_pool is a coroutine we need to instantiate the object within an async function
    if redis.pool_task is not None:
        redis.pool = await redis.pool_task

    # we can override the global expire and since we are using json the cache_value will be converted to json
    await redis.set_one_item(cache_key='aioradio', cache_value={'a': 'alpha', 'number': 123}, expire=2)

    result = await redis.get_one_item(cache_key='aioradio')
    print(f"retrieved cached value = {result}")

    await asyncio.sleep(3)
    result = await redis.get_one_item(cache_key='aioradio')
    print(f"wait 3 seconds and retrieved cached value = {result}")

    # we can get more than one cached item at a time
    await redis.set_one_item(cache_key='aioradio1', cache_value='one')
    await redis.set_one_item(cache_key='aioradio2', cache_value='two')
    await redis.set_one_item(cache_key='aioradio3', cache_value='three')
    results = await redis.get_many_items(['aioradio1', 'aioradio2', 'aioradio3'])
    print(f"Cached items retrieved = {results}")

    # build a cache key from a python unnested dictionary object, sorted by key
    # if the value is None or an empty list than exclude from adding to the cache key
    some_dict = {'code': 'aioradio', 'opinion': ['redis', 'rocks'], 'none': None, 'empty': [], 'rad': True}
    key = await redis.build_cache_key(some_dict)
    print(key)

asyncio.get_event_loop().run_until_complete(main())
```


## AWS S3 example code
aioradio abstracts using aiobotocore and aioboto3 making async AWS funtion calls simple one liners.
Besides what is shown below in the examples, there is also support for SQS, DynamoDB and Secrets Manager.


```python
import asyncio

from aioradio.aws.s3 import create_bucket
from aioradio.aws.s3 import delete_s3_object
from aioradio.aws.s3 import download_file
from aioradio.aws.s3 import get_object
from aioradio.aws.s3 import list_s3_objects
from aioradio.aws.s3 import upload_file

async def main():
    s3_bucket = 'aioradio'
    s3_prefix = 'test'
    filename = 'hello_world.txt'
    s3_key = f'{s3_prefix}/{filename}'

    # create an s3 bucket called aioradio
    await create_bucket(bucket=s3_bucket)

    # create hello_world.txt file
    with open(filename, 'w') as file_handle:
        file_handle.write('hello world of aioradio!')

    # upload the file from s3 and confirm it now exists in s3
    await upload_file(bucket=s3_bucket, filepath=filename, s3_key=s3_key)
    assert s3_key in await list_s3_objects(bucket=s3_bucket, s3_prefix=s3_prefix)

    # test downloading the file
    await download_file(bucket=s3_bucket, filepath=filename, s3_key=s3_key)

    # test getting file data to object
    result = await get_object(bucket=s3_bucket, s3_key=s3_key)
    assert result == b'hello world of aioradio!'

    # delete the file from s3
    await delete_s3_object(bucket=s3_bucket, s3_prefix=s3_key)
    assert s3_key not in await list_s3_objects(bucket=s3_bucket, s3_prefix=s3_prefix)

asyncio.get_event_loop().run_until_complete(main())
```

## MSSQL example code
aioredis uses the pyodbc library to work with ODBC databases.
It currently has support for connecting and sending queries to mssql.

```python
import asyncio

from aioradio.pyodbc import establish_pyodbc_connection
from aioradio.pyodbc import pyodbc_query_fetchone
from aioradio.pyodbc import pyodbc_query_fetchall

async def main():
    conn = await establish_pyodbc_connection(host='your-host', user='your-user', pwd='your-password')

    query = "SELECT homeruns FROM MLB.dbo.LosAngelesAngels WHERE lastname = 'Trout' AND year = '2020'"
    row = await pyodbc_query_fetchone(conn=conn, query=query)
    print(row)

    query = "SELECT homeruns FROM MLB.dbo.LosAngelesAngels WHERE lastname = 'Trout'"
    rows = await pyodbc_query_fetchall(conn=conn, query=query)
    print(rows)


asyncio.get_event_loop().run_until_complete(main())
```

## Jira example code
Jira uses the async library httpx behind the scene to send http requests.

```python
import asyncio

from aioradio.jira import add_comment_to_jira
from aioradio.jira import get_jira_issue
from aioradio.jira import post_jira_issue

async def main():

    # create a jira ticket
    url = 'https://aioradio.atlassian.net/rest/api/2/issue/'
    payload = {
        "fields": {
            "project": {"key": "aioradio"},
            "issuetype": {"name": "Task"},
            "reporter": {"accountId": "somebodies-account-id"},
            "priority": {"name": "Medium"},
            "summary": "Aioradio rocks!",
            "description": "Aioradio Review",
            "labels": ["aioradio"],
            "assignee": {"accountId": "somebodies-account-id"}
        }
    }
    resp = await post_jira_issue(url=url, jira_user='your-user', jira_token='your-password', payload=payload)
    jira_id = resp.json()['key']

    # get jira ticket info
    resp = await get_jira_issue(url=f'{url}/{jira_id}', jira_user='your-user', jira_token='your-password')

    # add comment to jira ticket
    comment = 'aioradio rocks!'
    response = await add_comment_to_jira(url=url, jira_user='your-user', jira_token='your-password', comment=comment)

asyncio.get_event_loop().run_until_complete(main())
```

## INSTALLING FOR DIRECT DEVELOPMENT OF AIORADIO

Install [python 3.9.X](https://www.python.org/downloads/)

Make sure you've installed [ODBC drivers](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15), required for using the python package pyodbc.

Clone aioradio locally and navigate to the root directory

Install and activate python VirtualEnv
```bash
python3.9 -m venv env
source env/bin/activate
```

Install python modules included in requirements.txt
```bash
pip install -r aioradio/requirements.txt
```

Run Makefile command from the root directory to test all is good before issuing push to master
```
make all
```

## AUTHORS

* **Tim Reichard** - [aioradio](https://github.com/nrccua/aioradio)

See also the list of [contributors](https://github.com/nrccua/aioradio/graphs/contributors) who participated in this project.

## ACKNOWLEDGEMENTS

* **Bryan Cusatis** - Architect contributing to aioradio.
* **Kyle Edwards** - Developer contributing to aioradio.
* **Pedro Artiga** - Developer contributing to aioradio.
