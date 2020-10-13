'''pytest dynamodb'''

from random import randint
from decimal import Decimal
from uuid import uuid4

import pytest
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key

from aioradio.aws.dynamodb import batch_get_items_from_dynamo
from aioradio.aws.dynamodb import batch_write_to_dynamo
from aioradio.aws.dynamodb import get_list_of_dynamo_tables
from aioradio.aws.dynamodb import put_item_in_dynamo
from aioradio.aws.dynamodb import query_dynamo
from aioradio.aws.dynamodb import scan_dynamo
from aioradio.aws.dynamodb import update_item_in_dynamo

# ****************************************
# DO NOT CHANGE THE DB_TABLE OR REGION
# ****************************************
DB_TABLE = 'pytest'
REGION = 'us-east-2'

pytestmark = pytest.mark.asyncio


async def test_dynamodb_create_table(create_table):
    '''Test creating a DynamoDB table.'''

    result = await create_table(table_name=DB_TABLE)
    assert result == DB_TABLE


async def test_dynamodb_get_list_of_tables():
    '''Test getting list of DynamoDB tables and our created table is in the list.'''

    assert DB_TABLE in await get_list_of_dynamo_tables(region=REGION)


@pytest.mark.parametrize('fice', ['XXXXXX', '012345', '999999'])
async def test_dynamo_put_item(fice):
    '''Test writing an item to DynamoDB table.'''

    item = {
        'fice': fice,
        'data': {
            'unique_id': {
                'uuid': str(uuid4()),
                'app': 'pytest',
                'user': 'Tim Reichard',
                'records': randint(0, 10),
                'pie': Decimal('3.14')
            }
        }
    }
    result = await put_item_in_dynamo(table_name=DB_TABLE, region=REGION, item=item)
    assert 'ResponseMetadata' in result and result['ResponseMetadata']['HTTPStatusCode'] == 200


async def test_dynamo_update_item():
    '''Test updating a nested value within an item.'''

    result = await update_item_in_dynamo(
        table_name=DB_TABLE,
        region=REGION,
        key={'fice': 'XXXXXX'},
        update_expression='SET #data.#unique_id.#uuid = :uuid',
        expression_attribute_names={'#data': 'data', '#unique_id': 'unique_id', '#uuid': 'uuid'},
        expression_attribute_values={':uuid': str(uuid4())}
    )
    assert 'ResponseMetadata' in result and result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = await update_item_in_dynamo(
        table_name=DB_TABLE,
        region=REGION,
        key={'fice': 'XXXXXX'},
        update_expression='REMOVE #data.#unique_id.#pie',
        expression_attribute_names={'#data': 'data', '#unique_id': 'unique_id', '#pie': 'pie'},
        condition_expression='attribute_exists(#data.#unique_id.#pie)'
    )
    assert 'ResponseMetadata' in result and result['ResponseMetadata']['HTTPStatusCode'] == 200


async def test_dynamo_write_batch():
    '''Test writing a batch of items to DynamoDB.'''

    items = []
    for fice in ['000000', '911911', '102779']:
        items.append({
            'fice': fice,
            'data': {
                'unique_id': {
                    'uuid': str(uuid4()),
                    'app': 'pytest',
                    'user': 'Tim Reichard',
                    'records': randint(0, 10),
                    'pie': Decimal('3.14')
                }
            }
        })
    assert await batch_write_to_dynamo(table_name=DB_TABLE, region=REGION, items=items)


async def test_batch_get_items_from_dynamo():
    '''Test getting batch of items from dynamo.'''

    items = [{'fice': fice} for fice in ['000000', '911911', '102779']]
    results = await batch_get_items_from_dynamo(table_name=DB_TABLE, region=REGION, items=items)
    assert len(results['Responses']['pytest']) == 3
    for item in results['Responses']['pytest']:
        assert item['data']['unique_id']['app'] == 'pytest'


async def test_dynamo_query():
    '''Test querying data from dynamoDB.'''

    key_condition_expression = Key('fice').eq('XXXXXX')
    result = await query_dynamo(table_name=DB_TABLE, region=REGION, key=key_condition_expression)
    assert len(result) == 1


async def test_dynamo_scan_table():
    '''Test scanning DynamoDB table.'''

    filter_expression = Attr('data.unique_id.records').between(-10, -1)
    result = await scan_dynamo(table_name=DB_TABLE, region=REGION, key=filter_expression)
    assert not result

    filter_expression = Attr('data.unique_id.records').gte(0)
    result = await scan_dynamo(table_name=DB_TABLE, region=REGION, key=filter_expression)
    assert len(result) == 6
