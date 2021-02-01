"""Generic async AWS functions for DynamoDB."""

# pylint: disable=too-many-arguments

from typing import Any, Dict, List

from aioradio.aws.utils import AwsServiceManager

AWS_SERVICE = AwsServiceManager(service='dynamodb', regions=['us-east-1'], module='aioboto3')
DYNAMO = AWS_SERVICE.service_dict


async def add_regions(regions: List[str]):
    """Add regions to DynamoDB AWS service.

    Args:
        regions (List[str]): List of AWS regions
    """

    AWS_SERVICE.add_regions(regions)


@AWS_SERVICE.active
async def create_dynamo_table(
        table_name: str,
        region: str,
        attribute_definitions: List[Dict[str, str]],
        key_schema: List[Dict[str, str]],
        provisioned_throughput: Dict[str, int]) -> str:
    """Create dynamo table.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        attribute_definitions (List[Dict[str, str]]): an attribute for describing the key schema for the table
        key_schema (List[Dict[str, str]]): attributes that make up the primary key of a table, or the key attributes of an index
        provisioned_throughput (Dict[str, int]): Throughput (ReadCapacityUnits & WriteCapacityUnits) for the dynamo table

    Returns:
        str: error message if any
    """

    error = ''

    try:
        table = await DYNAMO[region]['client']['obj'].create_table(
            TableName=table_name,
            AttributeDefinitions=attribute_definitions,
            KeySchema=key_schema,
            ProvisionedThroughput=provisioned_throughput
        )
        await table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    except DYNAMO[region]['client']['obj'].exceptions.ResourceInUseException as err:
        error = err.response['Error']['Message']

    return error


@AWS_SERVICE.active
async def get_list_of_dynamo_tables(region: str) -> List[str]:
    """Get list of Dynamo tables in a particular region.

    Args:
        region (str): AWS region

    Returns:
        List[str]: list of dynamo tables
    """

    tables = []
    result = await DYNAMO[region]['client']['obj'].list_tables()
    tables = result['TableNames']

    return tables


@AWS_SERVICE.active
async def scan_dynamo(table_name: str, region: str, key: Any=None) -> List[Any]:
    """Scan dynamo table using a filter_expression if supplied.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        key (Any, optional): filter expression to reduce items scaned. Defaults to None.

    Returns:
        List[Any]: list of scanned items
    """

    result = []
    scan_kwargs = {'FilterExpression': key} if key is not None else {}

    while True:
        table = await DYNAMO[region]['resource']['obj'].Table(table_name)
        resp = await table.scan(**scan_kwargs)
        result.extend(resp['Items'])
        if 'LastEvaluatedKey' not in resp:
            break
        scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']

    return result


@AWS_SERVICE.active
async def put_item_in_dynamo(table_name: str, region: str, item: Dict[str, Any]) -> Dict[str, Any]:
    """Put item in dynamo table.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        item (Dict[str, Any]): items to add/modifiy in dynamo table

    Returns:
        Dict[str, Any]: response of operation
    """

    result = {}
    table = await DYNAMO[region]['resource']['obj'].Table(table_name)
    result = await table.put_item(Item=item)
    return result


@AWS_SERVICE.active
async def query_dynamo(table_name: str, region: str, key: Any) -> List[Any]:
    """Query dynamo for with specific key_condition_expression.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        key (Any): KeyConditionExpression parameter to provide a specific value for the partition key

    Returns:
        List[Any]: [description]
    """

    result = []
    query_kwargs = {'KeyConditionExpression': key}

    while True:
        table = await DYNAMO[region]['resource']['obj'].Table(table_name)
        resp = await table.query(**query_kwargs)
        result.extend(resp['Items'])
        if 'LastEvaluatedKey' not in resp:
            break
        query_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']

    return result


@AWS_SERVICE.active
async def update_item_in_dynamo(
        table_name: str,
        region: str,
        key: Dict[str, Any],
        update_expression: str,
        expression_attribute_names: str,
        expression_attribute_values: str='',
        condition_expression: str='',
        return_values: str='UPDATED_NEW') -> Dict[str, Any]:
    """Update an item in Dynamo without overwriting the entire item.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        key (Dict[str, Any]): partition key and sort key if applicable
        update_expression (str): attributes to be updated, the action to be performed on them, and new value(s) for them
        expression_attribute_names (str): one or more substitution tokens for attribute names in an expression
        expression_attribute_values (str, optional): one or more values that can be substituted in an expression. Defaults to ''.
        condition_expression (str, optional): condition that must be satisfied in order for a conditional update to succeed. Defaults to ''.
        return_values (str, optional): items to return in response. Defaults to 'UPDATED_NEW'.

    Returns:
        Dict[str, Any]: [description]
    """

    result = {}
    update_kwargs = {
        'Key': key,
        'UpdateExpression': update_expression,
        'ExpressionAttributeNames': expression_attribute_names,
        'ReturnValues': return_values
    }
    if expression_attribute_values:
        update_kwargs['ExpressionAttributeValues'] = expression_attribute_values
    if condition_expression:
        update_kwargs['ConditionExpression'] = condition_expression

    table = await DYNAMO[region]['resource']['obj'].Table(table_name)
    result = await table.update_item(**update_kwargs)

    return result



@AWS_SERVICE.active
async def batch_write_to_dynamo(table_name: str, region: str, items: List[Dict[str, Any]]) -> bool:
    """Write batch of items to dynamo table.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        items (List[Dict[str, Any]]): items to write to dynamo table

    Returns:
        bool: success status of writing items
    """

    batch_writer_successful = False

    table = await DYNAMO[region]['resource']['obj'].Table(table_name)
    async with table.batch_writer() as db_writer:
        for item in items:
            await db_writer.put_item(Item=item)

        batch_writer_successful = True

    return batch_writer_successful


@AWS_SERVICE.active
async def batch_get_items_from_dynamo(
        table_name: str,
        region: str,
        items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get batch of items from dynamo.

    Args:
        table_name (str): dynamo table name
        region (str): AWS region
        items (List[Dict[str, Any]]): list of items to fetch from dynamo table

    Returns:
        Dict[str, Any]: response of operation
    """

    response = await DYNAMO[region]['resource']['obj'].batch_get_item(RequestItems={table_name: {'Keys': items}})

    return response
