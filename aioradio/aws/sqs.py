"""Generic async AWS functions for SQS."""

# pylint: disable=dangerous-default-value
# pylint: disable=too-many-arguments

import logging
from typing import Any, Dict, List

from botocore.exceptions import ClientError

from aioradio.aws.utils import AwsServiceManager

LOG = logging.getLogger(__name__)
AWS_SERVICE = AwsServiceManager(service='sqs', regions=['us-east-1'])
SQS = AWS_SERVICE.service_dict


async def add_regions(regions: List[str]):
    """Add regions to SQS AWS service.

    Args:
        regions (List[str]): List of AWS regions
    """

    AWS_SERVICE.add_regions(regions)


@AWS_SERVICE.active
async def create_queue(queue: str, region: str, attributes: Dict[str, str], account_id: str='') -> Dict[str, Any]:
    """Create SQS queue in region defined.

    Args:
        queue (str): sqs queue
        region (str): AWS region
        attributes (Dict[str, str]): sqs queue attributes
        account_id (str, optional): AWS account ID

    Returns:
        Dict[str, str]: response of operation
    """

    if account_id:
        result = await SQS[region]['client']['obj'].create_queue(QueueName=queue, QueueOwnerAWSAccountId=account_id, Attributes=attributes)
    else:
        result = await SQS[region]['client']['obj'].create_queue(QueueName=queue, Attributes=attributes)
    return result


@AWS_SERVICE.active
async def get_messages(
        queue: str,
        region: str,
        account_id: str='',
        wait_time: int=20,
        max_messages: int=10,
        visibility_timeout: int=30,
        attribute_names: List[str]=[]) -> List[dict]:
    """Get up to 10 messages from an SQS queue.

    Args:
        queue (str): sqs queue
        region (str): AWS region
        account_id (str, optional): AWS account ID
        wait_time (int, optional): time to wait polling for messages. Defaults to 20.
        max_messages (int, optional): max messages polled. Defaults to 10.
        visibility_timeout (int, optional): timeout for when message will return to queue if not deleted. Defaults to 30.
        attribute_names (List[str], optional): list of attributes for which to retrieve information. Defaults to [].

    Returns:
        List[dict]: list of dicts where each dict contains the message information
    """

    messages = []
    if account_id:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue, QueueOwnerAWSAccountId=account_id)
    else:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
    queue_url = resp['QueueUrl']
    resp = await SQS[region]['client']['obj'].receive_message(
        QueueUrl=queue_url,
        WaitTimeSeconds=wait_time,
        MaxNumberOfMessages=max_messages,
        VisibilityTimeout=visibility_timeout,
        AttributeNames=attribute_names)
    if 'Messages' in resp:
        messages = resp['Messages']

    return messages


@AWS_SERVICE.active
async def send_messages(queue: str, region: str, entries: List[Dict[str, str]], account_id: str='') -> Dict[str, list]:
    """Send up to 10 messages to an SQS queue.

    Args:
        queue (str): sqs queue
        region (str): AWS region
        entries (List[Dict[str, str]]): List of dicts containing the keys: Id and MessageBody
        account_id (str, optional): AWS account ID

    Returns:
        Dict[str, list]: dict with two keys, either Successful or Failed
    """

    if account_id:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue, QueueOwnerAWSAccountId=account_id)
    else:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)

    queue_url = resp['QueueUrl']
    result = await SQS[region]['client']['obj'].send_message_batch(QueueUrl=queue_url, Entries=entries)

    return result


@AWS_SERVICE.active
async def delete_messages(queue: str, region: str, entries: List[Dict[str, str]], account_id: str='') -> Dict[str, list]:
    """Delete up to 10 messages from an SQS queue.

    Args:
        queue (str): sqs queue
        region (str): AWS region
        entries (List[Dict[str, str]]): List of dicts containing the keys: Id and ReceiptHandle
        account_id (str, optional): AWS account ID

    Returns:
        Dict[str, list]: dict with two keys, either Successful or Failed
    """

    if account_id:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue, QueueOwnerAWSAccountId=account_id)
    else:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
    queue_url = resp['QueueUrl']
    result = await SQS[region]['client']['obj'].delete_message_batch(QueueUrl=queue_url, Entries=entries)

    return result


@AWS_SERVICE.active
async def purge_messages(queue: str, region: str, account_id: str='') -> str:
    """Purge messages from queue in region defined.

    Args:
        queue (str): sqs queue
        region (str): AWS region
        account_id (str, optional): AWS account ID

    Returns:
        str: error message if any
    """

    error = ''
    try:
        if account_id:
            resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue, QueueOwnerAWSAccountId=account_id)
        else:
            resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
        queue_url = resp['QueueUrl']
        await SQS[region]['client']['obj'].purge_queue(QueueUrl=queue_url)
    except ClientError as err:
        if err.response['Error']['Code'] == 'AWS.SimpleQueueService.PurgeQueueInProgress':
            error = err.response['Error']['Message']
        else:
            raise

    return error
