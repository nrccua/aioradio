'''Generic async AWS functions for SQS.'''

# pylint: disable=dangerous-default-value
# pylint: disable=too-many-arguments

import logging
from typing import Dict
from typing import List

from botocore.exceptions import ClientError

from aioradio.aws.utils import AwsServiceManager

LOG = logging.getLogger(__name__)
AWS_SERVICE = AwsServiceManager(service='sqs', regions=['us-east-1', 'us-east-2'])
SQS = AWS_SERVICE.service_dict


@AWS_SERVICE.active
async def create_queue(queue: str, region: str, attributes: Dict[str, str]) -> Dict[str, str]:
    '''Create SQS queue in region defined.'''

    return await SQS[region]['client']['obj'].create_queue(QueueName=queue, Attributes=attributes)


@AWS_SERVICE.active
async def get_messages(
        queue: str,
        region: str,
        wait_time: int=20,
        max_messages: int=10,
        visibility_timeout: int=30,
        attribute_names: List[str]=[]) -> List[dict]:
    """
    Get up to 10 messages from an SQS queue. Returns a list of dicts where each dict contains
    the message information, Here is an example of a message produce from an s3 -> sqs event:
    {
        'MessageId': '0050daf1-313b-4a5c-a4e7-8e5596085fa8',
        'ReceiptHandle': '<very-long-string>',
        'MD5OfBody': 'ec3212bbe0cf0239ba54eefd206338ef',
        'Body': '{
            "Records": [{
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-2",
                "eventTime": "2020-07-07T19:02:45.192Z",
                "eventName": "ObjectCreated:CompleteMultipartUpload",
                "userIdentity": {"principalId":"AWS:AIDATIJBHOZJSHFN3H2KY"},
                "requestParameters": {"sourceIPAddress":"52.249.199.100"},
                "responseElements": {"x-amz-request-id":"625BA0F414478E41",
                "x-amz-id-2": "<very-long-string>"},
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "tf-s3-queue-20191002201742888700000006",
                    "bucket": {
                        "name": "nrccua-datalab-efi-input-sandbox.us-east-2",
                        "ownerIdentity": {"principalId":"A1MQ0EIGU3DVVT"},
                        "arn": "arn:aws:s3:::nrccua-datalab-efi-input-sandbox.us-east-2"
                    },
                    "object": {
                        "key": "XXXXXX/hello_world.txt",
                        "size":23, "eTag":"bccd05d8b202eba5e812bcf501c1682a-1",
                        "versionId": "ZLMCPDFMu6W865WWyfiVsaWZU8pJRUb3",
                        "sequencer":"005F04C6DA2DF81A97"
                    }
                }
            }]
        }'
    }
    """

    messages = []
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
async def send_messages(
        queue: str,
        region: str,
        entries: List[Dict[str, str]]) -> Dict[str, list]:
    '''
    Send up to 10 messages to an SQS queue. Each dict in entries must have
    the keys: Id and MessageBody populated. The returned data is a dict with two keys, either
    Successful or Failed, for example:
    {
        'Successful': [
            {
                'Id': 'string',
                'MessageId': 'string',
                'MD5OfMessageBody': 'string',
                'MD5OfMessageAttributes': 'string',
                'MD5OfMessageSystemAttributes': 'string',
                'SequenceNumber': 'string'
            },
        ],
        'Failed': [
            {
                'Id': 'string',
                'SenderFault': True|False,
                'Code': 'string',
                'Message': 'string'
            },
        ]
    }
    '''

    resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
    queue_url = resp['QueueUrl']
    result = await SQS[region]['client']['obj'].send_message_batch(QueueUrl=queue_url, Entries=entries)

    return result


@AWS_SERVICE.active
async def delete_messages(
        queue: str,
        region: str,
        entries: List[Dict[str, str]]) -> Dict[str, list]:
    '''
    Delete up to 10 messages from an SQS queue. Each dict in entries must have
    the keys: Id and ReceiptHandle populated. The returned data is a dict with two keys, either
    Successful or Failed, for example:
    {
        'Successful': [
            {
                'Id': 'string'
            },
        ],
        'Failed': [
            {
                'Id': 'string',
                'SenderFault': True|False,
                'Code': 'string',
                'Message': 'string'
            },
        ]
    }
    '''

    resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
    queue_url = resp['QueueUrl']
    result = await SQS[region]['client']['obj'].delete_message_batch(QueueUrl=queue_url, Entries=entries)

    return result


@AWS_SERVICE.active
async def purge_messages(queue: str, region: str) -> str:
    '''Purge messages from queue in region defined.'''

    error = ''
    try:
        resp = await SQS[region]['client']['obj'].get_queue_url(QueueName=queue)
        queue_url = resp['QueueUrl']
        await SQS[region]['client']['obj'].purge_queue(QueueUrl=queue_url)
    except ClientError as err:
        if err.response['Error']['Code'] == 'AWS.SimpleQueueService.PurgeQueueInProgress':
            error = err.response['Error']['Message']
        else:
            raise

    return error
