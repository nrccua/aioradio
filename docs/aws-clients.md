# AWS client lifecycle

The S3, SQS, and DynamoDB modules share long-lived AWS clients rather than opening a client per call. `AwsServiceManager` in `aioradio/aws/utils.py` manages these clients.

## Creation at import

Each module creates its manager at import time:

| Module | Manager | Regions | Objects |
|---|---|---|---|
| `aioradio.aws.s3` | `AwsServiceManager(service='s3')` | default region from the AWS config | aiobotocore client |
| `aioradio.aws.sqs` | `AwsServiceManager(service='sqs', regions=['us-east-1', 'us-east-2'])` | `us-east-1`, `us-east-2` | aiobotocore client per region |
| `aioradio.aws.dynamodb` | `AwsServiceManager(service='dynamodb', regions=['us-east-1', 'us-east-2'], module='aioboto3')` | `us-east-1`, `us-east-2` | aioboto3 client and resource per region |

The manager starts an `aiojobs` scheduler. If an event loop is already running, it schedules client creation as a task. Otherwise it runs the scheduler to completion on the current loop, or on a new loop if none exists. Clients are created with `verify=False`.

The module-level dicts `S3`, `SQS`, and `DYNAMO` hold the live objects. SQS and DynamoDB dicts are keyed by region.

## Adding regions

SQS and DynamoDB clients exist only for the regions listed above. To use another region, call the module's `add_regions` first:

```python
from aioradio.aws import sqs

await sqs.add_regions(['us-west-2'])
```

## Refresh cycle

Every 300 seconds (`sleep_interval`), each client is closed and recreated. The `@AWS_SERVICE.active` decorator on every public function tracks how many calls are in flight:

1. Before a call runs, the decorator waits until the client is not marked `busy`.
2. The call increments the `active` counter and decrements it when it finishes.
3. At refresh time the manager waits up to 300 seconds for `active` to reach zero, then marks the client `busy`, closes it, and opens a new one.

A call that starts during a refresh blocks until the new client is ready. Client creation retries with exponential backoff on `ConnectTimeoutError` for up to 120 seconds.

## Secrets Manager

`aioradio.aws.secrets.get_secret` does not use the manager. It creates a synchronous `boto3` client on each call, optionally with explicit credentials passed as `aws_creds`, and returns `SecretString` or the base64-decoded `SecretBinary`.
