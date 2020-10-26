'''Generic async AWS functions for Secrets Manager.'''

import base64

from aioradio.aws.utils import AwsServiceManager

AWS_SERVICE = AwsServiceManager(service='secretsmanager', regions=['us-east-1', 'us-east-2'])
SECRETS = AWS_SERVICE.service_dict


@AWS_SERVICE.active
async def get_secret(secret_name: str, region: str) -> str:
    '''Get secret from AWS Secrets Manager.'''

    secret = ''
    response = await SECRETS[region]['client']['obj'].get_secret_value(SecretId=secret_name)
    if 'SecretString' in response:
        secret = response['SecretString']
    else:
        secret = base64.b64decode(response['SecretBinary'])

    return secret
