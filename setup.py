'''Python utility for NRCCUA common generic functions to reuse across projects.'''

from setuptools import setup

setup(name='aioradio',
    version='0.8.0',
    description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    long_description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    url='https://github.com/nrccua/aioradio',
    author='NRCCUA Architects',
    author_email='architecture@nrccua.org',
    packages=[
        'aioradio',
        'aioradio/aws',
    ],
    install_requires=[
        'aiobotocore',
        'aioboto3',
        'aiojobs',
        'aioredis',
        'boto3',
        'ddtrace',
        'fakeredis',
        'httpx',
        'python-json-logger',
        'mandrill',
        'pysmb',
        'orjson',
        'xlrd'
    ],
    include_package_data=True,
    tests_require=[
        'flask',
        'moto',
        'pre-commit',
        'pylint',
        'pytest',
        'pytest-asyncio',
        'pytest-cov'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
