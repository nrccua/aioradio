'''Python utility for NRCCUA common generic functions to reuse across projects.'''

from setuptools import setup

with open('README.md', 'r') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.8.5',
    description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nrccua/aioradio',
    author='NRCCUA Architects',
    author_email='architecture@nrccua.org',
    license="MIT",
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
