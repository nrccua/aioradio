"""Python utility for NRCCUA common generic functions to reuse across
projects."""

from setuptools import setup

with open('README.md', 'r') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.13.14',
    description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nrccua/aioradio',
    author='NRCCUA Architects',
    license="MIT",
    packages=[
        'aioradio',
        'aioradio/aws',
    ],
    install_requires=[
        'aioboto3==8.3.0',
        'aiobotocore==1.3.3',
        'aiojobs==0.3.0',
        'boto3==1.18.3',
        'ddtrace==0.50.1',
        'fakeredis==1.5.2',
        'httpx==0.18.2',
        'mandrill==1.0.60',
        'orjson==3.6.0',
        'psycopg2-binary==2.9.1',
        'pysmb==1.2.7',
        'python-json-logger==2.0.1',
        'redis==3.5.3',
        'xlrd==2.0.1'
    ],
    include_package_data=True,
    tests_require=[
        'flask==2.0.1',
        'moto==1.13.16',
        'pre-commit==2.13.0',
        'pylint==2.9.5',
        'pytest==6.2.4',
        'pytest-asyncio==0.15.1',
        'pytest-cov==2.12.1'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
