"""Python utility for NRCCUA common generic functions to reuse across
projects."""

from setuptools import setup

with open('README.md', 'r') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.14.4',
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
        'aioboto3==9.2.0',
        'aiobotocore==1.3.3',
        'aiojobs==0.3.0',
        'boto3==1.17.106',
        'ddtrace==0.50.3',
        'fakeredis==1.5.2',
        'httpx==0.18.2',
        'mandrill==1.0.60',
        'orjson==3.6.0',
        'psycopg2-binary==2.9.1',
        'pysmb==1.2.7',
        'python-json-logger==2.0.2',
        'redis==3.5.3',
        'xlrd==2.0.1'
    ],
    include_package_data=True,
    tests_require=[
        'flask==2.0.1',
        'flask-cors==3.0.10',
        'moto==2.2.0',
        'pre-commit==2.13.0',
        'pylint==2.9.6',
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
