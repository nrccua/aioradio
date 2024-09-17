"""Python utility for NRCCUA common generic functions to reuse across
projects."""

from setuptools import setup

with open('README.md', 'r', encoding='utf8') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.21.0',
    description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nrccua/aioradio',
    author='Encoura DS Team',
    author_email='tim.reichard@encoura.org',
    license="MIT",
    packages=[
        'aioradio',
        'aioradio/aws',
    ],
    install_requires=[
        'cython>=0.29.33',
        'aioboto3==13.1.1',
        'aiojobs>=1.0.0',
        'backoff>=2.1.2',
        'botocore==1.34.131',
        'boto3==1.34.131',
        'ddtrace>=0.60.1',
        'faust-cchardet>=2.1.18',
        'fakeredis>=2.20.0',
        'grpcio==1.62.2',
        'grpcio-status==1.62.2',
        'haversine>=2.8.0',
        'httpx>=0.23.0',
        'mandrill>=1.0.60',
        'mlflow>=2.10.2',
        'numpy==1.26.4',
        'openpyxl==3.0.10',
        'orjson>=3.6.8',
        'pandas>=1.3.5',
        'pkginfo==1.10.0',
        'polars>=0.19.12',
        'protobuf==4.25.4',
        'pyarrow>=13.0.0',
        'pysmb>=1.2.7',
        'python-json-logger>=2.0.2',
        'redis>=5.0.1'
    ],
    include_package_data=True,
    tests_require=[
        'flask==3.0.3',
        'flask-cors>=4.0.1',
        'moto==4.2.14',
        'pre-commit>=2.15.0',
        'pylint>=2.13.8',
        'pytest>=7.0.1',
        'pytest-asyncio>=0.15.1',
        'pytest-cov>=3.0.0',
        'typing_extensions>=4.10.0',
        'werkzeug==3.0.4'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
