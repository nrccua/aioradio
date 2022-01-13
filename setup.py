"""Python utility for NRCCUA common generic functions to reuse across
projects."""

from setuptools import setup

with open('README.md', 'r') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.16.2',
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
        'aioboto3>=9.2.2',
        'aiobotocore>=1.4.2',
        'aiojobs>=0.3.0',
        'boto3==1.17.106',
        'ddtrace>=0.57.0',
        'fakeredis>=1.7.0',
        'httpx>=0.19.0',
        'mandrill>=1.0.60',
        'numpy>=1.19',
        'orjson>=3.6.5',
        'psycopg2-binary==2.9.3',
        'pysmb>=1.2.7',
        'python-json-logger>=2.0.2',
        'redis==3.5.3',
        'xlrd==2.0.1'
    ],
    include_package_data=True,
    tests_require=[
        'flask>=2.0.2',
        'flask-cors>=3.0.10',
        'moto>=2.3.1',
        'pre-commit>=2.16.0',
        'pylint>=2.11.2',
        'pytest>=6.2.5',
        'pytest-asyncio>=0.16.0',
        'pytest-cov>=3.0.0'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
