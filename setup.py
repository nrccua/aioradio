"""Python utility for NRCCUA common generic functions to reuse across
projects."""

from setuptools import setup

with open('README.md', 'r') as fileobj:
    long_description = fileobj.read()

setup(name='aioradio',
    version='0.17.27',
    description='Generic asynchronous i/o python utilities for AWS services (SQS, S3, DynamoDB, Secrets Manager), Redis, MSSQL (pyodbc), JIRA and more',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nrccua/aioradio',
    author='NRCCUA Architects',
    author_email='tim.reichard@encoura.org',
    license="MIT",
    packages=[
        'aioradio',
        'aioradio/aws',
    ],
    install_requires=[
        'aioboto3==10.2.0',
        'aiojobs>=1.0.0',
        'backoff>=2.1.2',
        'botocore==1.27.59',
        'boto3==1.24.59',
        'cchardet>=2.1.7',
        'ddtrace>=0.60.1',
        'dominodatalab>=1.1.1',
        'fakeredis>=1.7.1',
        'httpx>=0.23.0',
        'mandrill>=1.0.60',
        'numpy>=1.19',
        'openpyxl>=3.0.10',
        'orjson>=3.6.8',
        'pandas>=1.3.5',
        'psycopg2-binary>=2.9.3',
        'pysmb>=1.2.7',
        'python-json-logger>=2.0.2',
        'redis==3.5.3'
    ],
    include_package_data=True,
    tests_require=[
        'flask==2.1.2',
        'flask-cors>=3.0.10',
        'moto==3.1.18',
        'pre-commit>=2.15.0',
        'pylint>=2.13.8',
        'pytest>=7.0.1',
        'pytest-asyncio>=0.15.1',
        'pytest-cov>=3.0.0',
        'werkzeug==2.1.2'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
