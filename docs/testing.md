# Testing

Tests live in `aioradio/tests/`, with shared fixtures in `conftest.py` at the repository root. They run with `pytest` and `pytest-asyncio`.

## Running tests

```bash
make test
```

This selects the repository's AWS profile and runs `pytest -vss --cov=aioradio --cov-config=.coveragerc --cov-report=html --cov-fail-under=40`. Coverage below 40% fails the run. `.coveragerc` excludes `aioradio/tests/` from coverage.

To run one file:

```bash
pytest -vss aioradio/tests/s3_test.py
```

Pass `--github=true` to skip the tests that use the `cache` fixture, a `fakeredis` instance used by `redis_test.py`.

## AWS mocking

`aioradio/aws/moto_server.py` defines `MotoService`, which runs a moto server in a background thread on `127.0.0.1`. Each service gets a fixed port:

| Service | Port |
|---|---|
| S3 | 5001 |
| SQS | 5002 |
| DynamoDB | 5004 |

The `s3_client`, `sqs_client`, `dynamodb_client`, and `dynamodb_resource` fixtures create clients against the moto endpoint. Each fixture swaps that client into the module-level `S3`, `SQS`, or `DYNAMO` dict for the duration of the test module, then restores the real client. Library functions under test therefore call moto without code changes.

`MotoService` is ref-counted, so one server per service runs per process. If PyCharm hosts the test run (`PYCHARM_HOSTED=1`), the connect timeout rises from 10 to 90 seconds.

## Skipped tests

These tests are hard-coded to skip because they need credentials or have side effects:

| File | Skipped tests | Reason |
|---|---|---|
| `file_ingestion_test.py` | Mandrill email, SMB/FTP read and write, `async_db_wrapper` | Sends real email, needs file-share and Secrets Manager access |
| `file_ingestion_test.py` | `async_wrapper` | Runs only for its designated local test user |
| `jira_test.py` | All three Jira tests | Creates real Jira tickets |
| `pyodbc_test.py` | Query test | Needs real database credentials |
| `psycopg2_test.py` | Connection test | Needs real database credentials |

The integration tests are intentionally skipped by default. Run them only in an approved test environment with suitable credentials and permission for their external effects.

## Linting

```bash
make lint
make pre-commit
```

`make lint` runs `pylint` with `.pylintrc` over the package, AWS modules, and tests, with the similarity check disabled. `make pre-commit` runs the hooks in `.pre-commit-config.yaml`, including `docformatter`, `requirements-txt-fixer`, and `name-tests-test`.
