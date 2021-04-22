
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '\.pytest_cache' -exec rm -rf {} +

install:
	. env/bin/activate; \
	pip install -r aioradio/requirements.txt

setup:
	. env/bin/activate; \
	python setup.py develop --no-deps
	rm -rf build dist aioradio.egg-info

lint:
	. env/bin/activate; \
	pylint aioradio/*.py aioradio/aws/*.py aioradio/tests/*.py --disable=similarities

test:
	. env/bin/activate; \
	pytest -vss --cov=aioradio  --cov-config=.coveragerc --cov-report=html --cov-fail-under=75

pre-commit:
	. env/bin/activate; \
	pre-commit run --all-files; \
	cd aioradio; \
	pre-commit run --all-files; \

twine:
	. env/bin/activate; \
	python setup.py sdist bdist_wheel
	twine upload dist/*
	make setup

all: install pre-commit lint test setup clean
