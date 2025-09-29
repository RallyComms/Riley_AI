.PHONY: install test lint ci precommit

install:
	python -m pip install -U pip
	pip install -r requirements.txt

lint:
	ruff check .
	black --check .

test:
	pytest -q --maxfail=1 --disable-warnings

ci: lint test

precommit:
	pre-commit install
