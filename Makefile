format-python:
	python -m ruff check --fix mlplatform/ tests/
	python -m ruff format mlplatform/ tests/

lint-python:
	python -m mypy mlplatform
	python -m ruff check mlplatform/ tests/
	python -m ruff format --check mlplatform/ tests/
