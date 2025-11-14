.PHONY: help install test lint format clean docker-build docker-up docker-down generate-data run-pipeline

help:
	@echo "E-commerce Data Pipeline - Available Commands"
	@echo "=============================================="
	@echo "install          - Install project dependencies"
	@echo "test             - Run unit tests"
	@echo "test-coverage    - Run tests with coverage report"
	@echo "lint             - Run code linting"
	@echo "format           - Format code with black"
	@echo "clean            - Clean generated files and caches"
	@echo "docker-build     - Build Docker containers"
	@echo "docker-up        - Start Docker containers"
	@echo "docker-down      - Stop Docker containers"
	@echo "generate-data    - Generate sample data"
	@echo "run-pipeline     - Run the complete ETL pipeline"

install:
	pip install -r requirements.txt
	pip install -e .

test:
	pytest tests/unit -v

test-coverage:
	pytest tests/ --cov=src --cov-report=html --cov-report=term

lint:
	flake8 src/ tests/ --max-line-length=100 --exclude=__pycache__
	pylint src/ --max-line-length=100

format:
	black src/ tests/ --line-length=100

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d
	@echo "Airflow webserver available at http://localhost:8080"
	@echo "Username: airflow"
	@echo "Password: airflow"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

generate-data:
	python src/utils/data_generator.py

run-pipeline:
	@echo "Running extraction..."
	python -m src.extraction.data_extractor
	@echo "Running transformation..."
	python -m src.transformation.data_transformer
	@echo "Running dimensional modeling..."
	python -m src.loading.dimensional_model
	@echo "Pipeline completed!"

setup-dev:
	pip install -e ".[dev]"
	pre-commit install
