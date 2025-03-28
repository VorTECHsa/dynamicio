# OS & Architecture Info
OS := $(shell uname)
ARCH := $(shell arch)

# Project Config
CODE_DIR = dynamicio
TESTS = tests
POETRY_VERSION ?= 1.7.1
VENV_DIR = .venv
VENV_VERSION = $(shell head -n 1 .python-version)
VENV_BIN_PATH = ${VENV_DIR}/bin

# Mac-specific binaries (optional)
install-binary-packages-Darwin:
	@brew list jq > /dev/null || brew install jq
	@brew list xz > /dev/null || brew install xz
	@brew list openblas > /dev/null || brew install openblas
	@brew list hdf5 > /dev/null || brew install hdf5
	@brew list postgresql@14 > /dev/null || brew install postgresql@14

# Install Poetry if not present
install-poetry:
	@echo "# Ensure correct Python version"
	@pyenv local $(VENV_VERSION)
	@if ! pyenv which poetry > /dev/null 2>&1; then \
		echo "Installing poetry for Python version $$(cat .python-version)..."; \
		pyenv exec pip install --upgrade pip "poetry==$(POETRY_VERSION)" --index-url 'https://pypi.python.org/simple'; \
	else \
		echo "Poetry is already installed."; \
	fi

# Full dev setup (OS deps + poetry + pre-commit)
dev-env-setup: install-poetry install-binary-packages-Darwin
	@echo "Installing Poetry dependencies..."
	@pyenv exec poetry install --sync
	@echo "Setting up pre-commit hooks..."
	@pyenv exec poetry self add poetry-pre-commit-plugin
	@pyenv exec poetry run pre-commit install
	@pyenv exec poetry run pre-commit install --hook-type commit-msg

# Checks for local development
check-linting:
	poetry run black ${CODE_DIR}
	poetry run flake8 --verbose ${CODE_DIR}
	poetry run pylint -v ${CODE_DIR}
	poetry run yamllint -v ${CODE_DIR}

check-docstring:
	@pyenv exec poetry run pydocstyle -e --count $(file)

create-jupyter-kernel:
	@pyenv exec poetry run python -m ipykernel install --user --name=dynamicio

# Tests
run-tests:
	@pyenv exec poetry run pytest --cache-clear --cov=${CODE_DIR} ${TESTS}
	@pyenv exec poetry run pytest --cache-clear --cov=demo/src demo/tests

run-unit-tests:
	@pyenv exec poetry run pytest -v -m unit ${TESTS}

run-integration-tests:
	@pyenv exec poetry run pytest -v -m integration ${TESTS}

update-test-coverage:
	@pyenv exec poetry run pytest ${TESTS} --cov=${CODE_DIR} --cov-report=html --cov-report=xml
	@pyenv exec poetry run coverage-badge -o docs/coverage_report/coverage-badge.svg -f || echo "⚠️ coverage-badge failed (likely due to version compatibility)"
	@mv coverage.xml htmlcov/coverage.xml

generate-docs:
	@pyenv exec poetry run python -m pdoc --force --html ${CODE_DIR} -o docs
	@mv docs/dynamicio/* docs
	@rm -rf docs/dynamicio

build-locally:
	@pyenv exec poetry build

upload-package:
	@pyenv exec poetry publish --build

# Git tagging
tag-release-candidate:
	@echo "The latest tag is:'$(shell git tag | sort -V | tail -1)'." \
	&&echo "Please, provide a new tag (format vX.Y.Z-rc.X)):" \
	&&read -p "> " tag \
	&&echo "$$tag\nChangelog:\n" >> CHANGELOG.txt \
	&&git log master..$(git branch --show-current) --pretty=%B >> CHANGELOG.txt \
	&&cat CHANGELOG.txt \
	&&git tag -a -F CHANGELOG.txt $$tag \
	&&rm CHANGELOG.txt \
	&&git push origin $$tag

tag-new-release:
	@git pull \
	&&git pull --rebase origin master \
	&&echo "The latest tag is:'$(shell git tag | sort -V | tail -1)'." \
	&&echo "Please, provide a new tag (format vX.Y.Z)):" \
	&&read -p "> " tag \
	&&echo "Please, provide the name of the **local** branch you worked on for this new tag:" \
	&&read -p "> " branch \
	&&echo "$$tag\nChangelog:\n" >> CHANGELOG.txt \
	&&git log master..$$branch --pretty=%B >> CHANGELOG.txt \
	&&cat CHANGELOG.txt \
	&&git tag -a -F CHANGELOG.txt $$tag \
	&&rm CHANGELOG.txt \
	&&git push origin $$tag