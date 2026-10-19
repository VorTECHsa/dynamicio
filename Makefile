# OS & Architecture Info
OS := $(shell uname)
ARCH := $(shell arch)

# Project Config
CODE_DIR = dynamicio
TESTS = tests

# Mac-specific binaries (optional)
install-binary-packages-Darwin:
	@brew list jq > /dev/null || brew install jq
	@brew list xz > /dev/null || brew install xz
	@brew list openblas > /dev/null || brew install openblas
	@brew list hdf5 > /dev/null || brew install hdf5
	@brew list postgresql@14 > /dev/null || brew install postgresql@14

# Install uv if not present (https://docs.astral.sh/uv/getting-started/installation/)
install-uv:
	@command -v uv > /dev/null 2>&1 || { echo "uv not found. Install it: curl -LsSf https://astral.sh/uv/install.sh | sh  (or: brew install uv)"; exit 1; }

# Full dev setup (OS deps + uv environment + pre-commit)
dev-env-setup: install-uv install-binary-packages-Darwin
	@echo "Creating the virtual environment (.venv) from uv.lock..."
	@uv sync --group dev --group docs
	@echo "Setting up pre-commit hooks..."
	@uv run pre-commit install
	@uv run pre-commit install --hook-type commit-msg

# Re-resolve dependencies and refresh uv.lock
update-lock:
	@uv lock --upgrade

# Checks for local development
check-linting:
	@uv run black ${CODE_DIR}
	@uv run flake8 --verbose ${CODE_DIR}
	@uv run pylint -v ${CODE_DIR}
	@uv run yamllint -v ${CODE_DIR}

check-docstring:
	@uv run pydocstyle -e --count $(file)

create-jupyter-kernel:
	@uv run python -m ipykernel install --user --name=dynamicio

# Tests
run-tests:
	@uv run pytest --cache-clear --cov=${CODE_DIR} ${TESTS}
	@uv run pytest --cache-clear --cov=demo/src demo/tests

run-unit-tests:
	@uv run pytest -v -m unit ${TESTS}

run-integration-tests:
	@uv run pytest -v -m integration ${TESTS}

check-test-coverage:
	@uv run pytest -vv --cov=$(CODE_DIR) --cov-report=term-missing

generate-docs:
	@uv run --group docs mkdocs build --strict

serve-docs:
	@uv run --group docs mkdocs serve

build-locally:
	@uv build

upload-package:
	@dist_dir=$$(mktemp -d) || exit 1; \
	trap 'rm -rf "$$dist_dir"' 0; \
	uv build --out-dir "$$dist_dir" && uv publish "$$dist_dir"/*

# Git tagging

# Create a release candidate tag using the format vX.Y.Z-rc.N and push it
# Changelog is auto-generated from commits between master and current branch
tag-release-candidate:
	@echo "The latest tag is:'$(shell git tag | sort -V | tail -1)'." \
	&& echo "Please, provide a new tag (format vX.Y.Z-rc.N):" \
	&& read -p "> " tag \
	&& echo "$$tag\nChangelog:\n" >> CHANGELOG.txt \
	&& git log master..$(git branch --show-current) --pretty=%B >> CHANGELOG.txt \
	&& cat CHANGELOG.txt \
	&& git tag -a -F CHANGELOG.txt $$tag \
	&& rm CHANGELOG.txt \
	&& git push origin $$tag

# Create a stable release tag using the format vX.Y.Z and push it
# Changelog is generated from commits between master and a given local branch
tag-new-release:
	@git pull \
	&& git pull --rebase origin master \
	&& echo "The latest tag is:'$(shell git tag | sort -V | tail -1)'." \
	&& echo "Please, provide a new tag (format vX.Y.Z):" \
	&& read -p "> " tag \
	&& echo "Please, provide the name of the **local** branch you worked on for this new tag:" \
	&& read -p "> " branch \
	&& echo "$$tag\nChangelog:\n" >> CHANGELOG.txt \
	&& git log master..$$branch --pretty=%B >> CHANGELOG.txt \
	&& cat CHANGELOG.txt \
	&& git tag -a -F CHANGELOG.txt $$tag \
	&& rm CHANGELOG.txt \
	&& git push origin $$tag
