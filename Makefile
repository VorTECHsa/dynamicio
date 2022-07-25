CODE_DIR = dynamicio
TESTS = tests
VENV_DIR = venv
VENV_VERSION = $(shell head -n 1 .python-version)
VENV_NAME =  $(shell head -n 1 .python-virtualenv)
VENV_BIN_PATH = ${VENV_DIR}/bin

dev-env-setup:
	@echo "Creating DEV virtual environment..."
	@pyenv local ${VENV_VERSION}
	@python -m venv ${VENV_DIR}
	@echo "Installing requirements..."
	@${VENV_BIN_PATH}/pip install --upgrade pip
	@${VENV_BIN_PATH}/pip install -r requirements.txt
	@${VENV_BIN_PATH}/pip install -r requirements-dev.txt
	@${VENV_BIN_PATH}/pip install -r requirements-docs.txt
	@${VENV_BIN_PATH}/pip install -r requirements-build.txt
	@echo "Installing pre-commit hook..."
	@${VENV_BIN_PATH}/pre-commit install
	@${VENV_BIN_PATH}/pre-commit install --hook-type commit-msg

check-linting:
	@python -m black ${CODE_DIR}
	@python -m flake8 --verbose ${CODE_DIR}
	@python -m pylint -v ${CODE_DIR}
	@python -m yamllint -v ${CODE_DIR}
	@python -m mypy ${CODE_DIR}

check-docstring:
	@${VENV_BIN_PATH}/pydocstyle -e --count $(file)

create-jupyter-kernel:
	@${VENV_BIN_PATH}/pip install ipykernel
	@${VENV_BIN_PATH}/ipython kernel install --user --name=${VIRTUALENV_NAME}

run-tests:
	@python -m pytest --cache-clear --cov=${CODE_DIR} ${TESTS}

run-unit-tests:
	@python -m pytest -v -m unit ${TESTS}

run-integration-tests:
	@python -m pytest -v -m integration ${TESTS}

update-test-coverage:
	@python -m pytest ${TESTS} --cov=${CODE_DIR} --cov-report=html --cov-report=xml
	@coverage-badge -o docs/coverage_report/coverage-badge.svg -f
	@mv coverage.xml htmlcov/coverage.xml

generate-docs:
	@python -m pdoc --force --html ${CODE_DIR} -o docs
	@mv docs/dynamicio/* docs
	@rm -rf docs/dynamicio

build-locally:
	@${VENV_BIN_PATH}/pip install --upgrade build
	@BUILD_VERSION=3.1.0-rc.2 ${VENV_BIN_PATH}/python setup.py sdist bdist_wheel # Add a <some_version>, e.g. 0.2.3

upload-package:
	@${VENV_BIN_PATH}/pip install --upgrade twine
	@${VENV_BIN_PATH}/twine check dist/*
	@${VENV_BIN_PATH}/twine upload dist/*

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