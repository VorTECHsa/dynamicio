## 1. Local interpreter Setup

## 1.1. `venv` setup (for MacOS and Linux)
1. Install `pyenv` by following [these instructions](https://github.com/pyenv/pyenv) and the `pyenv-virtualenv`
([instructions](https://github.com/pyenv/pyenv-virtualenv)) plugin. Once you do this then run:
```shell
>> pyenv install
```
This should install the python version contained in the `.python-version` file, on your machine. 

Then run: 
```shell
>> make dev-env-setup
```
This should automatically generate a `dynamicio`(for `.python-version`) virtual environment `venv` in the repo's root directory with all necessary requirements. 
It will also install and setup `pre-commit` hooks for the repo.

Activate with:
```shell script
>> source venv/bin/activate
```
To deactivate the environment, do:
```shell script
>> deactivate
```

### 2.1. PyCharm (venv):
1. Run `make dev-env-setup`, which will set up a python dev venv for you in your root directory.
2. Go to `Preferences > Search for `Python Interpreter`.
3. Next to the drop-down list select the three dots sign and choose `Add...`
4. Select `Existing environment option` and provide the path to the venvs python file, which should be something like:
```shell
>> /Users/{your-pc-name}/path/to/dynamicio/venv/bin/python
```

## 2. Committing
Based on how the repository has been configured, when you stage any new files to be committed, they will first be
inspected by `isort`, `black`, `flake8`, `yamllint` and `pylint` by means of `pre-commit`. If errors are found by 
`black`, you can simply re-add updated files to staging and try to commit again. Else, you will need to make some 
changes, stage the updated files and then try to commit again.

Hooks have not been configured for docs generation and for running tests, as docs can skew the purpose of a commit
and should be handled independently, while tests are not always part of a commit either. You can manually run these 
checks, you by using the below make instructions:

```shell
>> make check-linting
>> make run-tests
>> make generate-docs (please, always update docs with every release)
```

## 3. Tagging
To tag a release candidate, you need to run the following command from your feature-branch:
```shell
>> make tag-release-candidate
```
and follow the instructions.

This script will:
1. Ask you to provide a tag number after informing you of the latest tag;
2. Create a new tag adding a changelog-message based on the semantic commits in the branch that are ahead of master, and;
3. Push your tag to be built into an official release.

Once you have finalised and merged your feature-branch into master, you can then run the following command to tag a new release:
```shell
>> make tag-new-release
```

This script will:
1. Ask you to provide a tag number after informing you of the latest tag;
2. Ask you to provide the name of the local branch you used to work on the new features you will be brining in;
3. Create a new tag adding a changelog-message based on the semantic commits in the branch that are ahead of master, and;
4. Push your tag to be built into an official release.

