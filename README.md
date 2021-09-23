# Videbo

## Install

1. Copy `config.example.ini` to `config.ini` and adjust all settings.
2. Setup venv:
```
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

## Execute

```
. venv/bin/activate
python -m videbo [mode]
```

## Testing

Tests are located in the `tests` directory. 

### Running unit tests

The file/directory structure beneath it largely mirrors that of the `videbo` directory, with a test module for nearly every source module. As per convention, unit tests are located in modules with the prefix `test_` in the name. 

E.g. unit tests for the module `videbo.storage.api.routes` are placed in the module `tests.storage.api.test_routes`.

Thus, running `unittest discover` from the standard library should find and run all unit test cases.

Since all unit tests are done with the standard library tools, no other packages need to be installed, and individual tests can be run via the usual `unittest` interface.

### Coverage

To view code coverage, the recommended tool is `coverage` which can be installed with `pip install coverage`; it's documentation can be found [here](https://coverage.readthedocs.io/en/stable/).

For convenience the shell script `coverage.sh` can be executed. This will run unit test discovery through `coverage` and display a report in the terminal. NOTE: The script will delete the `.coverage` file, if one exists before running the tests and creating a new one.
