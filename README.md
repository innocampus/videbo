# Videbo

## Installing

1. Create a `config.ini` as needed. You can use `config.example.ini` as reference.
2. Setup your virtual environment:
    ```shell
    $ python -m venv .venv
    $ . venv/bin/activate
    (.venv) $ pip install -r requirements/full.txt
    ```
    If you don't need monitoring capabilities, install from `requirements/common.txt` instead.

## Running

To get general usage instructions, you can use the `-h` flag:
```shell
(.venv) $ python -m videbo -h
```

### Storage

The storage node is the central part of the application. It encompasses the main video hosting API, format checking, monitoring capabilities (optional), and distribution logic (optional).

You launch a storage node like this:
```shell
(.venv) $ python -m videbo storage
```

### Distributor

Any reasonable number of distributor nodes can be connected to one storage node. Depending on the overall configuration and load, requests for video streams can be redirected from storage to distributor.

You launch a distributor node like this:
```shell
(.venv) $ python -m videbo distributor
```

### CLI

The built-in CLI can be used for communication and even on-line interaction with a running storage node. It is currently possible to query the current node status, find orphaned files, as well as disabling/enabling distributor nodes on the fly.

To get more usage instructions and help on the various commands, simply use:
```shell
(.venv) $ python -m videbo cli -h
```

## Testing

All tests are located in the `tests` directory. 

### Running unit tests

The file/directory structure beneath it largely mirrors that of the `videbo` directory, with a test module for nearly every source module. As per convention, unit tests are located in modules with the prefix `test_` in the name. 

E.g. unit tests for the module `videbo.storage.util` are placed in the module `tests.storage.test_util`.

Thus, running `unittest discover` from the standard library should find and run all unit test cases.

Since all unit tests are done with the standard library tools, no other packages need to be installed, and individual tests can be run via the usual `unittest` interface. (see the [official documentation](https://docs.python.org/3/library/unittest.html#command-line-interface) for more)

### Coverage

To view code coverage, the recommended tool is `coverage` which can be installed alongside the rest of the requirements by simply running:
 ```shell
 (.venv) $ pip install -r requirements/dev.txt
 ```
It's documentation can be found [here](https://coverage.readthedocs.io/en/stable/).

For convenience the shell script `coverage.sh` can be executed. This will run unit test discovery through `coverage` and display a report in the terminal. NOTE: The script will delete the `.coverage` file, if one exists before running the tests and creating a new one.

### Integration tests

There are a couple of more involved and integrated test cases that are also part of the test suite. Those that rely on an actual video testfile (to upload, download, delete, etc.) will be skipped automatically if no video file is found under the path set by `test_video_file_path`.

Note that interrupting an integration test may leave undesired files lying around in the storage directories that need to be cleaned up manually.