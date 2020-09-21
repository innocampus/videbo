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

## Install Manager

The following steps were tested on Linux Debian Buster.

1. Install dependencies: `apt install python3 python3-venv python3-pip git ansible`
2. Create a directory for videbo, e.g. `mkdir /opt/videbo`
3. You may want to change the owner of `/opt/videbo` to some unprivileged/non-root user and run the following steps
with that user.
4. Create a system user for videbo using root
`adduser --system --shell /bin/bash --no-create-home -home /opt/videbo/var --group videbo`
5. Create subdirs `mkdir /opt/videbo/server /opt/videbo/var`
6. Give videbo write permission to var directory by changing the owner (run as root)
`chown videbo:videbo /opt/videbo/var`
7. Clone the videbo repository into directory `/opt/videbo/server`
8. Create a python3 virtual environment using the user from step 3 `python3 -m venv /opt/videbo/venv`
9. Install videbo's python requirements
`/opt/videbo/venv/bin/pip install -r /opt/videbo/server/requirements-manager.txt`
10. Copy `/opt/videbo/server/config.example.ini` to `/opt/videbo/server/config.ini` and adjust all settings.
Set the db_file to `/opt/videbo/var/manager.db`.
11. Create an ssh key for the videbo user (run as videbo user!) `ssh-keygen -t rsa -b 4096 -N '' -q`
12. Upload your public key in `/opt/videbo/var/.ssh/id_rsa.pub` to your cloud provider and specify the ssh key name
in `/opt/videbo/server/config.ini`.
13. If your cloud provider is OVH, you need to get a consumer key, e.g. using
`/opt/videbo/venv/bin/python /opt/videbo/server/generate_ovh_consumer_key.py`
14. Install lego, e.g.
`mkdir lego-download; cd lego-download;
wget https://github.com/go-acme/lego/releases/download/v3.7.0/lego_v3.7.0_linux_amd64.tar.gz;
tar -xvf lego_v3.7.0_linux_amd64.tar.gz; mv lego /usr/local/bin/; cd ..; rm -rf lego-download`
15. Obtain a Let's encrypt wildcard certificate with lego.
16. Copy the `.lego` directory from your home to `/opt/videbo/server`.
17. Run `./create_deployment_package.sh` from within `/opt/videbo/server`
18. Create a systemd unit in `/etc/systemd/system/videbo-manager.service`:
```
[Unit]
Description=Videbo Manager node service
After=network.target

[Service]
WorkingDirectory=/opt/videbo/server
ExecStart=/opt/videbo/venv/bin/python -m videbo manager
User=videbo
Group=videbo
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```
Enable and start the unit `systemctl enable videbo-manager; systemctl start videbo-manager`

## Manager Usage

You may use the CLI tool `/opt/videbo/venv/bin/python -m videbo cli` within `/opt/videbo/server`
to view all nodes and to manually create/disable/enable/remove nodes.


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

### Comprehensive tests

In addition to regular unit tests, some more monolithic and comprehensive tests are included.

##### DNSManagerINWX

This class is defined in the module `videbo.manager.cloud.dns_api` and it's test case `DNSManagerINWXTestCase` in the corresponding test module has the test method `test_API_interaction`, which not only simulates, but performs actual interactions with the INWX API, creating, updating and eventually deleting DNS records.

This is only done, if both the `domain` parameter in the `general` section and the credentials in the `cloud-inwx` section of the config file are specified (and valid). Otherwise, this test is skipped.

The test is written in a way that should minimize the risk of any unwanted side effects, such as changing "real" pre-existing records or leaving unwanted test-records. To make sure the latter does not happen, do not interrupt this test's execution.

##### Storage routes tests

t.b.p.
