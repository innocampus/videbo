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
18. Create a systemd unit in `/etc/systemd/system/videbo-manager.service`
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
19. Enable and start the unit `systemctl enable videbo-manager; systemctl start videbo-manager`

## Manager Usage

You may use the CLI tool `/opt/videbo/venv/bin/python -m videbo cli` within `/opt/videbo/server`
to view all nodes and to manually create/disable/enable/remove nodes.
