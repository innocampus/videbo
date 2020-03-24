#!/bin/bash

mkdir /opt/livestreaming-server
tar -czvf /opt/livestreaming-server/livestream_deploy.tar.gz \
      ./ansible ./livestreaming ./config.ini ./requirements.txt