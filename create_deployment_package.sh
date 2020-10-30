#!/bin/bash

tar -czvf videbo_deploy.tar.gz \
      --exclude="*/__pycache__" \
      ./videbo ./requirements.txt
