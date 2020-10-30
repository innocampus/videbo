Ansible
=======

This Ansible playbook installs the video distributor node on a Debian-based server.

It set ups the following directory structure:

* /opt/videbo/
    * server/ (owner videbo_deploy, 0755)
        * (python code)
    * var/acme-challenge/ (owner root, 0755)
    * venv/ (owner videbo_deploy, 0755)
    * videos/ (owner videbo, 0755)
