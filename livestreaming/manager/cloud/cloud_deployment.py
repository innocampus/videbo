import os
from .status import DeploymentStatus


def init_node(node):
    if node.node_type == "content":
        init_content_node(node.ip)
        # TODO run tests on newly initialized node
        node.deployment_status = DeploymentStatus.OPERATIONAL


# TODO: Error handling
def init_content_node(ip):
    os.system(f"export ANSIBLE_HOST_KEY_CHECKING=False; ansible-playbook -i root@{ip}, ansible/init_content_node.yml")
