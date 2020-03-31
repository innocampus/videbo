import os
from .status import DeploymentStatus
from .node import NodeType
import time


def init_node(node):
    time.sleep(30)
    if node.node_type == NodeType.content:
        node.deployment_status = DeploymentStatus.INITIALIZING
        init_content_node(node.ip)
        node.deployment_status = DeploymentStatus.INITIALIZED
        # TODO run tests on newly initialized node
        node.deployment_status = DeploymentStatus.OPERATIONAL


def init_content_node(ip):
    cmd_ret = os.system(f"export ANSIBLE_HOST_KEY_CHECKING=False; ansible-playbook -i root@{ip}, ansible/init_content_node.yml")
    if cmd_ret != 0:
        raise Exception(f"ansible failed for node with ip: {ip}")
