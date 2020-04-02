import asyncio
import os
from .definitions import ContentInstanceDefinition
from .status import DeploymentStatus
from .server import DynamicServer


async def init_node(node: DynamicServer):
    await asyncio.sleep(30) # TODO
    node.deployment_status = DeploymentStatus.INITIALIZING
    if isinstance(node.instance_definition, ContentInstanceDefinition):
        await init_content_node(node.host, node.instance_definition)
        node.deployment_status = DeploymentStatus.INITIALIZED
        # TODO run tests on newly initialized node
        node.deployment_status = DeploymentStatus.OPERATIONAL


async def init_content_node(host: str, definition: ContentInstanceDefinition):
    # Tell new node the definition.max_clients setting somehow.
    cmd_ret = os.system(f"export ANSIBLE_HOST_KEY_CHECKING=False; ansible-playbook -i root@{host}, ansible/init_content_node.yml")
    if cmd_ret != 0:
        raise Exception(f"ansible failed for node: {host}")
