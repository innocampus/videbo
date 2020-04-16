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
    domain = ""
    ramdisk_size = "1G"
    internal_api_secret = "secure"
    api_secret = "secure"
    become = ""

    if definition.user != "root":
        become = "--become"

    cmd_ret = os.system(f"export ANSIBLE_HOST_KEY_CHECKING=False;"
                        f" ansible-playbook {become} -i {definition.user}@{host}, ansible/init_content_node.yml"
                        f"--extra-vars \""
                        f"domain={domain} "
                        f"ramdisk_size={ramdisk_size} "
                        f"internal_api_secret={internal_api_secret} "
                        f"api_secret={api_secret} "
                        f"max_clients={definition.max_clients} "
                        f"\"")
    if cmd_ret != 0:
        raise Exception(f"ansible failed for node: {host}")
