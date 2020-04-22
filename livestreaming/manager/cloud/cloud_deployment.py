import asyncio
from typing import Dict
from livestreaming import settings
from livestreaming.manager import logger, manager_settings
from livestreaming.manager.node_types import NodeTypeBase, DistributorNode
from .definitions import InstanceDefinition, ContentInstanceDefinition, DistributorInstanceDefinition
from .status import DeploymentStatus
from .server import DynamicServer


async def init_node(node: NodeTypeBase):
    server = node.server
    if server is None or not isinstance(server, DynamicServer):
        logger.error(f"init_node can only be called with a DynamicServer ({node.base_url})")
        return

    await remove_ssh_key(server.host)
    await wait_ssh_port_open(server.host)
    await asyncio.sleep(5) # Just to be sure wait another seconds.
    await import_ssh_key(server.host)

    server.deployment_status = DeploymentStatus.INITIALIZING
    if isinstance(server.instance_definition, ContentInstanceDefinition):
        await init_content_node(server, node, server.instance_definition)
        server.deployment_status = DeploymentStatus.INITIALIZED
        # TODO run tests on newly initialized node
        server.deployment_status = DeploymentStatus.OPERATIONAL

    elif isinstance(server.instance_definition, DistributorInstanceDefinition) and \
            isinstance(node, DistributorNode):
        await init_distributor_node(server, node, server.instance_definition)
        server.deployment_status = DeploymentStatus.INITIALIZED
        # TODO run tests on newly initialized node
        server.deployment_status = DeploymentStatus.OPERATIONAL


async def wait_ssh_port_open(host: str):
    while True:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, 22), 2)
            writer.close()
            return
        except (asyncio.TimeoutError, ConnectionError):
            await asyncio.sleep(1)


async def init_content_node(server: DynamicServer, node: NodeTypeBase, definition: ContentInstanceDefinition):
    vars = {
        "domain": "",
        "ramdisk_size": "1G",
        "internal_api_secret": settings.general.internal_api_secret,
        "api_secret": settings.lms.api_secret,
        "max_clients": str(definition.max_clients),
    }

    await run_ansible("content", server.host, definition, vars)


async def init_distributor_node(server: DynamicServer, node: DistributorNode, definition: DistributorInstanceDefinition):
    vars = {
        "domain": "",
        "ramdisk_size": "1G",
        "internal_api_secret": settings.general.internal_api_secret,
        "api_secret": settings.lms.api_secret,
        "tx_max_rate_mbit": str(definition.tx_max_rate_mbit),
        "leave_free_space_mb": str(definition.leave_free_space_mb),
        "bound_to_storage_base_url": node.bound_to_storage_node_base_url,
    }

    await run_ansible("distributor", server.host, definition, vars)


async def run_ansible(node_type: str, host: str, definition: InstanceDefinition, vars: Dict[str, str]):
    become = ""
    if definition.user != "root":
        become = "--become"

    # Add common vars for all nodes
    vars["influx_url"] = manager_settings.influx_url
    vars["influx_database"] = manager_settings.influx_database
    vars["influx_username"] = manager_settings.influx_username
    vars["influx_password"] = manager_settings.influx_password

    shell_cmd = "export ANSIBLE_HOST_KEY_CHECKING=False;" \
                f" ansible-playbook {become} -i {definition.user}@{host}, ansible/init_{node_type}_node.yml " \
                f"--extra-vars \"" \

    for key, value in vars.items():
        if not isinstance(value, str):
            value = str(value)
            logger.warning(f"Run ansible: needed to convert value {value} to string, key {key}")
        shell_cmd += f"{key}={value} "

    shell_cmd += '"'

    logger.info(f"Start ansible for host {host}")
    process = await asyncio.create_subprocess_shell(shell_cmd, stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.STDOUT)

    output = ''
    try:
        while not process.stdout.at_eof():
            line = await process.stdout.readline()
            if settings.general.dev_mode:
                logger.info(line)
            output += line.decode()

        await process.wait()

        if process.returncode != 0:
            raise AnsibleError(f"ansible failed for node: {host}")
    except asyncio.CancelledError:
        if process.returncode is None:
            process.terminate()
        raise
    finally:
        logger.info(f"Ansible output for node: {host}\n\n" + output)


async def remove_ssh_key(host: str):
    shell_cmd = f"ssh-keygen -R {host}"
    process = await asyncio.create_subprocess_shell(shell_cmd, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
    await process.wait()
    # Ignore errors (e.g. when host not found)


async def import_ssh_key(host: str):
    shell_cmd = f"ssh-keyscan -H {host} >> ~/.ssh/known_hosts"
    process = await asyncio.create_subprocess_shell(shell_cmd, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
    await process.wait()


class AnsibleError(Exception):
    pass
