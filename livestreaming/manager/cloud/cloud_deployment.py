import asyncio
from livestreaming import settings
from livestreaming.manager import logger
from .definitions import ContentInstanceDefinition
from .status import DeploymentStatus
from .server import DynamicServer


async def init_node(node: DynamicServer):
    await wait_ssh_port_open(node.host)
    # Just to be sure wait another seconds.
    await asyncio.sleep(4)

    node.deployment_status = DeploymentStatus.INITIALIZING
    if isinstance(node.instance_definition, ContentInstanceDefinition):
        await init_content_node(node.host, node.instance_definition)
        node.deployment_status = DeploymentStatus.INITIALIZED
        # TODO run tests on newly initialized node
        node.deployment_status = DeploymentStatus.OPERATIONAL


async def wait_ssh_port_open(host: str):
    while True:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, 22), 2)
            writer.close()
            return
        except (asyncio.TimeoutError, ConnectionError):
            await asyncio.sleep(1)


async def init_content_node(host: str, definition: ContentInstanceDefinition):
    domain = ""
    ramdisk_size = "1G"
    internal_api_secret = settings.general.internal_api_secret
    api_secret = settings.lms.api_secret
    become = ""

    if definition.user != "root":
        become = "--become"

    shell_cmd = f"export ANSIBLE_HOST_KEY_CHECKING=False; " \
                f" ansible-playbook {become} -i {definition.user}@{host}, ansible/init_content_node.yml " \
                f"--extra-vars \"" \
                f"domain={domain} " \
                f"ramdisk_size={ramdisk_size} " \
                f"internal_api_secret={internal_api_secret} " \
                f"api_secret={api_secret} " \
                f"max_clients={definition.max_clients} " \
                f"\""

    logger.info(f"Start ansible for host {host}")
    process = await asyncio.create_subprocess_shell(shell_cmd, stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.STDOUT)

    output = ''
    try:
        while not process.stdout.at_eof():
            line = await process.stdout.readline()
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


class AnsibleError(Exception):
    pass
