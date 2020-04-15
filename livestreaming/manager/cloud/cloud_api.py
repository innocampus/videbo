from asyncio import get_event_loop, sleep
from typing import List, Dict, Optional, Tuple
from hcloud import Client
from hcloud import APIException
from hcloud.actions.domain import ActionFailedException, ActionTimeoutException
from hcloud.images.domain import Image
from hcloud.server_types.domain import ServerType
from hcloud.ssh_keys.domain import SSHKey
from hcloud.locations.domain import Location
import ovh
import json
from ipaddress import IPv4Address, IPv6Network, IPv6Address
import logging
import pickle
from random import choice
import secrets

from livestreaming.manager import manager_settings
from .definitions import CloudInstanceDefsController, InstanceDefinition, HetznerApiKeyDefinition, OvhApiKeyDefinition, \
    UnknownProviderError
from .status import DeploymentStatus, VmStatus
from .server import Server, DynamicServer


cloud_logger = logging.getLogger('livestreaming-cloud')


class CloudAPI:
    async def get_all_nodes(self) -> List[DynamicServer]:
        return []

    async def create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        pass

    async def update_vm_state(self, node: DynamicServer) -> None:
        pass

    async def delete_node(self, node: DynamicServer) -> None:
        pass

    async def validate_instance_definition(self, definition: InstanceDefinition) -> None:
        """Check that the location and server_type of the instance definition really exists.

        Raise InstanceValidationError on an error."""
        raise NotImplementedError()

    async def wait_node_running(self, server: DynamicServer):
        """Wait until the provider started the server."""
        while True:
            if DeploymentStatus.state_ok(server.deployment_status) and server.vm_status == VmStatus.RUNNING:
                return
            await sleep(3)
            await self.update_vm_state(server)


class CloudBlockingAPI(CloudAPI):
    """Interface for provider classes that can only be implemented with blocking calls."""
    async def get_all_nodes(self) -> List[DynamicServer]:
        return await get_event_loop().run_in_executor(None, self._get_all_nodes)

    async def create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        return await get_event_loop().run_in_executor(None, self._create_node, definition, name)

    async def update_vm_state(self, node: DynamicServer) -> None:
        status: VmStatus = await get_event_loop().run_in_executor(None, self._get_vm_state, node)
        node.vm_status = status

    async def delete_node(self, node: DynamicServer) -> None:
        await get_event_loop().run_in_executor(None, self._delete_node, node)
        node.vm_status = VmStatus.OFF
        node.deployment_status = DeploymentStatus.DESTROYED

    async def validate_instance_definition(self, definition: InstanceDefinition) -> None:
        return await get_event_loop().run_in_executor(None, self._validate_instance_definition, definition)

    def _get_all_nodes(self) -> List[DynamicServer]:
        raise NotImplementedError()

    def _create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        raise NotImplementedError()

    def _get_vm_state(self, node: DynamicServer) -> VmStatus:
        raise NotImplementedError()

    def _delete_node(self, name: DynamicServer) -> None:
        raise NotImplementedError()

    def _validate_instance_definition(self, definition: InstanceDefinition) -> None:
        raise NotImplementedError()


class CombinedCloudAPI(CloudAPI):
    def __init__(self, definitions: CloudInstanceDefsController):
        self.definitions = definitions
        self.provider_apis: Dict[str, CloudAPI] = {}  # map provider name to api
        self.random_words = {}

        self.init_cloud_apis()

    def init_cloud_apis(self):
        for _, definition in self.definitions.provider_definitions.items():
            if isinstance(definition, HetznerApiKeyDefinition):
                self.provider_apis["hetzner"] = HetznerAPI(definition)
            else:
                raise UnknownProviderError(str(definition))

        with open('livestreaming/manager/random_word_database.pickle', "rb") as f:
            self.random_words = pickle.load(f)

    async def get_all_nodes(self) -> List[DynamicServer]:
        nodes = []
        for _, provider in self.provider_apis.items():
            nodes += await provider.get_all_nodes()
        return nodes

    async def create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        provider = definition.provider
        if name is None:
            name = self._pick_node_name()
        return await self.provider_apis[provider].create_node(definition, name)

    async def update_vm_state(self, node: DynamicServer) -> None:
        provider = node.provider
        await self.provider_apis[provider].update_vm_state(node)

    async def delete_node(self, node: DynamicServer) -> None:
        provider = node.provider
        await self.provider_apis[provider].delete_node(node)

    def _pick_node_name(self):
        hash = secrets.token_hex(4)
        random_name = f"{manager_settings.dynamic_node_name_prefix}{choice(self.random_words['adj'])}-{choice(self.random_words['nouns'])}-{hash}"
        return random_name

    async def validate_instance_definition(self, definition: InstanceDefinition):
        """Check that the location and server_type of the instance definition really exists."""
        await self.provider_apis[definition.provider].validate_instance_definition(definition)


class HetznerAPI(CloudBlockingAPI):
    provider: str = "hetzner"
    image_name: str = "debian-10"

    def __init__(self, hetzner_def: HetznerApiKeyDefinition):
        self.token: str = hetzner_def.key
        self.ssh_key_name: str = hetzner_def.ssh_key_name
        self.client = Client(token=self.token)

    def get_all_locations(self):
        return self.client.locations.get_all()

    def _get_all_nodes(self) -> List[DynamicServer]:
        try:
            servers = self.client.servers.get_all()
            nodes = []
            for s in servers:
                ipv6 = next(IPv6Network(s.public_net.ipv6.ip).hosts())  # get first IPv6 address from the network
                nodes.append(DynamicServer(s.name, DeploymentStatus.UNKNOWN, IPv4Address(s.public_net.ipv4.ip), ipv6,
                                           self._str_to_vm_status(s.status), None, s.id, self.provider))
            return nodes
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

        return []

    def _create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        try:
            cloud_logger.info(f"Create a new node at Hetzner (server type {definition.server_type})")
            response = self.client.servers.create(name=name,
                                                  server_type=ServerType(definition.server_type),
                                                  image=Image(name=self.image_name),
                                                  location=Location(name=definition.location),
                                                  ssh_keys=[SSHKey(name=self.ssh_key_name)])
            server = response.server
            ipv6 = next(IPv6Network(server.public_net.ipv6.ip).hosts())  # get first IPv6 address from the network
            return DynamicServer(server.name, DeploymentStatus.CREATED, IPv4Address(server.public_net.ipv4.ip),
                                 ipv6, self._str_to_vm_status(server.status), definition, server.id, self.provider)
        except APIException as e:  # handle exceptions
            print(e)
        except ActionFailedException as e:
            print(e)
        except ActionTimeoutException as e:
            print(e)

        raise NodeCreateError()

    def _delete_node(self, node: DynamicServer) -> None:
        try:
            cloud_logger.info(f"Delete node at Hetzner ({node.name})")
            server = self.client.servers.get_by_name(node.name)
            server.delete()
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

    @staticmethod
    def _str_to_vm_status(s_status) -> VmStatus:
        if s_status == "running":
            status = VmStatus.RUNNING
        elif s_status == "initializing" or s_status == "starting":
            status = VmStatus.INIT
        else:
            status = VmStatus.ERROR
        return status

    def _get_vm_state(self, node: DynamicServer) -> VmStatus:
        try:
            server = self.client.servers.get_by_name(node.name)
            cloud_logger.debug(f"Got vm state from Hetzner ({node.name}) status: {server.status}")
            return self._str_to_vm_status(server.status)
        except Exception as e:
            return VmStatus.ERROR

    def _validate_instance_definition(self, definition: InstanceDefinition) -> None:
        raise NotImplementedError() # TODO


class OvhAPI(CloudBlockingAPI):
    provider: str = "ovh"
    region: str = "DE1"
    ostype: str = "linux"
    os: str = "Debian 10"
    flavor_type: str = "b2-7"

    def __init__(self, ovh_def: OvhApiKeyDefinition):
        self.client = ovh.Client(
                                    endpoint='ovh-eu',
                                    application_key=ovh_def.application_key,
                                    application_secret=ovh_def.application_secret,
                                    consumer_key=ovh_def.consumer_key
                                )
        self.service = ovh_def.service
        self.ssh_key = ovh_def.ssh_key_name

    def __get_ssh_key_id(self, key_name: str) -> str:
        keys = self.client.get(f"/cloud/project/{self.service}/sshkey")
        keys_filtered = list(filter(lambda x: x["name"] == key_name, keys))
        if not keys_filtered:
            raise Exception(f"OVH: ssh key with name {key_name} not found")
        return keys_filtered[0]["id"]

    def __get_image_id(self, flavor_type: str) -> str:
        images = self.client.get(f"/cloud/project/{self.service}/image",
                                 flavorType=flavor_type,
                                 osType=self.ostype,
                                 region=self.region
                                 )
        images_filtered = list(filter(lambda x: x["name"] == self.os, images))
        if not images_filtered:
            raise Exception(f"OVH: no image for flavor type {flavor_type}, os type {self.ostype}, region {self.region}")
        return images_filtered[0]["id"]

    def __get_flavor_id(self, flavor_type: str) -> str:
        flavor_list = self.client.get(f"/cloud/project/{self.service}/flavor", region=self.region)
        flavor_filtered = list(filter(lambda x: x["name"] == flavor_type and x["osType"] == self.ostype, flavor_list))
        if not flavor_filtered:
            raise Exception(f"OVH: flavor type {flavor_type} for os type {self.ostype} not found")
        return flavor_filtered[0]["id"]

    def __get_instance_id(self, name: str) -> str:
        instances = self.client.get(f"/cloud/project/{self.service}/instance")
        instances_filtered = list(filter(lambda x: x["name"] == name, instances))
        if not instances_filtered:
            raise Exception(f"OVH: instance with name {name} not found")
        return instances_filtered[0]["id"]

    def __get_instance(self, name: str) -> dict:
        instance_id = self.__get_instance_id(name)
        instance = self.client.get(f"/cloud/project/{self.service}/instance/{instance_id}")
        return instance

    def __get_instance_ip(self, name: str) -> Tuple[str, str]:
        instance = self.__get_instance(name)
        ips = instance["ipAddresses"]
        if not ips:
            return "", ""
        ipv4 = list(filter(lambda x: x["version"] == 4, ips))[0]["ip"]
        ipv6 = list(filter(lambda x: x["version"] == 6, ips))[0]["ip"]
        return ipv4, ipv6

    def _get_all_nodes(self) -> List[DynamicServer]:
        try:
            instances = self.client.get(f"/cloud/project/{self.service}/instance")
            nodes = []
            for instance in instances:
                ips = instance["ipAddresses"]
                if not ips:
                    ipv4, ipv6 = "", ""
                else:
                    ipv4 = list(filter(lambda x: x["version"] == 4, ips))[0]["ip"]
                    ipv6 = list(filter(lambda x: x["version"] == 6, ips))[0]["ip"]
                nodes.append(DynamicServer(instance["name"], DeploymentStatus.UNKNOWN, IPv4Address(ipv4),
                                           IPv6Address(ipv6), self._str_to_vm_status(instance["status"]),
                                           None, instance["id"], self.provider))
            return nodes
        except Exception as e:
            pass

        return []

    def _create_node(self, definition: InstanceDefinition, name: Optional[str] = None) -> DynamicServer:
        try:
            flavor_id = self.__get_flavor_id(definition.server_type)
            image_id = self.__get_image_id(definition.server_type)
            sshkey_id = self.__get_ssh_key_id(self.ssh_key)
            server = self.client.post(f"/cloud/project/{self.service}/instance",
                                      flavorId=flavor_id,
                                      name=name,
                                      region=self.region,
                                      imageId=image_id,
                                      sshKeyId=sshkey_id
                                      )
            server_name = server["name"]
            ipv4, ipv6 = self.__get_instance_ip(server_name)
            return DynamicServer(server_name, DeploymentStatus.CREATED, IPv4Address(ipv4),
                                 IPv6Address(ipv6), self._str_to_vm_status(server.status), definition, server.id,
                                 self.provider)
        except Exception as e:
            pass

        raise NodeCreateError()

    def _delete_node(self, node: DynamicServer) -> None:
        try:
            cloud_logger.info(f"Delete node at OVH ({node.name})")
            instance_id = self.__get_instance_id(node.name)
            self.client.delete(f"/cloud/project/{self.service}/instance/{instance_id}")
        except Exception as e:  # handle exceptions
            pass

    @staticmethod
    def _str_to_vm_status(s_status) -> VmStatus:
        """
        possible values:
                             "ACTIVE" "BUILDING" "DELETED" "DELETING" "ERROR" "HARD_REBOOT" "PASSWORD"
                             "PAUSED" "REBOOT" "REBUILD" "RESCUED" "RESIZED" "REVERT_RESIZE" "SOFT_DELETED"
                             "STOPPED" "SUSPENDED" "UNKNOWN" "VERIFY_RESIZE" "MIGRATING" "RESIZE" "BUILD"
                             "SHUTOFF" "RESCUE" "SHELVED" "SHELVED_OFFLOADED" "RESCUING" "UNRESCUING"
                             "SNAPSHOTTING" "RESUMING"
        """
        if s_status == "ACTIVE":
            status = VmStatus.RUNNING
        elif s_status == "BUILDING" or s_status == "BUILD":
            status = VmStatus.INIT
        else:
            status = VmStatus.ERROR
        return status

    def _get_vm_state(self, node: DynamicServer) -> VmStatus:
        try:
            instance = self.__get_instance(node.name)
            cloud_logger.debug(f"Got vm state from OVH ({node.name}) status: {instance['status']}")
            return self._str_to_vm_status(instance['status'])
        except Exception as e:
            return VmStatus.ERROR

    def _validate_instance_definition(self, definition: InstanceDefinition) -> None:
        raise NotImplementedError() # TODO


class InstanceValidationError(Exception):
    def __init__(self, invalid_server_type: Optional[str], invalid_location: Optional[str]):
        self.invalid_server_type = invalid_server_type
        self.invalid_location = invalid_location
        format = f"Invalid instance definition: "
        if invalid_server_type:
            format += f"invalid server type ({invalid_server_type}) "
        if invalid_location:
            format += f"invalid location ({invalid_location}) "
        super().__init__(format)


class NodeCreateError(Exception):
    pass