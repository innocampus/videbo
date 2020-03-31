from typing import List
from hcloud import Client
from hcloud import APIException
from hcloud.actions.domain import ActionFailedException, ActionTimeoutException
from hcloud.images.domain import Image
from hcloud.server_types.domain import ServerType
from hcloud.ssh_keys.domain import SSHKey
from hcloud.locations.domain import Location
import pickle
from random import choice
import secrets
from .status import DeploymentStatus, VmStatus
from .node import Node
from enum import Enum


class PlatformType(Enum):
    static = 0  # nodes that have been created manually and can't be created or destroyed using an API
    cloud = 1
    unknown = 2


class CloudAPI:
    def __init__(self, manager_settings):
        self.name = ""
        self.manager_settings = manager_settings

    def get_all_nodes(self) -> List[Node]:
        return []

    def create_node(self, name, node_type) -> Node:
        pass

    def update_vm_state(self, node):
        pass

    def delete_node(self, name):
        pass


class CombinedCloudAPI(CloudAPI):
    def __init__(self, manager_settings):
        super().__init__(manager_settings)
        self.providers = ""
        self.provider_apis = {}
        self.random_words = {}

        self.init_cloud_apis()

    def init_cloud_apis(self):
        self.providers = list(map(str.strip, self.manager_settings.cloud_providers.split(',')))

        for provider in self.providers:
            if provider == "hetzner":
                self.provider_apis["hetzner"] = HetznerAPI(self.manager_settings)

        with open('livestreaming/manager/random_word_database.pickle', "rb") as f:
            self.random_words = pickle.load(f)

    def get_all_nodes(self) -> List[Node]:
        nodes = []
        for provider in self.providers:
            nodes += self.provider_apis[provider].get_all_nodes()
        return nodes

    def create_node(self, name=False, node_type=False) -> Node:
        provider = self.__pick_provider_for_node_creation(node_type)
        name = self.__pick_node_name()
        return self.provider_apis[provider].create_node(name, node_type)

    def update_vm_state(self, node):
        provider = node.provider
        self.provider_apis[provider].update_vm_state(node)

    def delete_node(self, node):
        provider = node.provider
        return self.provider_apis[provider].delete_node(node.name)

    def __pick_provider_for_node_creation(self, node_type=False):
        # TODO: implement logic to pick provider based on utilisation
        return self.providers[0]

    def __pick_node_name(self):
        hash = secrets.token_hex(4)
        random_name = f"{choice(self.random_words['adj'])}-{choice(self.random_words['nouns'])}-{hash}"
        return random_name


class HetznerAPI(CloudAPI):
    def __init__(self, manager_settings):
        super().__init__(manager_settings)
        self.token = self.manager_settings.hetzner_api_token
        self.provider = "hetzner"
        self.client = Client(token=self.token)

    def get_all_locations(self):
        return self.client.locations.get_all()

    def get_all_nodes(self) -> List[Node]:
        try:
            servers = self.client.servers.get_all()
            nodes = []
            for s in servers:
                nodes.append(Node(s.name, "unknown", PlatformType.unknown, "unknown", self.__str_to_vm_status(s.status),
                                  s.public_net.ipv4.ip, s.id, self.provider))
            return nodes
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

    def create_node(self, name, node_type):
        try:
            # TODO: pick specs based on node type
            response = self.client.servers.create(name=name,
                                                  server_type=ServerType(self.__pick_server_type()),
                                                  image=Image(name=self.__pick_image()),
                                                  location=Location(name=self.__pick_location()),
                                                  ssh_keys=[SSHKey(name=self.__pick_ssh_key())])
            server = response.server
            return Node(server.name, node_type, PlatformType.cloud, DeploymentStatus.CREATED, server.public_net.ipv4.ip,
                        vm_status=self.__str_to_vm_status(server.status), vm_id=server.id, provider=self.provider)
        except APIException as e:  # handle exceptions
            print(e)
        except ActionFailedException as e:
            print(e)
        except ActionTimeoutException as e:
            print(e)

    def delete_node(self, name):
        try:
            server = self.client.servers.get_by_name(name)
            server.delete()
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

    @staticmethod
    def __str_to_vm_status(s_status):
        if s_status == "running":
            status = VmStatus.RUNNING
        elif s_status == "initializing":
            status = VmStatus.INIT
        else:
            status = VmStatus.ERROR
        return status

    def __pick_server_type(self):
        return self.manager_settings.hetzner_server_type

    def __pick_image(self):
        return self.manager_settings.hetzner_image

    def __pick_location(self):
        return self.manager_settings.hetzner_location

    def __pick_ssh_key(self):
        return self.manager_settings.hetzner_ssh_key

    def update_vm_state(self, node):
        try:
            server = self.client.servers.get_by_name(node.name)
            node.vm_status = self.__str_to_vm_status(server.status)
        except Exception as e:
            print(e)
            node.vm_status = VmStatus.ERROR
