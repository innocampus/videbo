from typing import List
from hcloud import Client
from hcloud import APIException
from hcloud.actions.domain import ActionFailedException, ActionTimeoutException
from hcloud.images.domain import Image
from hcloud.server_types.domain import ServerType
from hcloud.ssh_keys.domain import SSHKey
from hcloud.locations.domain import Location


class Node:
    def __init__(self, name, status, ip, id):
        self.name = name
        self.status = status
        self.ip = ip
        self.id = id

    def __str__(self):
        return f"[Node name: {self.name}; status: {self.status}; ip: {self.ip}; id: {self.id}]"


class CloudAPI:
    def __init__(self, token):
        self.name = ""
        self.token = token

    def get_all_nodes(self) -> List[Node]:
        return []

    def create_node(self, name) -> Node:
        pass

    def delete_node(self, name):
        pass


class HetznerAPI(CloudAPI):
    def __init__(self, token):
        super().__init__(token)
        self.name = "hetzner"
        self.client = Client(token=self.token)

    def get_all_locations(self):
        return self.client.locations.get_all()

    def get_all_nodes(self) -> List[Node]:
        try:
            servers = self.client.servers.get_all()
            nodes = []
            for s in servers:
                nodes.append(Node(s.name, s.status, s.public_net.ipv4.ip, s.id))
            return nodes
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

    def create_node(self, name):
        try:
            response = self.client.servers.create(name=name,
                                                  server_type=ServerType("cx11"),
                                                  image=Image(name="debian-10"),
                                                  location=Location(name="nbg1"),  # allowed locations: fsn1, nbg1, hel1
                                                  ssh_keys=[SSHKey(name="streamingkey")])
            server = response.server
            return Node(server.name, server.status, server.public_net.ipv4.ip, server.id)
        except APIException:  # handle exceptions
            pass
        except ActionFailedException:
            pass
        except ActionTimeoutException:
            pass

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
