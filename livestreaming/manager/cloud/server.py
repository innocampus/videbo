from typing import Optional
from ipaddress import IPv4Address, IPv6Address
from .dns_api import DNSManager, DNSRecord
from .status import VmStatus, DeploymentStatus
from .definitions import InstanceDefinition


class Server:
    def __init__(self, name: str, host: str):
        self.name: str = name
        self.host: str = host # ip or domain

    def is_operational(self) -> bool:
        raise NotImplementedError()


class StaticServer(Server):
    def __repr__(self):
        return f"[StaticServer name: {self.name}; host: {self.host}] "

    def is_operational(self) -> bool:
        return True # TODO


class DynamicServer(Server):
    """A dynamic node is a node that is created and deleted by the manager automatically."""
    def __init__(self, name: str, deployment_status: DeploymentStatus, ipv4: IPv4Address,
                 ipv6: IPv6Address, vm_status: VmStatus, instance_definition: Optional[InstanceDefinition] = None,
                 vm_id="unknown", provider: str = "unknown"):
        super().__init__(name, str(ipv4))
        self.deployment_status: DeploymentStatus = deployment_status
        self.vm_status = vm_status
        self.ipv4: IPv4Address = ipv4
        self.ipv6: IPv6Address = ipv6
        self.instance_definition: Optional[InstanceDefinition] = instance_definition
        self.id: str = vm_id  # internal id of the vm as given by the provider
        self.provider: str = provider
        self.dns_record: Optional[DNSRecord] = None
        self.domain: Optional[str] = None

    def __str__(self):
        return f"[DynamicServer name: {self.name}; host: {self.host}; " \
               f"deployment status: {self.deployment_status}; vm status: {self.vm_status}; " \
               f"ipv4: {self.ipv4}; ipv6: {self.ipv6}; id: {self.id}, provider: {self.provider}] "

    async def set_domain_and_records(self, dns_manager: DNSManager) -> None:
        self.dns_record = await dns_manager.add_dynamic_node_name_from_ipv4(self.ipv4)
        self.domain = self.dns_record.name

    def is_operational(self) -> bool:
        return self.vm_status == VmStatus.RUNNING and self.deployment_status == DeploymentStatus.OPERATIONAL
