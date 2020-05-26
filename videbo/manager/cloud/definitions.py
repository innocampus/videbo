from operator import attrgetter
from typing import Dict, List, Type, Optional
from videbo import Settings


class ProviderDefinition:
    pass


class ProviderApiKeyDefinition(ProviderDefinition):
    def __init__(self, key: str, ssh_key_name: str):
        self.key: str = key
        self.ssh_key_name: str = ssh_key_name

    def __repr__(self):
        return f"<{self.__class__.__name__}, key: {self.key}, ssh_key_name: {self.ssh_key_name}>"


class HetznerApiKeyDefinition(ProviderApiKeyDefinition):
    pass


class OvhApiKeyDefinition(ProviderDefinition):
    def __init__(self, application_key: str, application_secret: str, consumer_key: str,
                 service: str, ssh_key_name: str):
        self.application_key = application_key
        self.application_secret = application_secret
        self.consumer_key = consumer_key
        self.service = service
        self.ssh_key_name = ssh_key_name


class INWXApiAuthDefinition(ProviderDefinition):
    def __init__(self, username: str, password: str):
        self.username: str = username
        self.password: str = password


class InstanceDefinition:
    section_name_prefix: str

    def __init__(self, config: Settings, section_name: str):
        self.section_name: str = section_name
        self.provider: str = config.get_config(section_name, 'provider')
        self.location: str = config.get_config(section_name, 'location')
        self.user: str = config.get_config(section_name, 'login_user')
        self.server_type: str = config.get_config(section_name, 'server_type')
        self.priority: int = int(config.get_config(section_name, 'priority'))

    def __repr__(self):
        return f"<{self.__class__.__name__}, section: {self.section_name}, provider: {self.provider}, " \
               f"server_type: {self.server_type}>"


class DistributorInstanceDefinition(InstanceDefinition):
    section_name_prefix: str = "cloud-distributor-"

    def __init__(self, config: Settings, section_name: str):
        super().__init__(config, section_name)
        self.tx_max_rate_mbit: int = int(config.get_config(section_name, 'tx_max_rate_mbit'))
        self.leave_free_space_mb: int = int(config.get_config(section_name, 'leave_free_space_mb'))


OrderedInstanceDefinitionsList = List[InstanceDefinition]


class CloudInstanceDefsController:
    provider_definitions: Dict[str, ProviderDefinition] # map provider name to definition
    dns_provider_definition: Optional[ProviderDefinition]
    domain: Optional[str]

    def __init__(self):
        self.provider_definitions = {}
        self.dns_provider_definition = None
        self.domain = None
        self.distributor_definitions = {}
        self.node_def_by_name: Dict[str, InstanceDefinition] = {}

    def init_from_config(self, config: Settings):
        self._init_provider_definitions(config)
        self._init_node_definitions(config, self.distributor_definitions, DistributorInstanceDefinition)

    def _init_provider_definitions(self, config: Settings):
        providers = config.get_config('manager', 'cloud_providers')
        providers = list(map(str.strip, providers.split(',')))
        for provider in providers:
            if provider == 'hetzner':
                definition = HetznerApiKeyDefinition(config.get_config('cloud-hetzner', 'api_token'),
                                                     config.get_config('cloud-hetzner', 'ssh_key_name'))
            elif provider == 'ovh':
                definition = OvhApiKeyDefinition(config.get_config('cloud-ovh', 'application_key'),
                                                 config.get_config('cloud-ovh', 'application_secret'),
                                                 config.get_config('cloud-ovh', 'consumer_key'),
                                                 config.get_config('cloud-ovh', 'service'),
                                                 config.get_config('cloud-ovh', 'ssh_key_name')
                                                 )
            else:
                raise UnknownProviderError(provider)
            self.provider_definitions[provider] = definition

        dns_provider = config.get_config('manager', 'dns_provider')
        if dns_provider:
            self.domain = config.get_config('general', 'domain')
            if dns_provider == 'inwx':
                self.dns_provider_definition = INWXApiAuthDefinition(config.get_config('cloud-inwx', 'username'),
                                                                     config.get_config('cloud-inwx', 'password'))
            else:
                raise UnknownProviderError(dns_provider)

    def _init_node_definitions(self, config: Settings, collection: Dict, instance_type: Type[InstanceDefinition]):
        section: str
        for section in config.config.sections():
            if section.startswith(instance_type.section_name_prefix):
                instance = instance_type(config, section)
                if instance.provider not in self.provider_definitions:
                    raise UnknownProviderError(instance.provider)

                if instance.priority not in collection:
                    collection[instance.priority] = [instance]
                else:
                    collection[instance.priority].append(instance)

                self.node_def_by_name[instance.section_name] = instance

    def get_matching_distributor_defs(self, min_tx_rate_mbit: int) -> List[DistributorInstanceDefinition]:
        """Get a list of possible content instances in the order that we should try to buy."""
        return self._get_matching_defs(self.distributor_definitions, 'tx_max_rate_mbit', min_tx_rate_mbit)

    def _get_matching_defs(self, collection: Dict[int, List[InstanceDefinition]], key: str, min_value: int):
        list: List = []

        # iterate over all priority levels
        for priority, definitions_in_prio in sorted(collection.items()):
            # sort all instance definitions by the key value
            sorted_list = sorted(definitions_in_prio, key=attrgetter(key))

            # Add all instances that fulfil the min_value criterion or are bigger than wished.
            for instance in sorted_list:
                if getattr(instance, key) >= min_value:
                    list.append(instance)

            # Now add all instances that don't fulfil the criterion.
            sorted_list.reverse()
            for instance in sorted_list:
                if getattr(instance, key) < min_value:
                    list.append(instance)

        return list

    def get_node_definition_with_name(self, section_name: str) -> Optional[InstanceDefinition]:
        return self.node_def_by_name.get(section_name)


class UnknownProviderError(Exception):
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Config: provider {self.name} unknown")