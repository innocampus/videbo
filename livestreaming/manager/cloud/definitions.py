from operator import attrgetter
from typing import Dict, List, Type, Callable
from livestreaming import Settings


class ProviderDefinition:
    pass


class ProviderApiKeyDefinition(ProviderDefinition):
    def __init__(self, key: str):
        self.key: str = key

    def __repr__(self):
        return f"<{self.__class__.__name__}, key: {self.key}>"


class HetznerApiKeyDefinition(ProviderApiKeyDefinition):
    pass


class InstanceDefinition:
    section_name_prefix: str

    def __init__(self, config: Settings, section_name: str):
        self.section_name: str = section_name
        self.provider: str = config.get_config(section_name, 'provider')
        self.location: str = config.get_config(section_name, 'location')
        self.server_type: str = config.get_config(section_name, 'server_type')
        self.priority: int = int(config.get_config(section_name, 'priority'))

    def __repr__(self):
        return f"<{self.__class__.__name__}, section: {self.section_name}, provider: {self.provider}, " \
               f"server_type: {self.server_type}>"


class ContentInstanceDefinition(InstanceDefinition):
    section_name_prefix: str = "cloud-content-"

    def __init__(self, config: Settings, section_name: str):
        super().__init__(config, section_name)
        self.max_clients: int = int(config.get_config(section_name, 'max_clients'))


class EncoderInstanceDefinition(InstanceDefinition):
    section_name_prefix: str = "cloud-encoder-"

    def __init__(self, config: Settings, section_name: str):
        super().__init__(config, section_name)
        self.max_streams: int = int(config.get_config(section_name, 'max_streams'))


class CloudInstanceDefsController:
    provider_definitions: Dict[str, ProviderDefinition] # map provider name to definition
    content_definitions: Dict[int, List[ContentInstanceDefinition]] # priority to list of definitions
    encoder_definitions: Dict[int, List[EncoderInstanceDefinition]]

    def __init__(self):
        self.provider_definitions = {}
        self.content_definitions = {}
        self.encoder_definitions = {}

    def init_from_config(self, config: Settings):
        self._init_provider_definitions(config)
        self._init_node_definitions(config, self.content_definitions, ContentInstanceDefinition)
        self._init_node_definitions(config, self.encoder_definitions, EncoderInstanceDefinition)

    def _init_provider_definitions(self, config: Settings):
        providers = config.get_config('manager', 'cloud_providers').split(',')
        for provider in providers:
            if provider == 'hetzner':
                definition = HetznerApiKeyDefinition(config.get_config('manager', 'hetzner_api_token'))
            else:
                raise UnknownProviderError(provider)
            self.provider_definitions[provider] = definition

    def _init_node_definitions(self, config: Settings, collection: Dict, instance_type: Type[InstanceDefinition]):
        section: str
        for section in config.config.sections():
            if section.startswith(instance_type.section_name_prefix):
                instance = instance_type(config, section)
                if instance.priority not in collection:
                    collection[instance.priority] = [instance]
                else:
                    collection[instance.priority].append(instance)

    def get_matching_content_defs(self, clients: int) -> List[ContentInstanceDefinition]:
        """Get a list of possible content instances in the order that we should try to buy."""
        return self._get_matching_defs(self.content_definitions, 'max_clients', clients)

    def get_matching_encoder_defs(self, streams: int) -> List[EncoderInstanceDefinition]:
        return self._get_matching_defs(self.encoder_definitions, 'max_streams', streams)

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


class UnknownProviderError(Exception):
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Config: provider {self.name} unknown")
