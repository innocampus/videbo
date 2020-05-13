from typing import Optional, List
from videbo.web import JSONBaseModel


class NodeStatus(JSONBaseModel):
    id: Optional[int]
    type: str
    name: str
    base_url: Optional[str]
    operational: bool
    created_manually: bool
    shutdown: bool
    enabled: bool


class NodesStatusList(JSONBaseModel):
    nodes: List[NodeStatus]


class CreateNewDistributorNodeParams(JSONBaseModel):
    definition_name: str
    bound_to_storage_node_base_url: str


class RemoveDistributorNodeParams(JSONBaseModel):
    node_name: str


class SetDistributorStatusParams(JSONBaseModel):
    node_name: str
    enable: bool
