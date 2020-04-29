from typing import Optional, List
from livestreaming.web import JSONBaseModel
from livestreaming.streams import StreamState


class LMSNewStreamParams(JSONBaseModel):
    ip_range: Optional[str]
    rtmps: bool
    lms_stream_instance_id: int
    expected_viewers: Optional[int]


class StreamStatus(JSONBaseModel):
    stream_id: int
    lms_stream_instance_id: int
    state: StreamState
    state_last_update: int
    viewers: int
    thumbnail_urls: List[str]


class StreamStatusFull(StreamStatus):
    streamer_url: str
    streamer_key: str
    streamer_ip_restricted: bool
    streamer_connection_until: Optional[int]  # time to connect to encoder
    viewer_broker_url: str


class LMSNewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[StreamStatusFull]
    error: Optional[str]


class AllStreamsStatus(JSONBaseModel):
    streams: List[StreamStatus]


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
