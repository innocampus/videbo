from typing import Optional, Tuple

from videbo.web import JSONBaseModel


FileID = Tuple[str, str]  # (hash, file extension)


class NodeStatus(JSONBaseModel):
    tx_current_rate: float  # in Mbit/s
    tx_max_rate: float  # in Mbit/s
    rx_current_rate: float  # in Mbit/s
    tx_total: float  # in MB
    rx_total: float  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    files_total_size: float  # in MB
    files_count: int
    free_space: float  # in MB
