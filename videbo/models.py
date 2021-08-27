from typing import Optional

from videbo.web import JSONBaseModel


class NodeStatus(JSONBaseModel):
    tx_current_rate: int  # in Mbit/s
    tx_max_rate: int  # in Mbit/s
    rx_current_rate: int  # in Mbit/s
    tx_total: int  # in MB
    rx_total: int  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    files_total_size: int  # in MB
    files_count: int
    free_space: int  # in MB
