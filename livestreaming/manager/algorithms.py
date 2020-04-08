from typing import Optional, Tuple, List, Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from .streams import ManagerStream
    from .node_types import ContentNode


AVAILABLE_CLIENTS_PER_STREAM = 10  # always available client slots per stream
ContentNodeListType = List[int]
StreamToContentType = List[Tuple[int, ContentNodeListType]]  # stream id to list of content node ids


class EncoderToContentReturnStatus:
    def __init__(self, clients_left_out: int, stream_to_content: Optional[StreamToContentType] = None):
        self.clients_left_out: int = clients_left_out  # need more content nodes for n clients
        self.stream_to_content: Optional[StreamToContentType] = stream_to_content


class EncoderToContentAlgorithmBase:
    async def solve(self, streams: List["ManagerStream"], contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        raise NotImplementedError()


class MToNEncoderToContentAlgorithm(EncoderToContentAlgorithmBase):
    """Very simple algorithm: Every stream is available on all content nodes."""
    async def solve(self, streams: List["ManagerStream"], contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        # Distribute all streams on all content nodes.
        stream_to_content: StreamToContentType = []
        for stream in streams:
            all_content_ids: List[int] = []
            for content in contents:
                all_content_ids.append(content.id)
            stream_to_content.append((stream.stream_id, all_content_ids))

        # Check if every stream can have AVAILABLE_CLIENTS_PER_STREAM
        total_max_clients = 0
        current_clients = 0
        for content in contents:
            total_max_clients += content.max_clients
            current_clients += content.current_clients
        for stream in streams:
            current_clients += stream.waiting_clients

        more_needed_client_slots = len(streams) * AVAILABLE_CLIENTS_PER_STREAM
        clients_left_out = max(0, (current_clients + more_needed_client_slots) - total_max_clients)

        return EncoderToContentReturnStatus(clients_left_out, stream_to_content)


def get_algorithm(name: str) -> EncoderToContentAlgorithmBase:
    if name == "m-to-n":
        return MToNEncoderToContentAlgorithm()

    raise AlgorithmNotFoundError(name)


class AlgorithmNotFoundError(Exception):
    def __init__(self, name: str):
        super().__init__(f"Algorithm with name {name} not found.")