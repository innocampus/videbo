from time import time
from typing import Dict
from . import content_logger


class Client:
    def __init__(self, rid: str):
        self.rid: str = rid  # string to identify a unique user
        self.last_seen: float = time()

    def update_seen(self) -> None:
        self.last_seen = time()


class ClientsOnStream:
    def __init__(self, stream_id: int):
        self.stream_id: int = stream_id
        self.clients: Dict[str, Client] = {}  # maps rid to Client
        self.max_clients: int = -1  # maximum of clients on this stream, -1 means no limit


class ClientCollection:
    def __init__(self):
        self.streams: Dict[int, ClientsOnStream] = {}  # maps stream id
        self.max_clients: int = 0  # maximum of clients on the whole server
        self.current_clients: int = 0

    def init(self, max_clients: int):
        self.max_clients = max_clients

    def purge(self):
        """Remove all clients that were not seen for 5 seconds."""
        compare_time = time() - 5
        current_clients = 0
        for stream in self.streams.values():
            for client in list(stream.clients.values()):  # Copy to list as we modify the dict
                if client.last_seen <= compare_time:
                    stream.clients.pop(client.rid)
            current_clients += len(stream.clients)

        self.current_clients = current_clients

    def get_current_clients_total(self):
        total = 0
        for stream in self.streams.values():
            total += len(stream.clients)
        return total

    def serve_client(self, stream_id: int, client_rid: str) -> bool:
        """Check if we can (still) handle this client request."""
        stream = self.streams.get(stream_id)
        if stream is None:
            content_logger.info(f"Could not find <stream {stream_id}>")
            return False

        client = stream.clients.get(client_rid)
        if client:
            client.update_seen()

            # Check if we still have room for the client (max_client can change!).
            if self.current_clients > self.max_clients or \
                    (stream.max_clients >= 0 and len(stream.clients) > stream.max_clients):
                # Remove user.
                content_logger.info(f"No room more for existing <client {client_rid}>: "
                                    f"<stream {stream_id}> {len(stream.clients)}/{stream.max_clients}; "
                                    f"node {self.current_clients}/{self.max_clients}")
                stream.clients.pop(client.rid)
                self.current_clients -= 1
                return False

        else:
            # We don't know this client yet. Check if we have room for it.
            if (self.current_clients + 1) > self.max_clients or \
                    (stream.max_clients >= 0 and (len(stream.clients) + 1) > stream.max_clients):
                content_logger.info(f"No room for new <client {client_rid}>: "
                                    f"<stream {stream_id}> {len(stream.clients)}/{stream.max_clients}; "
                                    f"node {self.current_clients}/{self.max_clients}")
                return False

            stream.clients[client_rid] = Client(client_rid)
            self.current_clients += 1

        return True

    def add_stream(self, stream_id: int) -> ClientsOnStream:
        stream = ClientsOnStream(stream_id)
        self.streams[stream_id] = stream
        return stream

    def remove_stream(self, stream_id: int) -> None:
        try:
            self.streams.pop(stream_id)
        except KeyError:
            pass


client_collection = ClientCollection()