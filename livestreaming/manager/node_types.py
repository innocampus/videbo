from asyncio import Task, sleep
from typing import Optional
from .cloud.server import Server
from .streams import ManagerStream


class NodeTypeBase:
    def __init__(self):
        self.lifecycle_task: Optional[Task] = None
        self.server: Optional[Server] = None
        self.base_url: Optional[str] = None

    async def watchdog(self):
        raise NotImplementedError()


class EncoderNode(NodeTypeBase):
    pass


class ContentNode(NodeTypeBase):
    def __init__(self):
        super().__init__()
        self.max_clients: int = 0
        self.current_clients: int = 0

    async def watchdog(self):
        while True:
            # TODO
            await sleep(1)
