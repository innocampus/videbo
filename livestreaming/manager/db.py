import asyncio
import sqlite3
from typing import Optional, List, Dict, Tuple
from . import logger
from .node_types import NodeTypeBase, EncoderNode, ContentNode, DistributorNode
from .cloud.server import DynamicServer
from .cloud.status import VmStatus, DeploymentStatus

class Database:
    SCHEMA_VERSION = 1

    def __init__(self):
        self.con: Optional[sqlite3.Connection] = None
        self.file: Optional[str] = None
        self.async_con_lock = asyncio.Lock()

    def connect(self, file: str):
        # We only run queries within the async_con_lock. Hence we can pass check_same_thread.
        self.con = sqlite3.connect(file, check_same_thread=False)
        self.con.row_factory = sqlite3.Row  # change the way we can access the rows
        self.file = file
        self._create_update()

    async def disconnect(self):
        async with self.async_con_lock:
            self.con.close()

    def _create_update(self):
        c = self.con.cursor()
        create_tables = False
        schema_version = 0
        try:
            c.execute("PRAGMA user_version")
            schema_version = c.fetchone()[0]
            if schema_version == 0:
                # sqlite default is 0 when creating a new database
                create_tables = True
        except sqlite3.OperationalError:
            create_tables = True

        c.close()
        if create_tables:
            self._create_tables()
            return

        if schema_version != self.SCHEMA_VERSION:
            logger.fatal(f"DB: Invalid schema version, found {schema_version}, expected {self.SCHEMA_VERSION}")

    def _create_tables(self):
        logger.warning(f"Creating new tables in database {self.file}")
        c = self.con.cursor()
        c.executescript("""
PRAGMA user_version = """ + str(self.SCHEMA_VERSION) + """;

CREATE TABLE streams (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_range                TEXT NOT NULL,
    use_rtmps               NOT NULL,
    rtmp_stream_key         NOT NULL,
    encoder_subdir_name     NOT NULL,
    lms_stream_instance_id  INT NOT NULL,
    encoder_rtmp_port       INT,
    streamer_connection_until INT,
    expected_viewers        INT
);

CREATE TABLE dyn_nodes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    type                    NOT NULL,
    server_name             NOT NULL,
    server_host             NOT NULL,
    server_provider         NOT NULL,
    server_domain           NOT NULL,
    base_url                NOT NULL,
    created_manually        INT NOT NULL
);
        """)

        c.close()
        self.con.commit()

    async def save_node(self, node: NodeTypeBase):
        """Save a node with a dynamic server. Sets the id if there was no given yet."""
        if node.server is None:
            logger.warning("Trying to save a node that has no server.")
            return
        if not isinstance(node.server, DynamicServer):
            logger.warning("Trying to save a node that is not dynamic.")
            return
        if not node.server.is_operational():
            logger.warning("Trying to save a node that is not operational.")
            return

        node_type = type(node).__name__
        server = node.server

        def do_save():
            c = self.con.cursor()
            if node.id is None:
                c.execute("INSERT INTO dyn_nodes "
                          "(type, server_name, server_host, server_provider, server_domain, base_url, created_manually)"
                          "VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (node_type, server.name, server.host, server.provider, server.domain, node.base_url,
                           int(node.created_manually)))
                node.id = c.lastrowid
            else:
                c.execute("UPDATE dyn_nodes"
                          "SET type = ?, server_name = ?, server_host = ?, server_provider = ?, server_domain = ?,"
                          "    base_url = ?, created_manually = ?"
                          "WHERE id = ?",
                          (node_type, server.name, server.host, server.provider, server.domain, node.base_url,
                           int(node.created_manually),
                           node.id))
            c.close()
            self.con.commit()

        async with self.async_con_lock:
            await asyncio.get_event_loop().run_in_executor(None, do_save)

    async def delete_node(self, node: NodeTypeBase):
        if node.id is None:
            # Node was never saved.
            return

        def do_delete():
            c = self.con.cursor()
            c.execute("DELETE FROM dyn_nodes WHERE id = ?", (node.id, ))
            c.close()
            self.con.commit()

        async with self.async_con_lock:
            try:
                await asyncio.get_event_loop().run_in_executor(None, do_delete)
            except asyncio.CancelledError:
                pass
            except:
                logger.exception("Error in db delete_node")

    async def get_nodes(self, server_by_name: Dict[str, DynamicServer], loop) -> \
            Tuple[List[NodeTypeBase], List[NodeTypeBase]]:
        """
        Load nodes from database.

        :param server_by_name: existing servers
        :param loop: asyncio event loop
        :return: (nodes, orphaned_nodes)
        """
        def do_get():
            nodes = []
            orphaned_nodes = []

            c = self.con.cursor()
            rows = c.execute("SELECT * FROM dyn_nodes")
            for row in rows:
                node_type = row["type"]
                if node_type == "EncoderNode":
                    node = EncoderNode()
                elif node_type == "ContentNode":
                    node = ContentNode()
                elif node_type == "DistributorNode":
                    node = DistributorNode(loop)
                else:
                    logger.error(f"Unknown node type {node_type} in database found")
                    continue

                node.id = row["id"]
                node.base_url = row["base_url"]
                node.created_manually = bool(row["created_manually"])
                server = server_by_name.get(row["server_name"])
                if server:
                    # name is already set
                    server.host = row["server_host"]
                    server.provider = row["server_provider"]
                    server.domain = row["server_domain"]
                    # Assume the server is operational. It was that at least when saving the node in the db.
                    server.deployment_status = DeploymentStatus.OPERATIONAL
                    node.server = server
                    nodes.append(node)
                else:
                    orphaned_nodes.append(node)

            c.close()
            return nodes, orphaned_nodes

        async with self.async_con_lock:
            return await asyncio.get_event_loop().run_in_executor(None, do_get)


class NoConnectionError(Exception):
    pass