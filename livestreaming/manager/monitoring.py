import asyncio
from aiohttp.client_exceptions import ClientError
from aioinflux import InfluxDBClient
from datetime import datetime, timezone
from socket import gethostname
from typing import Dict, Optional
from typing import NamedTuple
import re

from livestreaming.misc import TaskManager
from .node_controller import NodeController
from .node_types import EncoderNode, DistributorNode, StorageNode

from . import logger, manager_settings


class DataPoint(NamedTuple):
    measurement: str
    fields: Dict
    time: str
    tags: Dict = {}


class InfluxManager:
    def __init__(self, host, db, username, password, port=8086, ssl=True):
        self.host = gethostname()
        self.static_tags = {'host': self.host, 'type': 'mgmt'}
        self.client = InfluxDBClient(host=host, port=port, ssl=ssl, db=db,
                                     username=username, password=password)

    async def write_data_point(self, data_point: DataPoint):
        data_point.tags.update(self.static_tags)
        await self.client.write(data_point._asdict())

    async def close(self):
        await self.client.close()


class Monitoring:
    def __init__(self, nc: NodeController):
        self.nc = nc
        self.operational = True

        influx_url = self.nc.manager_settings.influx_url
        url_match = re.search(r"(https:\/\/|http:\/\/)?([a-zA-Z\-\.]*):?([0-9]*)?", influx_url)
        self.influx_host = url_match.group(2)

        if url_match.group(3):
            self.influx_port = int(url_match.group(3))
        else:
            self.influx_port = 8086

        self.influx: Optional[InfluxManager] = None

    def stop(self):
        self.operational = False

    async def encoder_stats(self):
        encoder_nodes = self.nc.get_operating_nodes(EncoderNode)
        for e in encoder_nodes:
            current_streams = e.current_streams
            encoder_name = e.server.name
            fields = {'current_streams': current_streams}

            # add more fields here before sending metrics to influx
            # fields.update({'other_metric': 'metric'})

            data_point = DataPoint(measurement='mgmt_encoder_stats', tags={'encoder_name': encoder_name},
                                   fields=fields, time=datetime.now(timezone.utc).isoformat())
            await self.influx.write_data_point(data_point)

    async def distributor_stats(self):
        nodes = self.nc.get_operating_nodes(DistributorNode)
        for node in nodes:
            fields = {
                'tx_current_rate': node.tx_current_rate,  # in Mbit/s
                'tx_max_rate': node.tx_max_rate,  # in Mbit/s
                'rx_current_rate': node.rx_current_rate,  # in Mbit/s
                'tx_total': node.tx_total,  # in MB
                'rx_total': node.rx_total,  # in MB
                'current_connections': node.current_connections,  # HTTP connections serving videos
                'waiting_clients': node.waiting_clients,  # number of clients waiting for a file being downloaded
                'files_total_size': node.files_total_size,  # in MB
                'files_count': node.files_count,
                'free_space': node.free_space,  # in MB
                'copying_files_count': len(node.copy_files_status),  # how many files are being copied right now
            }

            data_point = DataPoint(measurement='mgmt_distributor_stats', tags={'distributor_name': node.server.name},
                                   fields=fields, time=datetime.now(timezone.utc).isoformat())
            await self.influx.write_data_point(data_point)

    async def storage_stats(self):
        nodes = self.nc.get_operating_nodes(StorageNode)
        for node in nodes:
            fields = {
                'tx_current_rate': node.tx_current_rate,  # in Mbit/s
                'tx_max_rate': node.tx_max_rate,  # in Mbit/s
                'rx_current_rate': node.rx_current_rate,  # in Mbit/s
                'tx_total': node.tx_total,  # in MB
                'rx_total': node.rx_total,  # in MB
                'current_connections': node.current_connections,  # HTTP connections serving videos
                'files_total_size': node.files_total_size,  # in MB
                'files_count': node.files_count,
                'free_space': node.free_space,  # in MB
                'dist_nodes_count': len(node.dist_nodes),
            }

            data_point = DataPoint(measurement='mgmt_storage_stats', tags={'storage_name': node.server.name},
                                   fields=fields, time=datetime.now(timezone.utc).isoformat())
            await self.influx.write_data_point(data_point)

    async def overall_stats(self):
        tx_current_rate = 0
        tx_max_rate = 0
        videos_current_connections = 0
        videos_waiting_clients = 0
        videos_files_total_size = 0
        videos_files_count = 0
        videos_copying = 0

        nodes = self.nc.get_operating_nodes(DistributorNode)
        for node in nodes:
            tx_current_rate += node.tx_current_rate
            tx_max_rate += node.tx_max_rate
            if node.current_connections:
                videos_current_connections += node.current_connections
            videos_waiting_clients += node.waiting_clients
            videos_copying += len(node.copy_files_status)

        nodes = self.nc.get_operating_nodes(StorageNode)
        for node in nodes:
            tx_current_rate += node.tx_current_rate
            tx_max_rate += node.tx_max_rate
            if node.current_connections:
                videos_current_connections += node.current_connections
            videos_files_total_size += node.files_total_size
            videos_files_count += node.files_count

        fields = {
            'tx_current_rate': tx_current_rate,  # in Mbit/s
            'tx_max_rate': tx_max_rate,  # in Mbit/s
            'videos_current_connections': videos_current_connections,
            'videos_waiting_clients': videos_waiting_clients,
            'videos_files_total_size': videos_files_total_size,
            'videos_files_count': videos_files_count,
            'videos_copying': videos_copying,
        }

        data_point = DataPoint(measurement='mgmt_overall_stats', tags=dict(),
                               fields=fields, time=datetime.now(timezone.utc).isoformat())
        await self.influx.write_data_point(data_point)

    async def _monitoring_loop(self):
        self.operational = True
        delay = 0
        try:
            while self.operational:
                try:
                    self.influx = InfluxManager(self.influx_host, self.nc.manager_settings.influx_database,
                                  self.nc.manager_settings.influx_username,
                                  self.nc.manager_settings.influx_password, port=self.influx_port)
                    await asyncio.sleep(15 - delay)
                    measurements_start = datetime.now()

                    await self.encoder_stats()
                    await self.distributor_stats()
                    await self.storage_stats()
                    await self.overall_stats()

                    measurements_end = datetime.now()
                    delay = (measurements_end - measurements_start).total_seconds()
                    await self.influx.close()
                    self.influx = None
                except ClientError:
                    # retry in a few seconds
                    logger.exception("Influx error")
                    await asyncio.sleep(120)
        finally:
            if self.influx:
                await self.influx.close()

    async def run(self):
        if self.nc.manager_settings.influx_url:
            task = asyncio.create_task(self._monitoring_loop())
            TaskManager.fire_and_forget_task(task)
