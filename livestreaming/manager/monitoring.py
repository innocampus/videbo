import asyncio
from aioinflux import InfluxDBClient
from datetime import datetime, timezone
from socket import gethostname
from typing import Dict
from typing import NamedTuple
import re

from .node_controller import NodeController
from .node_types import EncoderNode

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
        influx_host = url_match.group(2)

        if url_match.group(3):
            influx_port = int(url_match.group(3))
        else:
            influx_port = 8086

        self.influx = InfluxManager(influx_host, self.nc.manager_settings.influx_database,
                                    self.nc.manager_settings.influx_username,
                                    self.nc.manager_settings.influx_password, port=influx_port)

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

    async def _monitoring_loop(self):
        self.operational = True
        delay = 0
        while self.operational:
            await asyncio.sleep(15 - delay)
            measurements_start = datetime.now()

            await self.encoder_stats()

            measurements_end = datetime.now()
            delay = (measurements_end - measurements_start).total_seconds()

    async def run(self):
        if self.nc.manager_settings.influx_url:
            asyncio.create_task(self._monitoring_loop())
