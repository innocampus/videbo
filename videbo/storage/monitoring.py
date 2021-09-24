import asyncio
from typing import Container, Dict, Optional, Type
from time import time

from pydantic.main import BaseModel
from prometheus_client.registry import CollectorRegistry
from prometheus_client.metrics import Gauge
from prometheus_client.exposition import write_to_textfile
from prometheus_client.process_collector import ProcessCollector

from videbo.misc import TaskManager
from videbo.models import NodeStatus
from .util import FileStorage
from . import storage_settings, storage_logger


class Monitoring:
    METRIC_PREFIX = 'videbo_'
    DOC_PLACEHOLDER = '...'
    NODE_TYPE, BASE_URL = 'node_type', 'base_url'

    _instance: Optional['Monitoring'] = None

    @classmethod
    def get_instance(cls) -> 'Monitoring':
        if cls._instance is None:
            cls._instance = Monitoring()
        return cls._instance

    def __init__(self) -> None:
        self._operational = True
        self.update_freq_sec: float = storage_settings.prom_update_freq_sec
        self.registry = CollectorRegistry()
        self.metrics: Dict[str, Gauge] = {}
        self.add_metrics_from_model(NodeStatus, exclude={'tx_current_rate', 'rx_current_rate'})
        self.process_collector = ProcessCollector(registry=self.registry)

    def add_metrics_from_model(self, model_class: Type[BaseModel], exclude: Container[str] = ()) -> None:
        for name, field in model_class.__fields__.items():
            if name in exclude:
                continue
            if name in self.metrics.keys():
                storage_logger.warning(f"Existing metric `{name}` is being replaced")
            self.metrics[name] = Gauge(
                name=self.METRIC_PREFIX + name,
                documentation=field.field_info.description or self.DOC_PLACEHOLDER,
                labelnames=(self.NODE_TYPE, self.BASE_URL),
                registry=self.registry
            )

    async def update_all_metrics(self) -> None:
        """
        Retrieves the current status objects for the storage and all linked distributor nodes and updates the internal
        metrics dictionary accordingly. Uses node type and base url labels to distinguish storage and distributors.
        """
        storage = FileStorage.get_instance()
        storage_status = await storage.get_status()
        self._update_metrics(storage_status, 'storage', storage_settings.public_base_url)
        for url, status in storage.distribution_controller.get_nodes_status().items():
            self._update_metrics(status, 'dist', url)

    def _update_metrics(self, status_obj: NodeStatus, *labels: str) -> None:
        for name, metric in self.metrics.items():
            val = getattr(status_obj, name)
            metric.labels(*labels).set(val or 0)

    async def _monitoring_loop(self) -> None:
        """
        Periodically updates the internal metrics and writes them to the text file for the Prometheus node exporter.
        """
        self._operational = True
        delay = 0
        while self._operational:
            await asyncio.sleep(self.update_freq_sec - delay)
            started = time()
            await self.update_all_metrics()
            write_to_textfile(storage_settings.prom_text_file, self.registry)
            delay = time() - started

    async def run(self) -> None:
        TaskManager.fire_and_forget_task(asyncio.create_task(self._monitoring_loop()))

    def stop(self) -> None:
        self._operational = False
