from typing import Container, Dict, Tuple, Set, Callable, Optional, Type

from pydantic.main import BaseModel
from prometheus_client.registry import CollectorRegistry
from prometheus_client.metrics import Gauge
from prometheus_client.exposition import write_to_textfile
from prometheus_client.process_collector import ProcessCollector

from videbo.misc import Periodic
from videbo.models import NodeStatus
from videbo.distributor.api.models import DistributorStatus
from .api.models import StorageStatus
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

    @staticmethod
    def delete_text_file() -> None:
        storage_settings.prom_text_file.unlink()
        storage_logger.info(f"Deleted monitoring text file {storage_settings.prom_text_file}")

    def __init__(self) -> None:
        self._periodic: Periodic = Periodic(self.update_all_metrics)
        self._periodic.post_stop_callbacks.append(self.delete_text_file)

        self.update_freq_sec: float = storage_settings.prom_update_freq_sec
        self.registry = CollectorRegistry()
        self.metrics: Dict[str, Tuple[Gauge, Optional[Callable]]] = {}
        self.dist_urls: Set[str] = set()
        self._add_metrics_from_model(StorageStatus, exclude={'distributor_nodes'})
        self._add_metrics_from_model(DistributorStatus, exclude={'bound_to_storage_node_base_url', 'copy_files_status'})
        self._init_calc_metrics()

        self.process_collector = ProcessCollector(registry=self.registry)

    def _add_metrics_from_model(self, model_class: Type[BaseModel], exclude: Container[str] = ()) -> None:
        """Adds metrics to be taken directly "as is" from a status object."""
        for name, field in model_class.__fields__.items():
            if name in exclude:
                continue
            if name in self.metrics.keys():
                continue
            self.metrics[name] = (
                Gauge(
                    name=self.METRIC_PREFIX + name,
                    documentation=field.field_info.description or self.DOC_PLACEHOLDER,
                    labelnames=(self.NODE_TYPE, self.BASE_URL),
                    registry=self.registry
                ),
                None
            )

    def _init_calc_metrics(self) -> None:
        """Adds metrics that have to be calculated from a status object's attributes."""
        name = 'num_files_being_copied'
        self.metrics[name] = (
            Gauge(
                name=self.METRIC_PREFIX + name,
                documentation=self.DOC_PLACEHOLDER,
                labelnames=(self.NODE_TYPE, self.BASE_URL),
                registry=self.registry
            ),
            lambda status_obj: len(status_obj.copy_files_status)
        )

    async def update_all_metrics(self) -> None:
        """
        Retrieves the current status objects for the storage and all linked distributor nodes and updates the internal
        metrics dictionary accordingly. Uses node type and base url labels to distinguish storage and distributors.
        After updating the dictionaries, the metrics are written to the text file for the Prometheus node exporter.
        """
        storage = FileStorage.get_instance()
        storage_status = await storage.get_status()
        dist_status_dict = storage.distribution_controller.get_nodes_status(only_good=True, only_enabled=True)
        # Make sure the distributors returned here have not changed;
        # if they did in any way, clear the metrics before calling the update method.
        urls = set(dist_status_dict.keys())
        if urls != self.dist_urls:
            self._clear_all_metrics()
            self.dist_urls = urls
        self._update_metrics(storage_status, 'storage', storage_settings.public_base_url)
        for url, status in dist_status_dict.items():
            self._update_metrics(status, 'dist', url)
        write_to_textfile(storage_settings.prom_text_file, self.registry)

    def _update_metrics(self, status_obj: NodeStatus, *labels: str) -> None:
        for name, (metric, get_value) in self.metrics.items():
            try:
                val = get_value(status_obj) if get_value else getattr(status_obj, name)
            except AttributeError:
                pass  # some metrics apply exclusively to the storage node or a distributor, but not both
            else:
                metric.labels(*labels).set(val or 0)

    def _clear_all_metrics(self) -> None:
        for metric, _ in self.metrics.values():
            metric.clear()

    async def run(self) -> None:
        storage_logger.info(f"Started monitoring and writing to {storage_settings.prom_text_file}")
        self._periodic(self.update_freq_sec, call_immediately=True)

    async def stop(self) -> None:
        await self._periodic.stop()
