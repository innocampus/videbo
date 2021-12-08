from typing import Union

from .storage import StorageSettings
from .distributor import DistributorSettings


settings: Union[StorageSettings, DistributorSettings, None] = None
