from pathlib import Path

from pydantic import validator

from videbo.base_settings import CommonSettings
from videbo.misc.functions import ensure_url_does_not_end_with_slash as normalize_url


class DistributorSettings(CommonSettings):
    _section = 'distributor'

    listen_port: int = 9030
    files_path: Path = Path('/tmp/videbo/distributor')
    bound_to_storage_base_url: str = 'http://localhost:9020'
    leave_free_space_mb: float = 4000.0
    last_request_safety_hours: int = 4

    _norm_storage_base_url = validator('bound_to_storage_base_url', allow_reuse=True)(normalize_url)
