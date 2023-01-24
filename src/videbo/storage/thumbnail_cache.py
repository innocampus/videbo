from collections.abc import Iterable
from logging import getLogger
from pathlib import Path

from videbo import settings
from videbo.misc import MEGA
from videbo.misc.functions import run_in_default_executor
from videbo.misc.lru_dict import BytesLimitLRU


__all__ = ["ThumbnailCache"]

_log = getLogger(__name__)


class ThumbnailCache(BytesLimitLRU[Path]):
    def __init__(
        self,
        max_bytes: int = 0,
        other: Iterable[tuple[Path, bytes]] = (),
        **kwargs: bytes,
    ) -> None:
        if max_bytes != 0:
            _log.warning(
                f"Thumbnail cache size {max_bytes} independent of settings"
            )
        super().__init__(max_bytes, other=other, **kwargs)

    @property
    def max_bytes(self) -> int:
        return self._max_bytes or int(settings.thumbnails.cache_max_mb * MEGA)

    async def get_and_update(self, path: Path) -> bytes:
        if path in self:
            return self[path]
        data = await run_in_default_executor(path.read_bytes)
        if self.max_bytes:
            self[path] = data
        return data
