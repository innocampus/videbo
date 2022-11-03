import logging
from typing import TYPE_CHECKING
from unittest import TestCase


__all__ = [
    "SilentLogMixin",
]


if TYPE_CHECKING:
    _Base = TestCase
else:
    _Base = object


class SilentLogMixin(_Base):
    videbo_main_log: logging.Logger
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.videbo_main_log = logging.getLogger('videbo')
        cls.log_lvl = cls.videbo_main_log.level
        cls.videbo_main_log.setLevel(logging.CRITICAL)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.videbo_main_log.setLevel(cls.log_lvl)
        super().tearDownClass()
