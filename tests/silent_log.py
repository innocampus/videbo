import logging
from typing import TYPE_CHECKING
from unittest import TestCase


if TYPE_CHECKING:
    _Base = TestCase
else:
    _Base = object

main_log = logging.getLogger('videbo')


class SilentLogMixin(_Base):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = main_log.level
        main_log.setLevel(logging.CRITICAL)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        main_log.setLevel(cls.log_lvl)
        super().tearDownClass()
