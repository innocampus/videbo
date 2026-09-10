import logging
import os
from typing import TYPE_CHECKING


__all__ = [
    "SilentLogMixin",
]


# Set this to a non-empty value to keep the `videbo` log output visible.
# Useful when a test fails in a way that only the application log explains,
# e.g. on a CI runner that cannot be inspected directly.
SHOW_LOGS_ENV_VAR = "VIDEBO_TEST_LOGS"


if TYPE_CHECKING:
    from unittest import TestCase

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
        if not os.environ.get(SHOW_LOGS_ENV_VAR):
            cls.videbo_main_log.setLevel(logging.CRITICAL)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.videbo_main_log.setLevel(cls.log_lvl)
        super().tearDownClass()
