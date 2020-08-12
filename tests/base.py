import os
import sys
import asyncio
import unittest.mock
from pathlib import Path

from .utils import AsyncMockHelper
from videbo import settings


# Python 3.8 and greater has support for TestCases with `async` test methods
# as well as AsyncMock and other helpers. Therefore depending on the version used,
# the classes and functions imported from this module will be different.
# For more details on the new built-ins with `asyncio` support, see here:
# https://docs.python.org/3/library/unittest.html#unittest.IsolatedAsyncioTestCase
# https://docs.python.org/3/library/unittest.mock.html#unittest.mock.AsyncMock
NATIVE_ASYNC_TESTS = sys.version_info >= (3, 8)


if NATIVE_ASYNC_TESTS:
    _TestCase = getattr(unittest, 'IsolatedAsyncioTestCase')
    AsyncMock = getattr(unittest.mock, 'AsyncMock')
else:
    _TestCase = unittest.TestCase
    AsyncMock = AsyncMockHelper


class BaseTestCase(_TestCase, unittest.TestCase):
    CONFIG_FILE_NAME = 'config.ini'

    @classmethod
    def setUpClass(cls) -> None:
        """
        To provide access to to basic configuration during testing.
        """
        if settings.config.sections():
            return  # Config was already read
        config_file = Path(os.path.join(settings.topdir, cls.CONFIG_FILE_NAME))
        if not config_file.is_file():
            print(f"Config file does not exist: {config_file}")
            sys.exit(3)
        settings.config.read(config_file)
        settings.load()


def async_test(future):
    """
    Workaround for testing `async` functions in Python < 3.8.
    If Python version is high enough, it does nothing.
    When in doubt, decorate every `async` test method like so:
        ```
        @async_test
        async def test_something(self):
            ...
        ```
    """
    if NATIVE_ASYNC_TESTS:
        return future

    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(future(*args, **kwargs))
    return wrapper
