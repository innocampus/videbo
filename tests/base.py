import sys
import asyncio
import unittest.mock

# TODO: Get rid of this module

# Python 3.8 and greater has support for TestCases with `async` test methods
# as well as AsyncMock and other helpers. Therefore depending on the version used,
# the classes and functions imported from this module will be different.
# For more details on the new built-ins with `asyncio` support, see here:
# https://docs.python.org/3/library/unittest.html#unittest.IsolatedAsyncioTestCase
# https://docs.python.org/3/library/unittest.mock.html#unittest.mock.AsyncMock
NATIVE_ASYNC_TESTS = sys.version_info >= (3, 8)


class AsyncMock(unittest.mock.MagicMock):
    """
    Workaround for patching `async` functions in Python < 3.8.

    Used for patching; passed as argument for the `new_callable` parameter.
    Provides a simple way to do assertions on the mocked object like e.g.:
        `mocked_async_func.assert_awaited_with(...)`

    Note that from Python 3.8 onwards, there is the native `AsyncMock` class that does all this better.
    """
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)

    def assert_awaited_once(self):
        return super().assert_called_once()

    def assert_awaited_once_with(self, *args, **kwargs):
        return super().assert_called_once_with(*args, **kwargs)

    def assert_awaited_with(self, *args, **kwargs):
        return super().assert_called_with(*args, **kwargs)

    def assert_not_awaited(self):
        return super().assert_not_called()

    @property
    def await_args(self):
        return super().call_args


# To have IDEs recognize subclasses of our `BaseTestCase` as inheriting from `unittest.TestCase`, we make sure it
# always first inherits from `unittest.TestCase` and then if possible from `unittest.IsolatedAsyncioTestCase` as we
# get it dynamically; if it is not possible we let the second superclass be an empty placeholder.
class _TestCase:
    pass


if NATIVE_ASYNC_TESTS:
    _TestCase = getattr(unittest, 'IsolatedAsyncioTestCase')
    AsyncMock = getattr(unittest.mock, 'AsyncMock')


class BaseTestCase(_TestCase, unittest.TestCase):
    pass


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
