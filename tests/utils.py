from unittest.mock import MagicMock


class AsyncMockHelper(MagicMock):
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

    @property
    def await_args_list(self):
        return super().call_args_list
