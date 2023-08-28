from __future__ import annotations
from typing import Any
from unittest.mock import MagicMock


class SortableMock(MagicMock):
    """Sortable by the attribute named via `__mock_sort_key__`"""
    def __init__(
        self,
        __mock_sort_key__: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.__mock_sort_key__ = __mock_sort_key__

        def less_than(_self: SortableMock, other: SortableMock) -> bool:
            self_val = getattr(_self, _self.__mock_sort_key__)
            other_val = getattr(other, other.__mock_sort_key__)
            return self_val < other_val

        self.__lt__ = less_than
