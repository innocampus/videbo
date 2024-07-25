from __future__ import annotations
from typing import Any, TypeVar, overload


T = TypeVar("T")


class Singleton(type):
    """Simple singleton metaclass."""

    _instance: object

    @overload
    def __init__(cls, o: object, /) -> None: ...

    @overload
    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
        **kwargs: Any,
    ) -> None: ...

    def __init__(
        cls,
        name: Any,
        bases: Any = ...,
        namespace: Any = ...,
        /,
        **kwargs: Any,
    ) -> None:
        """Initializes the internal class instance attribute."""
        super().__init__(name, bases, namespace, **kwargs)
        cls._instance = None

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Ensures only one instance of the actual class is ever created."""
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

    @classmethod
    def from_meta(cls, metaclass: T) -> T:  # intersection type would be nice...
        """Utility method to combine this with another metaclass."""
        class NewSingletonMetaclass(metaclass, cls):  # type: ignore[valid-type, misc]
            pass
        return NewSingletonMetaclass  # type: ignore[return-value]
