from typing import Any, Generic, Optional, TypeVar, get_args, get_origin


_T = TypeVar("_T")


class GenericInsightMixin(Generic[_T]):
    """
    Utility mixin to allow runtime use of the type argument inside a class.

    The type argument of any inheriting **concrete** class can be used in a
    type-safe manner via the `get_type_arg` method.
    """
    _type_arg: Optional[type[_T]] = None

    @classmethod
    def get_type_arg(cls) -> type[_T]:
        """Returns the type argument of the class (if specified)."""
        if cls._type_arg is None:
            raise AttributeError(
                f"{cls.__name__} is generic; type argument unspecified"
            )
        return cls._type_arg

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Saves the type argument in the `_type_arg` class attribute."""
        super().__init_subclass__(**kwargs)
        for base in cls.__orig_bases__:  # type: ignore[attr-defined]
            origin = get_origin(base)
            if origin is None or not issubclass(origin, GenericInsightMixin):
                continue
            type_arg = get_args(base)[0]
            # Do not set the attribute for GENERIC subclasses!
            if not isinstance(type_arg, TypeVar):
                cls._type_arg = type_arg
                return
