from collections import OrderedDict
from collections.abc import Iterable, Hashable

from videbo.exceptions import SizeError


class BytesLimitLRU(OrderedDict[Hashable, bytes]):
    """
    Limit object memory size, evicting the least recently accessed key when the specified maximum is exceeded.
    Only `bytes` type values are accepted. Their size is calculated by passing them into the builtin `len()` function.
    """

    def __init__(self, max_bytes: int, other: Iterable[tuple[Hashable, bytes]] = (), **kwargs: bytes) -> None:
        """
        Uses the `dict` constructor to initialize; in addition the `.max_bytes` attribute is set and the protected
        attribute `._total_bytes` used for keeping track of the total size of the values stored is initialized.

        For details on the behaviour when adding items, refer to the `__setitem__` method.

        Since initializing a non-empty instance (i.e. using `other` or `kwargs`) makes it call the `__setitem__`
        method for each item, we introduce a private flag to prevent initializing with a total bytes size that exceeds
        the specified maximum. Otherwise some of the initial items would just be silently removed/replaced by others.
        This could cause unexpected behaviour, so the `__setitem__` method checks if the object is still initializing,
        if the maximum size is exceeded after adding an element, and raises an error in that case.
        """
        self.max_bytes = max_bytes
        self._total_bytes = 0
        self.__initializing = True
        super().__init__(other, **kwargs)
        self.__initializing = False

    def __delitem__(self, key: Hashable) -> None:
        """Usage is `del instance[key]` as with other mapping classes."""
        self._total_bytes -= len(self[key])
        super().__delitem__(key)

    def __getitem__(self, key: Hashable) -> bytes:
        """Usage is `instance[key]` as with other mapping classes."""
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key: Hashable, value: bytes) -> None:
        """
        Calling `instance[key] = value` adds a key-value-pair as with other mapping classes.
        Only accepts `bytes` objects as values; raises a `TypeError` otherwise.
        If the length/size of the provided `value` exceeds `.max_bytes` a `SizeError` is raised.
        The internal `._total_bytes` attribute is updated.
        If the key existed before and just the value is replaced, the item is treated as most recently accessed and
        thus moved to the end of the internal linked list.
        If after adding the item `._total_bytes` exceeds `.max_bytes`, items are deleted in order from least to most
        recently accessed until the total size (in bytes) is in line with the specified maximum.
        """
        if not isinstance(value, bytes):
            raise TypeError("Not a bytes object:", repr(value))
        size = len(value)
        if size > self.max_bytes:
            # If we allowed this, the loop at the end would remove all items from the dictionary,
            # so we raise an error to allow exceptions for this case.
            raise SizeError(f"Item itself exceeds maximum allowed size of {self.max_bytes} bytes")
        # Update `_total_bytes` depending on whether the key existed already or not;
        # if so, calling `__getitem__` to determine the current size also ensures that the key is moved to the end.
        self._total_bytes += size - len(self[key]) if key in self else size
        # Now actually set the new value (and possibly new key):
        super().__setitem__(key, value)
        if self._total_bytes <= self.max_bytes:
            return
        if self.__initializing:
            raise SizeError(f"Total bytes of {self.max_bytes} exceeded")
        # If size is too large now, remove items until it is less than or equal to the defined maximum
        while self._total_bytes > self.max_bytes:
            # Delete the current oldest item, by instantiating an iterator over all keys (in order)
            # and passing its next item (i.e. the first one in order) to `__delitem__`
            del self[next(iter(self))]

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    @property
    def space_left(self) -> int:
        return self.max_bytes - self._total_bytes
