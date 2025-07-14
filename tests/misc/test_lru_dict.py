from unittest import TestCase
from collections import OrderedDict
from string import ascii_lowercase

from videbo.exceptions import SizeError
from videbo.misc.lru_dict import BytesLimitLRU


class BytesLimitLRUTestCase(TestCase):
    MAX_BYTES = 100
    BYTES_PER_ITEM = 15
    NUM_ITEMS = 6
    ITEMS_BYTES = BYTES_PER_ITEM * NUM_ITEMS
    ITEMS_LIST: list[tuple[str, bytes]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.ITEMS_LIST = [(char, cls.BYTES_PER_ITEM * bytes(char, 'utf8')) for char in ascii_lowercase[:cls.NUM_ITEMS]]

    def setUp(self) -> None:
        self.obj = BytesLimitLRU(self.MAX_BYTES, self.ITEMS_LIST)

    def test_init(self) -> None:
        self.assertDictEqual(OrderedDict(self.ITEMS_LIST), self.obj)
        self.assertEqual(self.MAX_BYTES, self.obj.max_bytes)
        with self.assertRaises(TypeError):
            BytesLimitLRU(
                self.MAX_BYTES,
                foo=b"foo",
                bar="Not a `bytes` object; should throw an error",
            )

    def test_init_sizes(self) -> None:
        BytesLimitLRU(3, a=b'foo')
        with self.assertRaises(SizeError):
            BytesLimitLRU(3, a=b'foo', b=b'b')
        BytesLimitLRU(3, a=b'a', b=b'b', c=b'c')
        with self.assertRaises(SizeError):
            BytesLimitLRU(3, a=b'a', b=b'b', c=b'c', d=b'd')

    def test_total_bytes(self) -> None:
        self.assertEqual(self.ITEMS_BYTES, self.obj.total_bytes)
        self.assertEqual(self.ITEMS_BYTES, self.obj._total_bytes)

    def test_space_left(self) -> None:
        self.assertEqual(self.MAX_BYTES - self.ITEMS_BYTES, self.obj.space_left)

    def test_delitem(self) -> None:
        del self.obj[self.ITEMS_LIST[-1][0]]  # delete last item
        self.assertDictEqual(OrderedDict(self.ITEMS_LIST[:-1]), self.obj)
        self.assertEqual(self.ITEMS_BYTES - self.BYTES_PER_ITEM, self.obj._total_bytes)
        del self.obj[self.ITEMS_LIST[0][0]]  # delete first item
        self.assertDictEqual(OrderedDict(self.ITEMS_LIST[1:-1]), self.obj)
        self.assertEqual(self.ITEMS_BYTES - 2 * self.BYTES_PER_ITEM, self.obj._total_bytes)
        with self.assertRaises(KeyError):
            del self.obj['keydoesnotexit']
        self.assertEqual(self.ITEMS_BYTES - 2 * self.BYTES_PER_ITEM, self.obj._total_bytes)

    def test_getitem(self) -> None:
        key, value = self.ITEMS_LIST[0]  # The item that was added first to the object
        self.assertEqual(value, self.obj[key])
        # Check that the requested item was moved to the end.
        # To avoid the item getter method, we create and consume an iterator over the keys instead
        # and verify that the key that was requested before is the last item in the resulting sequence.
        self.assertEqual(key, tuple(iter(self.obj))[-1])

    def test_setitem(self) -> None:
        def not_bytes():
            return 1 if True else b'1'  # this is just a workaround, so PyCharm doesn't scream at me
        with self.assertRaises(TypeError):
            self.obj[self.ITEMS_LIST[0][0]] = not_bytes()
        with self.assertRaises(SizeError):
            self.obj['foo'] = b'1' * (self.MAX_BYTES + 1)
        key, value = self.ITEMS_LIST[0]  # The item that was added first to the object
        new_value = len(value) * b'A'  # make the new value the same number of bytes#
        # Call the setter:
        self.obj[key] = new_value
        # Ensure the correct set of key-value-pairs:
        self.assertDictEqual(OrderedDict([*self.ITEMS_LIST[1:], (key, new_value)]), self.obj)
        # As in the test of the getter method, we ensure the new item is in fact at the end now:
        self.assertEqual(key, tuple(iter(self.obj))[-1])
        # Ensure the size has not changed:
        self.assertEqual(self.ITEMS_BYTES, self.obj.total_bytes)
        # Try with a new key that also forces all other items out:
        new_key, new_value = 'B', b'B' * (self.MAX_BYTES - 1)
        self.obj[new_key] = new_value
        # Check that the new mapping indeed only contains the new item and the size was updated correctly:
        self.assertDictEqual(OrderedDict([(new_key, new_value)]), self.obj)
        self.assertEqual(self.MAX_BYTES - 1, self.obj.total_bytes)
