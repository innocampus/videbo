from typing import TypeVar
from unittest import TestCase

from videbo.misc.generic_insight import GenericInsightMixin

_T = TypeVar("_T")


class GenericInsightMixinTestCase(TestCase):
    def test(self) -> None:
        with self.assertRaises(AttributeError):
            GenericInsightMixin.get_type_arg()

        class Foo:
            pass

        class Bar(GenericInsightMixin[_T], Foo):
            pass

        class Baz(GenericInsightMixin[int], Foo):
            pass

        with self.assertRaises(AttributeError):
            Bar.get_type_arg()

        self.assertIs(int, Baz.get_type_arg())
