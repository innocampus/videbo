from unittest import TestCase

from videbo import exceptions


class SubprocessErrorTestCase(TestCase):
    def test___init__(self) -> None:
        timeout, stderr = True, "abc"
        exc = exceptions.SubprocessError(timeout=timeout, stderr=stderr)
        self.assertTrue(exc.timeout)
        self.assertEqual(stderr, exc.stderr)
        self.assertEqual(f"{timeout=}; stderr={stderr}", exc.args[0])

        timeout = False
        exc = exceptions.SubprocessError(timeout=timeout)
        self.assertFalse(exc.timeout)
        self.assertEqual(None, exc.stderr)
        self.assertEqual(f"{timeout=}", exc.args[0])


class MimeTypeNotAllowedTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.MimeTypeNotAllowed(mime_type="abc")
        self.assertEqual("abc", obj.mime_type)


class ContainerFormatNotAllowedTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.ContainerFormatNotAllowed(formats=("abc", "xyz"))
        self.assertListEqual(["abc", "xyz"], obj.formats)


class CodecNotAllowedTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.CodecNotAllowed(codec="abc")
        self.assertEqual("abc", obj.codec)
