from unittest import TestCase

from videbo import exceptions


class SubprocessErrorTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.SubprocessError(timeout=True, stderr="abc")
        self.assertTrue(obj.timeout)
        self.assertEqual("abc", obj.stderr)


class InvalidMimeTypeErrorTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.InvalidMimeTypeError(mime_type="abc")
        self.assertEqual("abc", obj.mime_type)


class InvalidVideoErrorTestCase(TestCase):
    def test___init__(self) -> None:
        obj = exceptions.InvalidVideoError(
            container="a",
            video_codec="b",
            audio_codec="c",
        )
        self.assertEqual("a", obj.container)
        self.assertEqual("b", obj.video_codec)
        self.assertEqual("c", obj.audio_codec)
