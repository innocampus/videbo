from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from pydantic import ValidationError

from videbo.video import models


class FFProbeStreamTestCase(TestCase):
    @patch.object(models, "settings")
    def test_is_allowed(self, mock_settings: MagicMock) -> None:
        mock_settings.video.video_codecs_allowed = {"foo", "bar"}
        mock_settings.video.audio_codecs_allowed = {"spam", "eggs"}
        obj = models.FFProbeStream(
            index=1,
            codec_name="foo",
            codec_type="video",
        )
        self.assertTrue(obj.is_allowed())
        obj.codec_name = "baz"
        self.assertFalse(obj.is_allowed())

        obj = models.FFProbeStream(
            index=1,
            codec_name="eggs",
            codec_type="audio",
        )
        self.assertTrue(obj.is_allowed())
        obj.codec_name = "beans"
        self.assertFalse(obj.is_allowed())

        obj = models.FFProbeStream(
            index=1,
            codec_name="whatever",
            codec_type="something weird",
        )
        self.assertTrue(obj.is_allowed())


class FFProbeFormatTestCase(TestCase):
    def test_split_str(self) -> None:
        data = {
            "filename": "foo",
            "nb_streams": 1,
            "format_name": "a,b,c",
            "size": 2,
            "bit_rate": 3,
        }
        obj = models.FFProbeFormat.parse_obj(data)
        self.assertListEqual(data["format_name"].split(","), obj.format_name)

    def test_names(self) -> None:
        obj = models.FFProbeFormat(
            filename=Path("foo"),
            nb_streams=1,
            format_name=["a"],
            size=2,
            bit_rate=3,
        )
        self.assertEqual(obj.format_name, obj.names)

    def test_get_suggested_file_extension(self) -> None:
        obj = models.FFProbeFormat(
            filename=Path("foo"),
            nb_streams=1,
            format_name=["mp4"],
            size=2,
            bit_rate=3,
        )
        output = obj.get_suggested_file_extension()
        self.assertEqual(".mp4", output)

        obj.format_name = ["webm"]
        output = obj.get_suggested_file_extension()
        self.assertEqual(".webm", output)

        obj.format_name = ["foo", "bar"]
        with self.assertRaises(ValueError):
            obj.get_suggested_file_extension()

    @patch.object(models, "settings")
    def test_is_allowed(self, mock_settings: MagicMock) -> None:
        mock_settings.video.container_formats_allowed = {"foo", "bar"}
        obj = models.FFProbeFormat(
            filename=Path("foo"),
            nb_streams=1,
            format_name=["bar", "baz"],
            size=2,
            bit_rate=3,
        )
        self.assertTrue(obj.is_allowed())

        obj.format_name = ["spam", "eggs"]
        self.assertFalse(obj.is_allowed())


class VideoInfoTestCase(TestCase):
    @patch.object(models.FFProbeFormat, "get_suggested_file_extension")
    def test_get_file_ext(
        self,
        mock_get_suggested_file_extension: MagicMock,
    ) -> None:
        mock_get_suggested_file_extension.return_value = ext = "baz"
        stream = models.FFProbeStream(
            index=1,
            codec_name="spam",
            codec_type="eggs",
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        obj = models.VideoInfo(streams=[stream], format=fmt)
        self.assertEqual(ext, obj.file_ext)
        mock_get_suggested_file_extension.assert_called_once_with()

        mock_get_suggested_file_extension.reset_mock()

        # Extension with dot matches format names:
        ext = ".bar"
        obj = models.VideoInfo(streams=[stream], format=fmt, file_ext=ext)
        self.assertEqual(ext, obj.file_ext)

        # Extension without dot matches format names:
        ext = "foo"
        obj = models.VideoInfo(streams=[stream], format=fmt, file_ext=ext)
        self.assertEqual("." + ext, obj.file_ext)

        # Inconsistent:
        ext = "xyz"
        with self.assertRaises(ValidationError):
            models.VideoInfo(streams=[stream], format=fmt, file_ext=ext)

    def test_get_first_stream_of_type(self) -> None:
        stream_0 = models.FFProbeStream(
            index=0,
            codec_name="spam",
            codec_type="eggs",
        )
        stream_1 = models.FFProbeStream(
            index=1,
            codec_name="x",
            codec_type="y",
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        obj = models.VideoInfo(streams=[stream_0, stream_1], format=fmt, file_ext="foo")
        output = obj.get_first_stream_of_type("y")
        self.assertEqual(stream_1, output)

        output = obj.get_first_stream_of_type("something else")
        self.assertIsNone(output)

    @patch.object(models.AudioCodecNotAllowed, "__init__", return_value=None)
    @patch.object(models.VideoCodecNotAllowed, "__init__", return_value=None)
    @patch.object(models.ContainerFormatNotAllowed, "__init__", return_value=None)
    @patch.object(models.VideoInfo, "get_first_stream_of_type")
    @patch.object(models.FFProbeStream, "is_allowed")
    @patch.object(models.FFProbeFormat, "is_allowed")
    def test_ensure_is_allowed(
        self,
        mock_format_is_allowed: MagicMock,
        mock_stream_is_allowed: MagicMock,
        mock_get_first_stream_of_type: MagicMock,
        mock_container_format_not_allowed_init: MagicMock,
        mock_video_codec_not_allowed_init: MagicMock,
        mock_audio_codec_not_allowed_init: MagicMock,
    ) -> None:
        stream = models.FFProbeStream(
            index=1,
            codec_name="spam",
            codec_type="eggs",
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        obj = models.VideoInfo(streams=[stream], format=fmt, file_ext=".foo")

        # Bad format:
        mock_format_is_allowed.return_value = False
        with self.assertRaises(models.ContainerFormatNotAllowed):
            obj.ensure_is_allowed()
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_not_called()
        mock_get_first_stream_of_type.assert_not_called()
        mock_container_format_not_allowed_init.assert_called_once_with(fmt.names)
        mock_video_codec_not_allowed_init.assert_not_called()
        mock_audio_codec_not_allowed_init.assert_not_called()

        mock_format_is_allowed.reset_mock()
        mock_container_format_not_allowed_init.reset_mock()

        # No video stream:
        mock_format_is_allowed.return_value = True
        mock_get_first_stream_of_type.return_value = None
        with self.assertRaises(models.VideoCodecNotAllowed):
            obj.ensure_is_allowed()
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_not_called()
        mock_get_first_stream_of_type.assert_called_once_with("video")
        mock_container_format_not_allowed_init.assert_not_called()
        mock_video_codec_not_allowed_init.assert_called_once_with(None)
        mock_audio_codec_not_allowed_init.assert_not_called()

        mock_format_is_allowed.reset_mock()
        mock_get_first_stream_of_type.reset_mock()
        mock_video_codec_not_allowed_init.reset_mock()

        # Bad video stream:
        mock_get_first_stream_of_type.return_value = stream
        mock_stream_is_allowed.return_value = False
        with self.assertRaises(models.VideoCodecNotAllowed):
            obj.ensure_is_allowed()
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_called_once_with()
        mock_get_first_stream_of_type.assert_called_once_with("video")
        mock_container_format_not_allowed_init.assert_not_called()
        mock_video_codec_not_allowed_init.assert_called_once_with(stream.codec_name)
        mock_audio_codec_not_allowed_init.assert_not_called()

        mock_format_is_allowed.reset_mock()
        mock_stream_is_allowed.reset_mock()
        mock_get_first_stream_of_type.reset_mock()
        mock_video_codec_not_allowed_init.reset_mock()

        # Bad audio stream:
        mock_stream_is_allowed.side_effect = (True, False)  # pass video check
        with self.assertRaises(models.AudioCodecNotAllowed):
            obj.ensure_is_allowed()
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_has_calls([call(), call()])
        mock_get_first_stream_of_type.assert_has_calls([call("video"), call("audio")])
        mock_container_format_not_allowed_init.assert_not_called()
        mock_video_codec_not_allowed_init.assert_not_called()
        mock_audio_codec_not_allowed_init.assert_called_once_with(stream.codec_name)

        mock_format_is_allowed.reset_mock()
        mock_stream_is_allowed.reset_mock()
        mock_get_first_stream_of_type.reset_mock()
        mock_audio_codec_not_allowed_init.reset_mock()

        # No audio stream: (passes)
        mock_stream_is_allowed.side_effect = (True, False)  # pass video check
        mock_get_first_stream_of_type.side_effect = (stream, None)  # video normal
        self.assertIsNone(obj.ensure_is_allowed())
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_called_once_with()
        mock_get_first_stream_of_type.assert_has_calls([call("video"), call("audio")])
        mock_container_format_not_allowed_init.assert_not_called()
        mock_video_codec_not_allowed_init.assert_not_called()
        mock_audio_codec_not_allowed_init.assert_not_called()

        mock_format_is_allowed.reset_mock()
        mock_stream_is_allowed.reset_mock()
        mock_get_first_stream_of_type.reset_mock()

        # Valid:
        mock_stream_is_allowed.side_effect = (True, True)  # pass video check
        mock_get_first_stream_of_type.side_effect = (stream, stream)
        self.assertIsNone(obj.ensure_is_allowed())
        mock_format_is_allowed.assert_called_once_with()
        mock_stream_is_allowed.assert_has_calls([call(), call()])
        mock_get_first_stream_of_type.assert_has_calls([call("video"), call("audio")])
        mock_container_format_not_allowed_init.assert_not_called()
        mock_video_codec_not_allowed_init.assert_not_called()
        mock_audio_codec_not_allowed_init.assert_not_called()

    def test_get_duration(self) -> None:
        stream = models.FFProbeStream(
            index=0,
            codec_name="spam",
            codec_type="eggs",
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        obj = models.VideoInfo(streams=[stream], format=fmt, file_ext=".foo")
        self.assertEqual(0., obj.get_duration())

        obj.format.duration = duration = 3.14
        self.assertEqual(duration, obj.get_duration())

        obj.streams[0].duration = duration = 420
        self.assertEqual(duration, obj.get_duration())

    def test_content_type_for(self) -> None:
        self.assertEqual("video/mp4", models.VideoInfo.content_type_for(".mp4"))
        self.assertEqual("video/webm", models.VideoInfo.content_type_for(".webm"))
        with self.assertRaises(ValueError):
            models.VideoInfo.content_type_for(".abc")
