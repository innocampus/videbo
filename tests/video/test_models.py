import logging
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from videbo.video import models


class FFProbeStreamTestCase(TestCase):
    @patch.object(models, "settings")
    def test_ensure_whitelisted_codec(self, mock_settings: MagicMock) -> None:
        mock_settings.video.video_codecs_allowed = {"foo", "bar"}
        mock_settings.video.audio_codecs_allowed = {"spam", "eggs"}
        obj = models.FFProbeStream(
            index=1,
            codec_name="foo",
            codec_type=models.CodecType.video,
        )
        self.assertEqual("foo", obj.codec_name)
        with self.assertRaises(ValidationError):
            models.FFProbeStream(
                index=1,
                codec_name="not whitelisted name",
                codec_type=models.CodecType.video,
            )

        obj = models.FFProbeStream(
            index=1,
            codec_name="eggs",
            codec_type=models.CodecType.audio,
        )
        self.assertEqual("eggs", obj.codec_name)
        with self.assertRaises(ValidationError):
            models.FFProbeStream(
                index=1,
                codec_name="not whitelisted name",
                codec_type=models.CodecType.audio,
            )

        obj = models.FFProbeStream(
            index=1,
            codec_type=models.CodecType.data,
        )
        self.assertEqual(None, obj.codec_name)


class FFProbeFormatTestCase(TestCase):
    @patch.object(models, "settings")
    def test_split_str(self, mock_settings: MagicMock) -> None:
        mock_settings.video.container_formats_allowed = {"c", "d"}
        data = {
            "filename": "foo",
            "nb_streams": 1,
            "format_name": "a,b,c",
            "size": 2,
            "bit_rate": 3,
        }
        obj = models.FFProbeFormat.parse_obj(data)
        self.assertListEqual(data["format_name"].split(","), obj.format_name)

    @patch.object(models, "settings")
    def test_ensure_whitelisted_format(self, mock_settings: MagicMock) -> None:
        mock_settings.video.container_formats_allowed = {"a", "b"}
        with self.assertRaises(ValidationError):
            models.FFProbeFormat(
                filename=Path("foo"),
                nb_streams=1,
                format_name=["c"],
                size=2,
                bit_rate=3,
            )

    @patch.object(models, "settings")
    def test_names(self, mock_settings: MagicMock) -> None:
        mock_settings.video.container_formats_allowed = {"a", "b"}
        obj = models.FFProbeFormat(
            filename=Path("foo"),
            nb_streams=1,
            format_name=["a"],
            size=2,
            bit_rate=3,
        )
        self.assertEqual(obj.format_name, obj.names)


class VideoInfoTestCase(TestCase):
    @patch.object(models, "settings")
    def test_get_consistent_file_ext(
        self,
        mock_settings: MagicMock,
    ) -> None:
        mock_settings.video.video_codecs_allowed = {"spam", "eggs"}
        mock_settings.video.container_formats_allowed = {"foo", "bar", "webm", "mp4"}
        stream = models.FFProbeStream(
            index=1,
            codec_name="spam",
            codec_type=models.CodecType.video,
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc.baz"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        video_info = models.VideoInfo(streams=[stream], format=fmt)

        # Extension does not match one of the format names;
        # should choose one of them instead:
        with self.assertLogs(models._log, logging.WARNING):
            ext = video_info.get_consistent_file_ext()
        self.assertIn(ext[1:], fmt.format_name)

        fmt.format_name.append("baz")
        # Extension matches one of the format names;
        # should return that:
        ext = video_info.get_consistent_file_ext()
        self.assertIn(".baz", ext)

        fmt.format_name.append("webm")
        # Normalization should pick `.webm`:
        ext = video_info.get_consistent_file_ext()
        self.assertIn(".webm", ext)

        fmt.format_name.append("mp4")
        # Normalization should pick `.mp4`:
        ext = video_info.get_consistent_file_ext()
        self.assertIn(".mp4", ext)

        # Without normalization it should return `.baz` again:
        ext = video_info.get_consistent_file_ext(normalize=False)
        self.assertIn(".baz", ext)

    @patch.object(models, "settings")
    def test_ensure_video_stream_is_present(
        self,
        mock_settings: MagicMock,
    ) -> None:
        mock_settings.video.video_codecs_allowed = {"spam", "eggs"}
        mock_settings.video.audio_codecs_allowed = {"a", "b"}
        mock_settings.video.container_formats_allowed = {"foo", "bar"}
        video_stream = models.FFProbeStream(
            index=0,
            codec_name="spam",
            codec_type=models.CodecType.video,
        )
        audio_stream = models.FFProbeStream(
            index=1,
            codec_name="a",
            codec_type=models.CodecType.audio,
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc.foo"),
            nb_streams=1,
            format_name=["foo", "bar"],
            size=2,
            bit_rate=3,
        )
        obj = models.VideoInfo(
            streams=[video_stream, audio_stream],
            format=fmt,
            file_ext=".foo",
        )
        self.assertIn(video_stream, obj.streams)

        with self.assertRaises(ValidationError):
            models.VideoInfo(
                streams=[audio_stream],
                format=fmt,
                file_ext=".foo",
            )

    @patch.object(models, "settings")
    def test_get_duration(self, mock_settings: MagicMock) -> None:
        mock_settings.video.video_codecs_allowed = {"spam", "eggs"}
        mock_settings.video.container_formats_allowed = {"foo", "bar"}
        stream = models.FFProbeStream(
            index=0,
            codec_name="spam",
            codec_type=models.CodecType.video,
        )
        fmt = models.FFProbeFormat(
            filename=Path("abc.foo"),
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
