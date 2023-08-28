import logging
from asyncio.tasks import sleep
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from videbo.video import analyze


class AnalyzeTestCase(IsolatedAsyncioTestCase):
    def test_default_timeout(self) -> None:
        self.assertEqual(10., analyze.DEFAULT_SUBPROCESS_TIMEOUT)

    @patch.object(analyze.FileCmdError, "__init__", return_value=None)
    @patch.object(analyze, "create_user_subprocess")
    async def test_get_video_mime_type(
        self,
        mock_create_user_subprocess: AsyncMock,
        mock_file_cmd_error_init: MagicMock,
    ) -> None:
        async def mock_communicate() -> tuple[bytes, object]:
            await sleep(0.01)
            return b" FOO;bar", object()
        expected_output = "foo"
        mock_proc = MagicMock(communicate=mock_communicate)
        mock_create_user_subprocess.return_value = mock_proc

        path = "spam"
        sudo_user = "eggs"
        bin_file = "beans"
        output = await analyze.get_video_mime_type(
            path,
            sudo_user=sudo_user,
            binary_file=bin_file,
        )
        self.assertEqual(expected_output, output)
        mock_create_user_subprocess.assert_awaited_once_with(
            bin_file,
            "-b",
            "-i",
            path,
            sudo_user=sudo_user,
            stdout=analyze.PIPE,
        )
        mock_proc.kill.assert_not_called()
        mock_file_cmd_error_init.assert_not_called()

        mock_create_user_subprocess.reset_mock()

        # Induce timeout:
        with self.assertRaises(analyze.FileCmdError):
            await analyze.get_video_mime_type(path, timeout_seconds=0.001)
        mock_create_user_subprocess.assert_awaited_once_with(
            analyze.settings.video.binary_file,
            "-b",
            "-i",
            path,
            sudo_user=analyze.settings.video.check_user,
            stdout=analyze.PIPE,
        )
        mock_proc.kill.assert_called_once_with()
        mock_file_cmd_error_init.assert_called_once_with(timeout=True)

    @patch.object(analyze.VideoInfo, "parse_raw")
    @patch.object(analyze.FFProbeError, "__init__", return_value=None)
    @patch.object(analyze, "create_user_subprocess")
    async def test_get_ffprobe_info(
        self,
        mock_create_user_subprocess: AsyncMock,
        mock_ffprobe_error_init: MagicMock,
        mock_video_info_parse_raw: MagicMock,
    ) -> None:
        async def mock_communicate() -> tuple[bytes, bytes]:
            await sleep(0.01)
            return b"foo", b"bar"
        mock_proc = MagicMock(communicate=mock_communicate)
        mock_create_user_subprocess.return_value = mock_proc
        mock_video_info_parse_raw.return_value = expected_output = object()

        path = "spam"
        sudo_user = "eggs"
        bin_ffprobe = "beans"
        output = await analyze.get_ffprobe_info(
            path,
            sudo_user=sudo_user,
            binary_ffprobe=bin_ffprobe,
        )
        self.assertIs(expected_output, output)
        mock_create_user_subprocess.assert_awaited_once_with(
            bin_ffprobe,
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            path,
            sudo_user=sudo_user,
            stdout=analyze.PIPE,
            stderr=analyze.PIPE,
        )
        mock_proc.kill.assert_not_called()
        mock_ffprobe_error_init.assert_not_called()
        mock_video_info_parse_raw.assert_called_once_with("foo")

        mock_create_user_subprocess.reset_mock()
        mock_video_info_parse_raw.reset_mock()

        # Induce timeout:
        with self.assertRaises(analyze.FFProbeError):
            await analyze.get_ffprobe_info(path, timeout_seconds=0.001)
        mock_create_user_subprocess.assert_awaited_once_with(
            analyze.settings.video.binary_ffprobe,
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            path,
            sudo_user=analyze.settings.video.check_user,
            stdout=analyze.PIPE,
            stderr=analyze.PIPE,
        )
        mock_proc.kill.assert_called_once_with()
        mock_ffprobe_error_init.assert_called_once_with(timeout=True)
        mock_video_info_parse_raw.assert_not_called()

        mock_create_user_subprocess.reset_mock()
        mock_proc.kill.reset_mock()
        mock_ffprobe_error_init.reset_mock()

        # Error while parsing:
        err = analyze.ValidationError(MagicMock(), MagicMock)
        mock_video_info_parse_raw.side_effect = err
        with self.assertRaises(analyze.FFProbeError):
            await analyze.get_ffprobe_info(path)
        mock_create_user_subprocess.assert_awaited_once_with(
            analyze.settings.video.binary_ffprobe,
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            path,
            sudo_user=analyze.settings.video.check_user,
            stdout=analyze.PIPE,
            stderr=analyze.PIPE,
        )
        mock_proc.kill.assert_not_called()
        mock_ffprobe_error_init.assert_called_once_with(stderr="bar")
        mock_video_info_parse_raw.assert_called_once_with("foo")

    @patch.object(analyze, "get_ffprobe_info")
    @patch.object(analyze.MimeTypeNotAllowed, "__init__", return_value=None)
    @patch.object(analyze, "settings")
    @patch.object(analyze, "get_video_mime_type")
    async def test_get_video_info(
        self,
        mock_get_video_mime_type: AsyncMock,
        mock_settings: MagicMock,
        mock_mime_type_not_allowed_init: MagicMock,
        mock_get_ffprobe_info: AsyncMock,
    ) -> None:
        # Fail due to wrong mime type:
        mock_settings.video.mime_types_allowed = {"foo", "bar"}
        mock_get_video_mime_type.return_value = mime_type = "baz"
        path = "abc"
        with self.assertRaises(analyze.MimeTypeNotAllowed):
            with self.assertLogs(analyze._log, logging.WARNING):
                await analyze.get_video_info(path)
        mock_get_video_mime_type.assert_called_once_with(path)
        mock_mime_type_not_allowed_init.assert_called_once_with(mime_type)
        mock_get_ffprobe_info.assert_not_called()

        mock_get_video_mime_type.reset_mock()
        mock_mime_type_not_allowed_init.reset_mock()

        mock_get_video_mime_type.return_value = "foo"
        ffprobe_err = analyze.FFProbeError()
        vid_not_allowed = analyze.VideoNotAllowed()
        mock_info = MagicMock()
        mock_get_ffprobe_info.side_effect = (ffprobe_err, vid_not_allowed, mock_info)

        # Fail due to ffprobe error:
        with self.assertRaises(analyze.FFProbeError) as ctx:
            with self.assertLogs(analyze._log, logging.WARNING):
                await analyze.get_video_info(path)
        self.assertIs(ffprobe_err, ctx.exception)
        mock_get_video_mime_type.assert_called_once_with(path)
        mock_mime_type_not_allowed_init.assert_not_called()
        mock_get_ffprobe_info.assert_called_once_with(path)

        mock_get_video_mime_type.reset_mock()
        mock_get_ffprobe_info.reset_mock()

        # Fail due to video not allowed:
        with self.assertRaises(analyze.VideoNotAllowed) as ctx:
            with self.assertLogs(analyze._log, logging.WARNING):
                await analyze.get_video_info(path)
        self.assertIs(vid_not_allowed, ctx.exception)
        mock_get_video_mime_type.assert_called_once_with(path)
        mock_mime_type_not_allowed_init.assert_not_called()
        mock_get_ffprobe_info.assert_called_once_with(path)

        mock_get_video_mime_type.reset_mock()
        mock_get_ffprobe_info.reset_mock()

        # Succeed:
        output = await analyze.get_video_info(path)
        self.assertIs(mock_info, output)
        mock_get_video_mime_type.assert_called_once_with(path)
        mock_mime_type_not_allowed_init.assert_not_called()
        mock_get_ffprobe_info.assert_called_once_with(path)

    @patch.object(analyze.FFMpegError, "__init__", return_value=None)
    @patch.object(analyze, "create_user_subprocess")
    async def test_create_thumbnail(
        self,
        mock_create_user_subprocess: AsyncMock,
        mock_ffmpeg_error_init: MagicMock,
    ) -> None:
        async def mock_wait() -> None:
            await sleep(0.01)
        mock_proc = MagicMock(wait=mock_wait)
        mock_create_user_subprocess.return_value = mock_proc

        video_path = "spam"
        thumbnail_dst = "eggs"
        offset = 1
        height = 2
        sudo_user = "beans"
        bin_ffmpeg = "toast"
        self.assertIsNone(await analyze.create_thumbnail(
            video_path,
            thumbnail_dst,
            offset,
            height,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
        ))
        mock_create_user_subprocess.assert_awaited_once_with(
            bin_ffmpeg,
            "-ss",
            str(offset),
            "-i",
            str(video_path),
            "-vframes",
            "1",
            "-an",
            "-vf",
            f"scale=-1:{height}",
            "-y",
            str(thumbnail_dst),
            sudo_user=sudo_user,
        )
        mock_proc.kill.assert_not_called()
        mock_ffmpeg_error_init.assert_not_called()

        mock_create_user_subprocess.reset_mock()

        # Induce timeout:
        with self.assertRaises(analyze.FFMpegError):
            await analyze.create_thumbnail(
                video_path,
                thumbnail_dst,
                offset,
                height,
                timeout_seconds=0.001,
            )
        mock_create_user_subprocess.assert_awaited_once_with(
            analyze.settings.video.binary_ffmpeg,
            "-ss",
            str(offset),
            "-i",
            str(video_path),
            "-vframes",
            "1",
            "-an",
            "-vf",
            f"scale=-1:{height}",
            "-y",
            str(thumbnail_dst),
            sudo_user=analyze.settings.video.check_user,
        )
        mock_proc.kill.assert_called_once_with()
        mock_ffmpeg_error_init.assert_called_once_with(timeout=True)

    @patch.object(analyze, "Path", autospec=True)
    @patch.object(analyze, "run_in_default_executor")
    @patch.object(analyze, "create_thumbnail")
    async def test_create_thumbnail_securely(
        self,
        mock_create_thumbnail: AsyncMock,
        mock_run_in_default_executor: AsyncMock,
        mock_path_cls: MagicMock,
    ) -> None:
        video_path = "spam"
        thumbnail_dst = "eggs"
        offset = 1
        height = 2
        interim_path = "sausage"
        sudo_user = "beans"
        bin_ffmpeg = "toast"
        timeout = 3
        self.assertIsNone(await analyze.create_thumbnail_securely(
            video_path,
            thumbnail_dst,
            offset,
            height,
            interim_path,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
        ))
        mock_create_thumbnail.assert_awaited_once_with(
            video_path,
            interim_path,
            offset,
            height,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
        )
        mock_path_cls.assert_called_once_with(interim_path)
        self.assertListEqual(
            [
                call(analyze.copy_file, interim_path, thumbnail_dst),
                call(mock_path_cls().unlink, True),
            ],
            mock_run_in_default_executor.await_args_list,
        )

    @patch.object(analyze, "create_thumbnail_securely")
    @patch.object(analyze, "create_thumbnail")
    async def test_generate_thumbnails(
        self,
        mock_create_thumbnail: AsyncMock,
        mock_create_thumbnail_securely: AsyncMock,
    ) -> None:
        video_path = "spam"
        video_duration = 3.14
        interim_dir = "eggs"
        height = 2
        count = 1  # one iteration
        sudo_user = "beans"
        bin_ffmpeg = "toast"
        timeout = 420

        expected_destination = Path(f"{video_path}_0.jpg")
        expected_offset = int(video_duration * 0.5)

        # Without interim paths:
        output = await analyze.generate_thumbnails(
            video_path,
            video_duration,
            height=height,
            count=count,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
        )
        self.assertEqual(count, output)
        mock_create_thumbnail.assert_awaited_once_with(
            video_path=Path(video_path),
            height=height,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
            thumbnail_dst=expected_destination,
            offset=expected_offset,
        )
        mock_create_thumbnail_securely.assert_not_called()

        mock_create_thumbnail.reset_mock()

        # With interim paths:
        output = await analyze.generate_thumbnails(
            video_path,
            video_duration,
            interim_dir=interim_dir,  # <---
            height=height,
            count=count,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
        )
        self.assertEqual(count, output)
        mock_create_thumbnail.assert_not_called()
        mock_create_thumbnail_securely.assert_awaited_once_with(
            video_path=Path(video_path),
            height=height,
            sudo_user=sudo_user,
            binary_ffmpeg=bin_ffmpeg,
            timeout_seconds=timeout,
            thumbnail_dst=expected_destination,
            offset=expected_offset,
            interim_path=Path(interim_dir, expected_destination.name),
        )
