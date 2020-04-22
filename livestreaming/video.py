import asyncio
import json
import os
import shutil
from typing import Dict
from typing import List
from typing import Optional
from livestreaming.storage import storage_settings

MIME_TYPE_WHITELIST = ['video/mp4', 'video/webm']
CONTAINER_WHITELIST = ['mp4', 'webm']
VIDEO_CODEC_WHITELIST = ['h264', 'vp8']
AUDIO_CODEC_WHITELIST = ['aac', 'vorbis', 'mp3']


class VideoConfig:

    def __init__(self, settings):
        self.user =  settings.check_user
        self.binary_ffmpeg = settings.binary_ffmpeg
        self.binary_ffprobe = settings.binary_ffprobe
        self.binary_file = settings.binary_file
        self.video_check_user = settings.check_user

    async def create_sudo_subprocess(self, args: List[str], binary: str = None, stdout: int = asyncio.subprocess.DEVNULL,
                                     stderr: int = asyncio.subprocess.DEVNULL) -> asyncio.subprocess.Process:
        if binary == "file":
            program = self.binary_file
        elif binary == "ffprobe":
            program = self.binary_ffprobe
        else:
            raise UnknownProgramError()

        # Run as different user
        if self.user:
            program = "sudo"
            args = ["-u", self.user, binary] + args

        return await asyncio.create_subprocess_exec(program, *args, stdout=stdout, stderr=stderr)


class VideoInfo:
    """Wrapper for the ffprobe application to get information about a video file."""

    def __init__(self, video_file: str, video_config: VideoConfig):
        self.video_file = video_file
        self.valid_video = False
        self._video_config = video_config
        self.mime_type = None
        self.streams: List[Dict] = []
        self.format: Optional[Dict] = None

    async def fetch_mime_type(self) -> None:
        """Call file and fetch mime type."""

        args = ["-b", "-i", self.video_file]
        # Run file command
        proc = await self._video_config.create_sudo_subprocess(args = args, binary = "file",  stdout=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
            self.mime_type = stdout.decode().strip().split(';')[0]  # strip linebreaks, extract first part
        except asyncio.TimeoutError:
            proc.kill()
            raise FileCmdError(True)

    async def fetch_info(self) -> None:
        """Call ffprobe and fetch information."""

        args = ["-show_format", "-show_streams", "-print_format", "json", self.video_file]
        # Run ffprobe
        proc = await self._video_config.create_sudo_subprocess(args = args, binary = "ffprobe",  stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE)
        err = None
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
            err = stderr.decode()
            data = json.loads(stdout.decode())
            self.streams = data["streams"]
            self.format = data["format"]
        except asyncio.TimeoutError:
            proc.kill()
            raise FFProbeError(True, stderr=err)
        except KeyError:
            raise FFProbeError(False, stderr=err)

    def get_one_stream_type(self, codec_type: str) -> Optional[Dict]:
        """Get one stream in video of a certain type (video, audio)"""
        for stream in self.streams:
            if stream["codec_type"] == codec_type:
                return stream

        return None

    def get_length(self) -> float:
        """Get the length/duration of the video."""
        max_duration = 0.0
        # Get duration of longest stream
        for stream in self.streams:
            if "duration" in stream:
                duration = float(stream["duration"])
                if duration == duration:  # Skip NaNs
                    max_duration = max(duration, max_duration)

        if "duration" in self.format:
            duration = float(self.format["duration"])
            if duration == duration:  # Skip NaNs
                max_duration = max(duration, max_duration)

        return max_duration

    def get_file_format_container(self) -> List[str]:
        """Return the container formats as list."""
        formats = self.format.get("format_name", "")  # type: str
        return formats.split(',')

    def get_suggested_file_extension(self) -> str:
        """Get a reasonable file extension."""
        formats = self.get_file_format_container()
        if "mp4" in formats:
            return ".mp4"
        if "webm" in formats:
            return ".webm"

        raise InvalidVideoError()


class VideoValidator:
    def __init__(self, info: VideoInfo):
        self._info = info

    def check_valid_mime_type(self) -> Optional[bool]:
        """Check if this is a valid, whitelisted video file type."""
        if self._info.mime_type.lower() in MIME_TYPE_WHITELIST:
            return True
        raise InvalidMimeTypeError(self._info.mime_type)

    def check_valid_video(self) -> None:
        """Check if this a video whose video/audio codec and container format are on our whitelists."""
        valid_format = False
        valid_video = False
        valid_audio = False
        video_codec = ''
        audio_codec = ''

        # Check container format, for some reasons ffprobe returns multiple format names.
        formats = self._info.get_file_format_container()
        for file_format in formats:
            if file_format in CONTAINER_WHITELIST:
                valid_format = True

        video_stream = self._info.get_one_stream_type("video")
        if video_stream:
            video_codec = video_stream["codec_name"]
            valid_video = video_codec in VIDEO_CODEC_WHITELIST

        audio_stream = self._info.get_one_stream_type("audio")
        if audio_stream:
            audio_codec = audio_stream["codec_name"]
            valid_audio = audio_codec in AUDIO_CODEC_WHITELIST
        else:
            # Accept videos without an audio stream.
            valid_audio = True

        if not valid_format or not valid_video or not valid_audio:
            raise InvalidVideoError(','.join(formats), video_codec, audio_codec)


class Video:
    def __init__(self, video_config: VideoConfig):
        self.video_config = video_config

    async def save_thumbnail(self, video_in: str, thumbnail_out: str, offset: int, height: int, temp_output_file: str = None) -> None:
        """Call ffmpeg and save scaled frame at specified offset."""
        output_file = thumbnail_out
        if temp_output_file is not None:
            output_file = temp_output_file

        args = ["-ss", str(offset), "-i", video_in, "-vframes", "1", "-an", "-vf",
                "scale=-1:{0}".format(height), "-y", output_file]
        # Run ffmpeg
        if self.video_config.user:
            args = ["-u", self.video_config.user, self.video_config.binary_ffmpeg] + args
            binary = "sudo"
        else:
            binary = self.video_config.binary_ffmpeg
        proc = await asyncio.create_subprocess_exec(binary, *args, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.wait(), 10)
            if proc.returncode != 0:
                raise FFMpegError(False)

            if output_file != thumbnail_out:
                # Copy temp file to target
                await asyncio.get_event_loop().run_in_executor(None, self.copy_file, output_file, thumbnail_out)

        except asyncio.TimeoutError:
            proc.kill()
            raise FFMpegError(True)

        finally:
            if output_file != thumbnail_out:
                # Delete temp file
                await asyncio.get_event_loop().run_in_executor(None, self.delete_file, output_file)

    @staticmethod
    def delete_file(file_path) -> bool:
        # Check source file really exists.
        if not os.path.isfile(file_path):
            return False

        os.remove(file_path)
        return True

    @staticmethod
    def copy_file(source_file: str, target_file: str) -> None:
        # Check source file really exists.
        if not os.path.isfile(source_file):
            raise FileDoesNotExistError()

        # Copy if destination doesn't exist
        if not os.path.isfile(target_file):
            shutil.copyfile(source_file, target_file)


def get_content_type_for_video(file_ext: str):
    if file_ext == ".mp4":
        return "video/mp4"
    if file_ext == ".webm":
        return "video/webm"


class FileCmdError(Exception):
    def __init__(self, timeout):
        self.timeout = timeout


class FFMpegError(Exception):
    def __init__(self, timeout, stderr=None):
        self.timeout = timeout
        self.stderr = stderr


class FFProbeError(Exception):
    def __init__(self, timeout, stderr=None):
        self.timeout = timeout
        self.stderr = stderr


class InvalidMimeTypeError(Exception):
    def __init__(self, mimetype: str):
        self.mime_type = mimetype


class InvalidVideoError(Exception):
    def __init__(self, container: str = "", video_codec: str = "", audio_codec: str = ""):
        self.container = container
        self.video_codec = video_codec
        self.audio_codec = audio_codec


class UnknownProgramError(Exception):
    pass


class FileDoesNotExistError(Exception):
    pass
