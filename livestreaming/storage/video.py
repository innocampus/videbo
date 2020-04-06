import asyncio
import json
from typing import Dict
from typing import List
from typing import Optional

from livestreaming.storage.exceptions import FileCmdError
from livestreaming.storage.exceptions import FFProbeError
from livestreaming.storage.exceptions import InvalidVideoError
from livestreaming.storage.exceptions import InvalidMimeTypeError

MIME_TYPE_WHITELIST = ['video/mp4', 'video/webm']
CONTAINER_WHITELIST = ['mp4', 'webm']
VIDEO_CODEC_WHITELIST = ['h264', 'vp8']
AUDIO_CODEC_WHITELIST = ['aac', 'vorbis', 'mp3']


class VideoInfo:
    """Wrapper for the ffprobe application to get information about a video file."""

    def __init__(self, video_file: str):
        self.video_file = video_file
        self.valid_video = False
        self.mime_type = None
        self.streams = []  # type: List[Dict]
        self.format = None  # type: Optional[Dict]

    async def fetch_mimetype(self, binary: str = "file", user: str = None) -> None:
        """Call file and fetch mime type."""

        args = ["-b", "-i", self.video_file]
        # Run file command
        proc = await self.create_sudo_subprocess(binary, args, user=user, stdout=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
            self.mime_type = stdout.decode().strip().split(';')[0]  # strip linebreaks, extract first part
        except asyncio.TimeoutError:
            proc.kill()
            raise FileCmdError(True)

    async def fetch_info(self, binary: str = "ffprobe", user: str = None) -> None:
        """Call ffprobe and fetch information."""

        args = ["-show_format", "-show_streams", "-print_format", "json", self.video_file]
        # Run ffprobe
        proc = await self.create_sudo_subprocess(binary, args, user=user, stdout=asyncio.subprocess.PIPE,
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

    @staticmethod
    async def create_sudo_subprocess(binary: str, args: List[str], user: str = None,
                                     stdout: int = asyncio.subprocess.DEVNULL,
                                     stderr: int = asyncio.subprocess.DEVNULL) -> asyncio.subprocess.Process:
        program = binary
        # Run as different user
        if user:
            program = "sudo"
            args = ["-u", user, binary] + args

        return await asyncio.create_subprocess_exec(program, *args, stdout=stdout, stderr=stderr)


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
        if video_stream:
            audio_codec = audio_stream["codec_name"]
            valid_audio = audio_codec in AUDIO_CODEC_WHITELIST

        if not valid_format or not valid_video or not valid_audio:
            raise InvalidVideoError(','.join(formats), video_codec, audio_codec)
