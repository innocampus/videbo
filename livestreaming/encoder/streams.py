import asyncio
import functools
import random
import shlex
import secrets
import socket
import string
import errno
import re
from enum import Enum
from pathlib import Path
from shutil import rmtree as shutil_rmtree
from typing import Optional
from livestreaming import settings
from livestreaming.streams import StreamState
from livestreaming.encoder import encoder_settings
from livestreaming.streams import Stream
from livestreaming.streams import StreamCollection
from livestreaming.encoder.api.models import StreamRecordingStartParams
from . import logger


class PortType(Enum):
    INTERNAL = 0
    PUBLIC = 1


def get_unused_port(port_type: PortType) -> int:
    if port_type == PortType.INTERNAL:
        raw_ports = encoder_settings.rtmp_internal_ports
    else:
        raw_ports = encoder_settings.rtmp_public_ports
    for ports in raw_ports.split(","):
        port_split = ports.split("-")
        if len(port_split) > 1:
            ports = range(int(port_split[0]), int(port_split[1]) + 1)
        else:
            ports = [int(port_split[0])]
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("127.0.0.1", port))
                    # if port is taken exception will raise
                    return port
                except socket.error as e:
                    if e.errno == errno.EADDRINUSE:
                        # expected case at runtime
                        pass
                    else:
                        raise e
    raise PortsInUseException()


class FFmpeg:
    """
    FFmpeg and converting to HLS playlist and segments.
    """
    STREAM_NAME_PREFIX = "stream_"

    def __init__(self, stream: 'EncoderStream'):
        self.stream: 'EncoderStream' = stream
        self.ffmpeg_process: Optional[asyncio.subprocess.Process] = None
        self.socat_process: Optional[asyncio.subprocess.Process] = None
        self.watch_task: Optional[asyncio.Task] = None
        self.current_time: float = 0.0

    async def start(self):
        program = encoder_settings.binary_ffmpeg
        url = self.stream.get_local_ffmpeg_url()

        args = f"-y -listen 1 -hide_banner -i {url} " \
               f"-filter_complex \"" \
               f"[v:0]scale=w='min(1920, iw)':h=-1[vout001];" \
               f"[v:0]scale=w=768:h=-1[vout002]\" " \
               f"-preset veryfast -g 30 -sc_threshold 0 " \
               f"-map a:0 -map a:0 -c:a aac -b:a 128k -ac 2 " \
               f"-map [vout001] -c:v:0 libx264 -b:v:0 3500k -maxrate:v:0 4000k -bufsize:v:0 6000k " \
               f"-map [vout002] -c:v:1 libx264 -b:v:1 800k -maxrate:v:1 900k -bufsize:v:1 1500k " \
               f"-hls_time 2 -hls_list_size 2 -hls_delete_threshold 2 " \
               f"-hls_flags delete_segments+independent_segments " \
               f"-master_pl_name stream.m3u8 -hls_segment_filename {self.STREAM_NAME_PREFIX}%v_d%06d.ts " \
               f"-var_stream_map \"v:0,a:0 v:1,a:1\" {self.STREAM_NAME_PREFIX}%v.m3u8 "

        if self.stream.recording_file is not None:
            args += f"-map v:0 -c:v:0 copy -map a:0 -c:a:0 copy {self.stream.recording_file}"

        self.ffmpeg_process = await asyncio.create_subprocess_exec(program, *shlex.split(args),
                                                                   stdout=asyncio.subprocess.PIPE,
                                                                   stderr=asyncio.subprocess.STDOUT,
                                                                   cwd=self.stream.dir)

        self.watch_task = asyncio.create_task(self.watch_ffmpeg())

        if encoder_settings.rtmps_cert and self.stream.use_rtmps:
            args = f"openssl-listen:{self.stream.public_port},cert={encoder_settings.rtmps_cert},verify=0"
        else:
            args = f"tcp-listen:{self.stream.public_port}"
        if self.stream.ip_range:
            args += f",range={self.stream.ip_range}"
        args += f" tcp:127.0.0.1:{self.stream.port}"

        self.socat_process = await asyncio.create_subprocess_exec(encoder_settings.binary_socat, *shlex.split(args),
                                                                  stdout=asyncio.subprocess.DEVNULL,
                                                                  stderr=asyncio.subprocess.DEVNULL,
                                                                  cwd=self.stream.dir)

    async def watch_ffmpeg(self):
        try:
            if self.ffmpeg_process is None:
                logger.warning("started watch of ffmpeg process with no process alive")
                return
            else:
                logger.info(f"started watch for stream {self.stream.stream_id}")
            buffer_counter = 0
            expected_states = [StreamState.NOT_YET_STARTED, StreamState.WAITING_FOR_CONNECTION, StreamState.BUFFERING,
                               StreamState.STREAMING]

            if self.stream.state not in expected_states:
                logger.error(f"stream {self.stream.stream_id} in unexpected state ({self.stream.state})")

            unexpected_stream_regex = re.compile(r"\[.*\] Unexpected stream")
            open_file_regex = re.compile(r"\[hls.*\] Opening '" + self.STREAM_NAME_PREFIX +
                                         r"0\.m3u8\.tmp' for writing")
            time_regex = re.compile(r"time=(\d{2}):(\d{2}):(\d{2}).(\d{2})")

            while self.stream.state in expected_states:
                if self.stream.state == StreamState.NOT_YET_STARTED:
                    self.stream.state = StreamState.WAITING_FOR_CONNECTION

                # first ffmpeg output will come when stream started
                line = await self.ffmpeg_process.stdout.readline()
                line_str = line.decode('utf-8').rstrip()
                logger.debug(line)

                if self.stream.state == StreamState.WAITING_FOR_CONNECTION:
                    self.stream.state = StreamState.BUFFERING

                if self.stream.state == StreamState.BUFFERING:
                    if unexpected_stream_regex.search(line_str) is not None:
                        # handle this case for security reasons
                        logger.error(f"unexpected stream, stop stream {self.stream.stream_id} immediately")
                        self.stream.state = StreamState.UNEXPECTED_STREAM
                        self.ffmpeg_process.kill()
                    if open_file_regex.search(line_str) is not None:
                        buffer_counter += 1
                        logger.debug(f"watch <stream {self.stream.stream_id}>: buffer_counter: {buffer_counter}")
                        if buffer_counter >= 3:
                            self.stream.state = StreamState.STREAMING

                times_found = time_regex.findall(line_str)
                if len(times_found):
                    time = times_found[-1]
                    self.current_time = float(time[0]) * 3600 + float(time[1]) * 60 + float(time[2]) + \
                                        float(time[3]) / 100.0

                if self.ffmpeg_process.stdout.at_eof():
                    self.stream.state = StreamState.STOPPED
                    break
            logger.info(f"watch for stream {self.stream.stream_id} ended")
        except asyncio.CancelledError:
            logger.warning(f"watch for stream {self.stream.stream_id} was CANCELLED unexpectedly")
            self.watch_task = None

    async def wait(self):
        # never wait for watch_task since it waits itself
        await self.ffmpeg_process.wait()
        await self.socat_process.wait()

    async def stop(self, kill_in: float = 2.0):
        for process_name in ['ffmpeg_process', 'socat_process']:
            process: asyncio.subprocess.Process = getattr(self, process_name, None)
            if process and process.pid:
                try:
                    if kill_in > 0:
                        process.terminate()
                        try:
                            await asyncio.wait_for(process.wait(), kill_in)
                        except asyncio.TimeoutError:
                            process.kill()
                    else:
                        process.kill()
                except OSError:
                    # The process most probably already does not exist anymore.
                    pass
            setattr(self, process_name, None)
        if self.watch_task:
            self.watch_task.cancel()

    async def kill(self):
        await self.stop(kill_in=0)


class EncoderStream(Stream):
    _start_new_stream_lock = asyncio.Lock()

    def __init__(self, stream_id: int, ip_range: Optional[str] = None, use_rtmps: bool = True):
        super().__init__(stream_id, ip_range, use_rtmps, logger)
        self.port: Optional[int] = None
        self.public_port: Optional[int] = None
        self.control_task: Optional[asyncio.Task] = None
        self._destroy_task: Optional[asyncio.Task] = None
        self.ffmpeg: Optional[FFmpeg] = None
        self.dir: Optional[Path] = None
        self.recording_file: Optional[Path] = None
        self.state = StreamState.NOT_YET_STARTED
        if settings.general.dev_mode:
            self.rtmp_stream_key = "random"
        else:
            self.rtmp_stream_key = secrets.token_hex(8)
        self.encoder_subdir_name = secrets.token_hex(8)
        self.waiting_for_connection_event: asyncio.Event = asyncio.Event()

    def start(self):
        # Run everything in an own task.
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        try:
            await self.create_temp_path()

            # Start ffmpeg (use a lock to avoid race conditions when multiple streams are started at the same time).
            async with EncoderStream._start_new_stream_lock:
                self.port = get_unused_port(port_type=PortType.INTERNAL)
                self.public_port = get_unused_port(port_type=PortType.PUBLIC)
                logger.info(f"Start ffmpeg on port {self.port} for stream id {self.stream_id}")
                self.state = StreamState.NOT_YET_STARTED
                self.ffmpeg = FFmpeg(self)
                await self.ffmpeg.start()

            self.waiting_for_connection_event.set()
            await self.ffmpeg.wait()
            logger.info(f"ffmpeg on port {self.port} for stream id {self.stream_id} ended")
            await asyncio.sleep(5)
            if self.ffmpeg.watch_task and not self.ffmpeg.watch_task.done():
                logger.warning(f"ffmpeg watch still running for stream id {self.stream_id} - cancelling task")
                self.ffmpeg.watch_task.cancel()

        except asyncio.CancelledError:
            if self.ffmpeg:
                await self.ffmpeg.stop()
        except Exception as err:
            logger.error(f"Error in stream controller: {str(err)}")
            self.state = StreamState.ERROR
            if self.ffmpeg:
                await self.ffmpeg.stop()

    async def create_temp_path(self):
        """Create a temporary directory for the stream.

        It also creates all parent dirs if needed and a temporary file for the recording.
        Since the directory is not access-protected, we use the random encoder_subdir_name as a protection."""
        if self.dir:
            # As long as this exists, we assume the dir exists in the system.
            return

        self.dir = Path(encoder_settings.hls_temp_dir, str(self.stream_id), self.encoder_subdir_name)
        # there might be blocking io, run in another thread
        await asyncio.get_event_loop().run_in_executor(None,
                                                       functools.partial(self.dir.mkdir, parents=True, exist_ok=True))
        rec_name = "recording_" + ''.join(random.choices(string.ascii_lowercase, k=8)) + ".mp4"
        self.recording_file = Path(self.dir, rec_name)

    def get_local_ffmpeg_url(self):
        return f"rtmp://127.0.0.1:{self.port}/stream/{self.rtmp_stream_key}"

    def get_public_url(self):
        return f"rtmp://localhost:{self.public_port}/stream"

    async def start_recording(self, info: StreamRecordingStartParams):
        # save current position in video (current video duration or better current file size?)
        raise NotImplementedError()

    async def stop_recording(self, withdraw_recording: bool):
        # save current position in video (current video duration or better current file size?)
        # Be prepared that the teacher may start another recording after stopping the current.
        raise NotImplementedError()

    def destroy(self):
        if self._destroy_task:
            # Destroy task is already running.
            logger.info(f"already destroying <stream {self.stream_id}>")
            return

        async def destroyer():
            try:
                try:
                    stream_collection.remove(self)
                    old_state = self.state
                    if self.ffmpeg:
                        await self.ffmpeg.stop()

                    if StreamState.BUFFERING <= old_state <= StreamState.STOPPED:
                        # Give content nodes a chance to get the last segments.
                        logger.info(f"<stream {self.stream_id}> wait 30 seconds until removing all files")
                        await asyncio.sleep(30)

                finally:
                    rm_func = None
                    if self.dir:
                        # Remove parent dir (named after the stream id).
                        rm_func = functools.partial(shutil_rmtree,
                                                    path=self.dir.parent,
                                                    # function, path, error (sys.exc_info)
                                                    onerror=lambda f, p, e: logger.error(f"{f} {p}:{e}"))
                    try:
                        if rm_func:
                            await asyncio.get_event_loop().run_in_executor(None, rm_func)
                    except OSError as err:
                        logger.exception(f"Error when deleting {self.dir}")
                    finally:
                        self.segments = []
                        self.current_playlist = None
                        logger.info(f"<stream {self.stream_id}> destroyed")
            except:
                logger.exception(f"Error in destroyer task for <stream {self.stream_id}>")

        self._destroy_task = asyncio.create_task(destroyer())
        logger.info(f"destroy <stream {self.stream_id}>")


class EncoderStreamCollection(StreamCollection[EncoderStream]):

    def create_new_stream(self, stream_id: int, ip_range: Optional[str] = None, use_rtmps: bool = True) -> EncoderStream:
        if stream_id in self.streams:
            raise StreamIdAlreadyExistsError()

        new_stream = EncoderStream(stream_id, ip_range, use_rtmps)
        self.streams[stream_id] = new_stream
        return new_stream


class StreamIdAlreadyExistsError(Exception):
    pass


class PortsInUseException(Exception):
    pass


stream_collection = EncoderStreamCollection()
