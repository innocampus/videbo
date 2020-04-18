from enum import Enum
from typing import Optional, Dict
from livestreaming.streams import StreamState
from livestreaming.web import JSONBaseModel


class NewStreamParams(JSONBaseModel):
    ip_range: Optional[str]
    rtmps: bool
    lms_stream_instance_id: int


class NewStreamCreated(JSONBaseModel):
    rtmp_port: str
    rtmp_stream_key: str
    encoder_subdir_name: str


class NewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[NewStreamCreated]
    error: Optional[str]


class EncoderStreamStatus(JSONBaseModel):
    stream_id: int
    state: StreamState
    state_last_update: int


class EncoderStatus(JSONBaseModel):
    max_streams: int
    current_streams: int
    streams: Dict[int, EncoderStreamStatus]


class StreamRecordingMeta(JSONBaseModel):
    """Data that is needed for the LMS that we only passthrough here and do not interpret."""
    availability: int
    title: str
    description: str
    date: int
    commentsallowed: int
    lms_user_id_streamer: int
    lms_collection_id: int


class StreamRecordingStartParams(JSONBaseModel):
    meta: StreamRecordingMeta
    video_storage_upload_url: str


class StreamRecordingStopParams(JSONBaseModel):
    withdraw_recording: bool  # do not save video


class StreamRecordingState(Enum):
    WAITING_FOR_STREAMER_START = 0
    RECORDING = 1
    QUEUED = 2 # recording stopped, waiting for processing to be started
    PROCESSING = 3 # recording is reencoded right now
    FINISHED = 4


class StreamRecordingStatus(JSONBaseModel):
    recording_id: int
    meta: StreamRecordingMeta
    start_time: int
    stop_time: Optional[int]
    state: StreamRecordingState
    processing_progress: int # in %


class StreamRecordingsStatusReturn(JSONBaseModel):
    recordings: Dict[int, StreamRecordingStatus]
