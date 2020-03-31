from enum import Enum
from typing import Optional, List, Dict
from livestreaming.streams import StreamState
from livestreaming.web import JSONBaseModel


class NewStreamParams(JSONBaseModel):
    ip_range: Optional[str]


class NewStreamCreated(JSONBaseModel):
    url: str


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
    streams: List[EncoderStreamStatus]


class StreamRecordingMeta(JSONBaseModel):
    """Data that is needed for the LMS that we only passthrough here and do not interpret."""
    video_availability: int
    video_title: str
    video_description: str
    video_date: int
    lms_user_id_streamer: int
    lms_instance_id: int


class StreamRecordingStartParams(JSONBaseModel):
    meta: StreamRecordingMeta
    video_storage_upload_url: str


class StreamRecordingStopParams(JSONBaseModel):
    withdraw_recording: bool


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
    lms_url: Optional[str] # after successful upload where to find the video


class StreamRecordingsStatusReturn(JSONBaseModel):
    recordings: Dict[int, StreamRecordingStatus]
