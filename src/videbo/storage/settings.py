from pathlib import Path
from typing import List, Set, Optional

from pydantic import validator

from videbo.base_settings import CommonSettings, PROJECT_DIR
from videbo.misc import ensure_url_does_not_end_with_slash as normalize_url


class StorageSettings(CommonSettings):
    _section = 'storage'

    listen_port: int = 9020
    files_path: Path = Path('/tmp/videbo/storage')
    public_base_url: str = 'http://localhost:9020'
    max_file_size_mb: float = 200.0
    thumb_suggestion_count: int = 3
    thumb_height: int = 90
    mime_types_allowed: Set[str] = {'video/mp4', 'video/webm'}
    container_formats_allowed: Set[str] = {'mp4', 'webm'}
    video_codecs_allowed: Set[str] = {'h264', 'vp8'}
    audio_codecs_allowed: Set[str] = {'aac', 'vorbis'}
    check_user: Optional[str] = None
    binary_file: str = 'file'
    binary_ffmpeg: str = 'ffmpeg'
    binary_ffprobe: str = 'ffprobe'
    static_dist_node_base_urls: List[str] = ['http://localhost:9030/', ]
    copy_to_dist_views_threshold: int = 3
    reset_views_every_hours: int = 4
    dist_free_space_target_ratio: float = 0.1
    max_parallel_copying_tasks: int = 20
    thumb_cache_max_mb: int = 30
    prom_text_file: Optional[Path] = None
    prom_update_freq_sec: float = 15.0
    test_video_file_path: Path = Path(PROJECT_DIR, 'tests', 'test_video.mp4')

    @validator('reset_views_every_hours')
    def ensure_min_reset_freq(cls, freq: int) -> int:
        return max(freq, 1)

    _norm_public_base_url = validator('public_base_url', allow_reuse=True)(normalize_url)
    _norm_dist_node_urls = validator('static_dist_node_base_urls', each_item=True, allow_reuse=True)(normalize_url)
