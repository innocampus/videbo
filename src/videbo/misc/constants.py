HTTP_CODE_OK = 200

JPG_EXT = ".jpg"  # for thumbnails

MAX_NUM_FILES_TO_PRINT = 20

# TODO: Introduce consistency between binary and decimal prefixes
MEGA = 1024 * 1024

VIDEO_FILE_EXT_MIME_TYPE = {
    "3g2": "video/3gpp2",
    "3gp": "video/3gpp",
    "asf": "video/x-ms-asf",
    "asx": "video/x-ms-asf",
    "avi": "video/x-msvideo",
    "dvb": "video/vnd.dvb.file",
    "f4v": "video/x-f4v",
    "fli": "video/x-fli",
    "flv": "video/x-flv",
    "fvt": "video/vnd.fvt",
    "h261": "video/h261",
    "h263": "video/h263",
    "h264": "video/h264",
    "jpgm": "video/jpm",
    "jpgv": "video/jpeg",
    "jpm": "video/jpm",
    "m1v": "video/mpeg",
    "m2v": "video/mpeg",
    "m4u": "video/vnd.mpegurl",
    "m4v": "video/x-m4v",
    "mj2": "video/mj2",
    "mjp2": "video/mj2",
    "mk3d": "video/x-matroska",
    "mks": "video/x-matroska",
    "mkv": "video/x-matroska",
    "mng": "video/x-mng",
    "movie": "video/x-sgi-movie",
    "mov": "video/quicktime",
    "mp4": "video/mp4",
    "mp4v": "video/mp4",
    "mpeg": "video/mpeg",
    "mpe": "video/mpeg",
    "mpg4": "video/mp4",
    "mpg": "video/mpeg",
    "mxu": "video/vnd.mpegurl",
    "ogv": "video/ogg",
    "pyv": "video/vnd.ms-playready.media.pyv",
    "qt": "video/quicktime",
    "smv": "video/x-smv",
    "uvh": "video/vnd.dece.hd",
    "uvm": "video/vnd.dece.mobile",
    "uvp": "video/vnd.dece.pd",
    "uvs": "video/vnd.dece.sd",
    "uvu": "video/vnd.uvvu.mp4",
    "uvvh": "video/vnd.dece.hd",
    "uvvm": "video/vnd.dece.mobile",
    "uvvp": "video/vnd.dece.pd",
    "uvvs": "video/vnd.dece.sd",
    "uvvu": "video/vnd.uvvu.mp4",
    "uvv": "video/vnd.dece.video",
    "uvvv": "video/vnd.dece.video",
    "viv": "video/vnd.vivo",
    "vob": "video/x-ms-vob",
    "webm": "video/webm",
    "wm": "video/x-ms-wm",
    "wmv": "video/x-ms-wmv",
    "wmx": "video/x-ms-wmx",
    "wvx": "video/x-ms-wvx",
}
VIDEO_CONTAINER_FORMATS = set(VIDEO_FILE_EXT_MIME_TYPE.keys())
VIDEO_MIME_TYPES = set(VIDEO_FILE_EXT_MIME_TYPE.values())
