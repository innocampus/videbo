from videbo.exceptions import CouldNotCreateDir


class CouldNotCreateTempDir(CouldNotCreateDir):
    pass


class CouldNotCreateTempOutDir(CouldNotCreateTempDir):
    pass


class HashedFileInvalidExtensionError(Exception):
    pass


class UploadError(Exception):
    pass


class FormFieldMissing(UploadError):
    pass


class BadFileExtension(UploadError):
    pass


class NoValidFileInRequestError(Exception):
    pass


class FileTooBigError(Exception):
    pass
