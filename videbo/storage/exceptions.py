from videbo.exceptions import CouldNotCreateDir


class CouldNotCreateTempDir(CouldNotCreateDir):
    pass


class CouldNotCreateTempOutDir(CouldNotCreateTempDir):
    pass


class HashedFileInvalidExtensionError(Exception):
    pass


class NoValidFileInRequestError(Exception):
    pass


class FileTooBigError(Exception):
    pass
