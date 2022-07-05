from videbo.exceptions import VideboBaseException


class HashedFileInvalidExtensionError(VideboBaseException):
    pass


class UploadError(VideboBaseException):
    pass


class FormFieldMissing(UploadError):
    pass


class BadFileExtension(UploadError):
    pass


class FileTooBigError(UploadError):
    pass


class DistributionError(VideboBaseException):
    pass


class DistStatusUnknown(DistributionError):
    pass


class UnknownDistURL(DistributionError):
    pass


class DistAlreadyDisabled(DistributionError):
    pass


class DistAlreadyEnabled(DistributionError):
    pass
