from logging import getLogger

from videbo.storage.stored_file import StoredVideoFile


log = getLogger(__name__)


class DownloadScheduler:
    """
    Helper class to collect files that should be downloaded ASAP.

    Additionally, the `in` operator can be used on an instance to determine if a file is already scheduled,
    and the built-in `len(...)` function can be used get the number of files currently scheduled.
    """
    class NothingScheduled(Exception):
        pass

    def __init__(self) -> None:
        self.files_from_urls: set[tuple[StoredVideoFile, str]] = set()

    def schedule(self, file: StoredVideoFile, from_url: str) -> None:
        self.files_from_urls.add((file, from_url))
        log.info(f"File {file} from {from_url} now scheduled for download")

    def next(self) -> tuple[StoredVideoFile, str]:
        """
        If any files are left to be downloaded, chose the one with the highest number of views.
        Note that the `StoredVideoFile` class compares objects by their `.views` attribute.
        """
        try:
            tup = max(self.files_from_urls, key=lambda x: x[0])
        except ValueError:
            raise self.NothingScheduled from None
        else:
            self.files_from_urls.discard(tup)
            log.info(f"Next file to download: {tup[0]} from {tup[1]}")
            return tup

    def __contains__(self, item: StoredVideoFile) -> bool:
        for file, _ in self.files_from_urls:
            if file == item:
                return True
        return False

    def __len__(self) -> int:
        return len(self.files_from_urls)
