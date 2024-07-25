import re
from pathlib import Path
from typing_extensions import Self


FILE_NAME_PATTERN = re.compile(r"^([0-9a-f]{64})(\.[0-9a-z]{1,10})$")


class HashedFile:
    __slots__ = ("hash", "ext", "size")

    hash: str
    ext: str
    size: int  # in bytes

    @classmethod
    def from_path(cls, file_path: Path) -> Self:
        return cls(
            file_hash=file_path.stem,
            file_ext=file_path.suffix,
            file_size=file_path.stat().st_size,
        )

    def __init__(self, file_hash: str, file_ext: str, file_size: int) -> None:
        if not re.search(FILE_NAME_PATTERN, file_hash + file_ext):
            raise ValueError(
                f"Invalid file hash and/or extension: {file_ext}{file_ext}"
            )
        if file_size < 0:
            raise ValueError(f"File cannot have negative size: {file_size}")
        self.hash = file_hash
        self.ext = file_ext
        self.size = file_size

    def __str__(self) -> str:
        return self.hash + self.ext

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.hash!r}, {self.ext!r}, {self.size!r})"

    def __hash__(self) -> int:
        return int(self.hash, 16)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HashedFile) or type(self) is not type(other):
            return NotImplemented
        return self.hash == other.hash
