class HashedFile:
    __slots__ = ("hash", "ext")

    hash: str
    ext: str

    def __init__(self, file_hash: str, file_ext: str) -> None:
        if file_ext[0] != ".":
            raise ValueError(file_ext)
        self.hash = file_hash
        self.ext = file_ext

    def __str__(self) -> str:
        return self.hash + self.ext

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.hash}{self.ext})"

    def __hash__(self) -> int:
        return int(self.hash, 16)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HashedFile) or type(self) is not type(other):
            return NotImplemented
        return self.hash == other.hash
