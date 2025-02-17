from abc import abstractmethod, ABC
from io import IOBase
from typing import Callable

from stdl.utils.fs.fs_common_types import FileInfo
from stdl.utils.fs.fs_s3_utils import to_dir_path


class FsAccessor(ABC):
    @abstractmethod
    def head(self, path: str) -> FileInfo | None:
        pass

    def exists(self, path: str) -> bool:
        if self.head(path) is not None:
            return True
        return False

    @abstractmethod
    def mkdir(self, dir_path: str):
        pass

    def rmdir(self, dir_path: str):
        children = self.get_list(dir_path)
        if len(children) == 0:
            return
        for c in children:
            if c.is_dir:
                self.rmdir(c.path)
            else:
                self.delete(c.path)
        if dir_path == "" or dir_path == "/":
            return
        self.delete(to_dir_path(dir_path))

    @abstractmethod
    def get_list(self, dir_path: str) -> list[FileInfo]:
        pass

    @abstractmethod
    def read(self, path: str) -> IOBase:
        pass

    @abstractmethod
    def write(self, path: str, data: bytes | IOBase):
        pass

    @abstractmethod
    def delete(self, path: str):
        pass

    def walk(self, file: FileInfo, cb: Callable[[FileInfo], None]):
        path = file.path
        if file.is_dir is False:
            cb(file)
        children = self.get_list(path)
        for c in children:
            if c.is_dir is False:
                cb(c)
            else:
                self.walk(c, cb)
