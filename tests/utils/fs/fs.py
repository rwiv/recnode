import os
import threading
import time
from datetime import datetime
from io import IOBase
from typing import TypeVar

from stdl.common.fs import read_fs_config_by_file
from stdl.utils.env import load_dot_env
from stdl.utils.fs.fs_common_types import FileInfo
from stdl.utils.fs.fs_local import LocalFsAccessor
from stdl.utils.fs.fs_s3 import S3FsAccessor
from stdl.utils.path import find_project_root, path_join

load_dot_env(path_join(find_project_root(), "dev", ".env"))
conf = read_fs_config_by_file(path_join(find_project_root(), "dev", "test_fs_conf.yaml"))
base_path = os.getenv("OUT_DIR_PATH")

s3_conf = conf.s3
# ac = S3FsAccessor(s3_conf)
ac = LocalFsAccessor()
s3 = S3FsAccessor(s3_conf)

target = path_join(find_project_root(), "dev", "test_fs_conf.yaml")


def print_file(file: FileInfo):
    print(file.path)


def test_s3_fs():
    print()

    if base_path is None:
        raise ValueError("OUT_DIR_PATH is not set")

    start_time = time.time()
    s3.mkdir("ts")
    for f in ac.get_list(path_join(base_path, "ts")):
        with open(f.path, "rb") as file:
            s3.write(f"ts/{f.name}", file)
    elapsed_time = time.time() - start_time
    print(f"실행 시간: {elapsed_time:.6f}초")

    parallel = 10
    start_time = time.time()
    s3.mkdir("ts")
    for sub in sublist(ac.get_list(path_join(base_path, "ts")), parallel):
        reqs: list[tuple[str, bytes]] = []
        for f in sub:
            with open(f.path, "rb") as file:
                reqs.append((f"ts/{f.name}", file.read()))
        # write_batch(reqs)
        write_batch(reqs)
    elapsed_time = time.time() - start_time
    print(f"실행 시간: {elapsed_time:.6f}초")

    # start_time = time.time()
    # s3.rmdir("/")
    # elapsed_time = time.time() - start_time
    # print(f"실행 시간: {elapsed_time:.6f}초")

    # ac.mkdir(join(base_path, "zxc"))
    # ac.delete(join(base_path, "zxc"))
    # ac.rmdir(join(base_path, "zxc"))

    # create_hierarchy()
    # s3.walk(root(), print_file)

    # stream = ac.read(target)
    # chunk_size = 4096
    # while True:
    #     data = stream.read(chunk_size)
    #     if not data:
    #         break
    #     print(data)


def create_hierarchy():
    s3.mkdir("/a1")
    s3.mkdir("/a1/b1")
    s3.mkdir("/a1/b2")
    s3.mkdir("/a2")
    s3.mkdir("/a2/b1")
    s3.mkdir("/a2/b2")
    s3.write("/a1/b1/test1.txt", b"test")
    s3.write("/a1/b1/test2.txt", b"test")
    s3.write("/a1/b2/test1.txt", b"test")
    s3.write("/a1/b2/test2.txt", b"test")
    s3.write("/a2/b1/test1.txt", b"test")
    s3.write("/a2/b1/test2.txt", b"test")
    s3.write("/a2/b2/test1.txt", b"test")
    s3.write("/a2/b2/test2.txt", b"test")


def root():
    return FileInfo(name="/", path="/", is_dir=True, size=0, mtime=datetime.now())


def write_batch(reqs: list[tuple[str, bytes | IOBase]]):
    threads = []
    for path, data in reqs:
        thread = threading.Thread(target=s3.write, args=(path, data))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


T = TypeVar("T")


def sublist(lst: list[T], size: int) -> list[list[T]]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]
