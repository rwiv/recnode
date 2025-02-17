from datetime import datetime

from stdl.common.fs_config import read_fs_config_by_file
from stdl.utils.fs.fs_s3 import S3Accessor
from stdl.utils.fs.fs_common_types import FileInfo

conf = read_fs_config_by_file("../../../dev/test_fs_conf.yaml")

s3_conf = conf.s3[0]
s3 = S3Accessor(s3_conf)


def print_file(file: FileInfo):
    print(file.path)


def test_s3_fs():
    print()

    s3.rmdir("/")
    # create_hierarchy()

    # s3.walk(root(), print_file)

    # s3.write("aa/bb/test2.txt", b"test")
    # s3.write("aa/cc/test1.txt", b"test")
    # s3.delete("ab/test.txt")
    # print(s3.exists("ab/test.txt"))
    for f in s3.get_list("/"):
        print(f.path)
    # stream = s3.read("ab/test.txt")
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
