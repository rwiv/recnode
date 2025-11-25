import os
import tarfile
import time

from pyutils import path_join, find_project_root

from recnode.utils import stem

base_dir_path = path_join(find_project_root(), "dev", "test")
paths = [
    path_join(base_dir_path, "src", "1.ts"),
    path_join(base_dir_path, "src", "2.ts"),
]


def test_tarfile():
    os.makedirs(path_join(base_dir_path, "src"), exist_ok=True)

    with open(paths[0], "w") as f:
        f.write("test1")
    with open(paths[1], "w") as f:
        f.write("test2")

    out_filename = "out.tar"

    start = time.time()
    with tarfile.open(path_join(base_dir_path, out_filename), "w") as tar:
        for file in paths:
            tar.add(file, arcname=file.split("/")[-1])
    print("tarfile write time:", time.time() - start)

    start = time.time()
    with tarfile.open(path_join(base_dir_path, out_filename), "r:*") as tar:
        tar.extractall(path=path_join(base_dir_path, "out"))
    print("tarfile read time:", time.time() - start)


def test_tarfile2():
    os.makedirs(path_join(base_dir_path, "src"), exist_ok=True)
    file_names = sorted(os.listdir(path_join(base_dir_path, "src")), key=lambda x: int(stem(x)))
    file_paths = [path_join(base_dir_path, "src", file_name) for file_name in file_names]

    start = time.time()
    with tarfile.open(path_join(base_dir_path, "out.tar"), "w") as tar:
        for file in file_paths:
            tar.add(file, arcname=file.split("/")[-1])
    print("tarfile write time:", time.time() - start)
