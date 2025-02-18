import os


def write_bfile(file_path: str, data: bytes, dir_check: bool = True):
    if dir_check:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(data)


def write_file(file_path: str, data: str, dir_check: bool = True):
    if dir_check:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(data)


def delete_file(file_path: str):
    if os.path.exists(file_path):
        os.remove(file_path)
