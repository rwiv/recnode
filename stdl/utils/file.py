import os


def write_file(file_path: str, data: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(data)


def delete_file(file_path: str):
    if os.path.exists(file_path):
        os.remove(file_path)
