import os
import subprocess

from pyutils import path_join


def merge_ts(chunks_path: str) -> str:
    merged_ts_path = f"{chunks_path}.ts"
    with open(merged_ts_path, "wb") as outfile:
        ts_filenames = sorted(
            [f for f in os.listdir(chunks_path) if f.endswith(".ts")], key=lambda x: int(x.split(".")[0])
        )
        for ts_filename in ts_filenames:
            with open(path_join(chunks_path, ts_filename), "rb") as infile:
                outfile.write(infile.read())
    return merged_ts_path


def convert_vid(src_path: str, out_path: str):
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
