import os
import shutil
import subprocess

from stdl.utils.logger import log


def merge_chunks(src_chunks_path: str, tmp_dir_path: str, channel_id: str):
    dirname = os.path.basename(src_chunks_path)
    tmp_channel_path = f"{tmp_dir_path}/{channel_id}"
    os.makedirs(tmp_channel_path, exist_ok=True)

    # merge ts files
    merged_ts_path = f"{tmp_channel_path}/{dirname}.ts"
    with open(merged_ts_path, "wb") as outfile:
        ts_filenames = sorted(
            [f for f in os.listdir(src_chunks_path) if f.endswith(".ts")],
            key=lambda x: int(x.split(".")[0])
        )
        for ts_filename in ts_filenames:
            with open(os.path.join(src_chunks_path, ts_filename), "rb") as infile:
                outfile.write(infile.read())

    # convert ts to mp4
    mp4_path = f"{tmp_channel_path}/{dirname}.mp4"
    command = ['ffmpeg', '-i', merged_ts_path, '-c', 'copy', mp4_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # move mp4 file
    shutil.move(mp4_path, f"{src_chunks_path}.mp4")

    # remove tmp files
    os.remove(merged_ts_path)
    shutil.rmtree(src_chunks_path)
    log.info("Convert file", {"file_path": mp4_path})
