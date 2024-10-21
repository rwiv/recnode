import os
import shutil
import subprocess

from stdl.utils.logger import log


def merge_chunks(tmp_chunks_path: str, out_dir_path: str, name: str):
    # merge ts files
    merged_ts_path = f"{tmp_chunks_path}.ts"
    with open(merged_ts_path, "wb") as outfile:
        ts_filenames = sorted(
            [f for f in os.listdir(tmp_chunks_path) if f.endswith(".ts")],
            key=lambda x: int(x.split(".")[0])
        )
        for ts_filename in ts_filenames:
            with open(os.path.join(tmp_chunks_path, ts_filename), "rb") as infile:
                outfile.write(infile.read())
    shutil.rmtree(tmp_chunks_path)

    # convert ts to mp4
    mp4_path = f"{tmp_chunks_path}.mp4"
    command = ['ffmpeg', '-i', merged_ts_path, '-c', 'copy', mp4_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    os.remove(merged_ts_path)

    # move mp4 file
    dirname = os.path.basename(tmp_chunks_path)
    os.makedirs(f"{out_dir_path}/{name}", exist_ok=True)
    shutil.move(mp4_path, f"{out_dir_path}/{name}/{dirname}.mp4")
    shutil.rmtree(f"{out_dir_path}/{name}/{dirname}")

    # remove tmp files
    log.info("Convert file", {"file_path": mp4_path})
