import os
import shutil
import subprocess
import threading
import time

from stdl.downloaders.streamlink.stream import StreamlinkManager, StreamlinkArgs
from stdl.utils.logger import log


class StreamRecorder:

    def __init__(self, args: StreamlinkArgs, once: bool = True):
        self.once = once

        self.restart_delay_sec = 3
        self.chunk_threshold = 20
        self.streamlink = StreamlinkManager(StreamlinkArgs(
            url=args.url,
            name=args.name,
            out_dir=args.out_dir,
            cookies=args.cookies,
            options=args.options,
        ))

    def record(self):
        if self.once:
            self.__record_once()
        else:
            self.__record_endless()

    def __record_once(self):
        while True:
            self.__record()
            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break
        log.info("End Record", {"latest_state": self.streamlink.state.name})

    def __record_endless(self):
        while True:
            self.__record()
            time.sleep(self.restart_delay_sec)
            log.info("Restart Record", {"latest_state": self.streamlink.state.name})

    def __record(self):
        streams = self.streamlink.wait_for_live()
        log.info("Stream Start")

        dir_path = self.streamlink.record(streams)

        if len(os.listdir(dir_path)) < self.chunk_threshold:
            shutil.rmtree(dir_path)
        else:
            thread = threading.Thread(target=merge_chunks, args=(dir_path,))
            thread.daemon = True
            thread.start()


def merge_chunks(src_dir_path: str):
    merged_ts_path = f"{src_dir_path}.ts"
    mp4_path = f"{src_dir_path}.mp4"

    # merge ts files
    with open(merged_ts_path, "wb") as outfile:
        ts_filenames = sorted(
            [f for f in os.listdir(src_dir_path) if f.endswith(".ts")],
            key=lambda x: int(x.split(".")[0])
        )
        for ts_filename in ts_filenames:
            with open(os.path.join(src_dir_path, ts_filename), "rb") as infile:
                outfile.write(infile.read())

    # convert ts to mp4
    command = ['ffmpeg', '-i', merged_ts_path, '-c', 'copy', mp4_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    shutil.rmtree(src_dir_path)
    os.remove(merged_ts_path)
    log.info("Convert file", {"file_path": mp4_path})
