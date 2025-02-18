from typing import Any

import yt_dlp


def my_hook(d):
    # if d["status"] == "downloading":
    #     print("downloading")
    if d["status"] == "finished":
        print("\nDone downloading, now converting ...")


class MyCustomPP(yt_dlp.postprocessor.PostProcessor):
    def run(self, information):
        # self.to_screen('Doing stuff')
        print(information)
        return [], information


def opts(out_dir_path: str, cookie_path: str | None = None):
    opts: dict[str, Any] = {
        # "listsubtitles": True,  # 자막 리스트 확인
        # "writesubtitles": True,  # 자막 포함
        # "subtitleslangs": ["en-US"],  # 자막 언어 선택
        # "subtitlesformat": "vtt",  # 자막 format 선택
        # "skip_download": True,
        "paths": {"home": out_dir_path},
        # "progress_hooks": [my_hook],
        # "daterange": DateRange("20240101", "20240109")
    }
    if cookie_path is not None:
        opts["cookiefile"] = cookie_path
    return opts


class YtdlDownloader:
    def __init__(self, out_dir_path: str):
        self.out_dir_path = out_dir_path

    def download(self, urls: list[str]):
        with yt_dlp.YoutubeDL(opts(self.out_dir_path)) as ydl:
            ydl.download(urls)
