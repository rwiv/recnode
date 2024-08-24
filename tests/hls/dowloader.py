import asyncio
from stdl.hls.downloader import HlsDownloader, download_file
from stdl.hls.parser import parse_media_playlist


def test():
    print()
    m3u8_path = "../../dev/test_media.m3u8"
    url_path = "../../dev/test_url.txt"
    with open(m3u8_path, "r") as file:
        m3u8 = file.read()
    with open(url_path, "r") as file:
        base_url = file.read()

    urls = parse_media_playlist(m3u8, base_url.replace("\n", "")).segment_paths
    dl = HlsDownloader(
        urls=urls,
        base_dir_path="../../dev",
        out_name="out"
    )
    asyncio.run(dl.download())
