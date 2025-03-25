import re
from dataclasses import dataclass

from .utils import merge_intersected_strings, get_ext


class M3u8ElemError(Exception):
    def __init__(self, message: str):
        super().__init__(f"M3U8 Elem Error: {message}")


@dataclass
class Resolution:
    resolution: int
    name: str


@dataclass
class MasterPlaylist:
    resolutions: list[Resolution]


@dataclass
class MediaPlaylist:
    segment_paths: list[str]
    init_section_path: str | None
    ext: str | None


@dataclass
class MediaPaths:
    segment_paths: list[str]
    ext: str | None


@dataclass
class M3u8Element:
    header: str
    value: str | None


def is_media(m3u8: str) -> bool:
    return "#EXTINF" in m3u8


def parse_master_playlist(m3u8: str) -> MasterPlaylist:
    elems = __parse_elems(m3u8)
    elems = [elem for elem in elems if elem.header.startswith("#EXT-X-STREAM-INF")]

    resolutions = []
    for elem in elems:
        match = re.search(r"RESOLUTION=\d*x(\d*)", elem.header)
        if match is None or len(match.groups()) < 1:
            raise M3u8ElemError(f"{elem}")

        resolution = int(match.group(1))
        name = elem.value
        if name is None:
            raise M3u8ElemError(f"{elem}")

        resolutions.append(Resolution(resolution=resolution, name=name))

    return MasterPlaylist(resolutions=resolutions)


def parse_media_playlist(m3u8: str, base_url_param: str = "", qs: str | None = None) -> MediaPaths:
    base_url = base_url_param.rstrip("/")
    media_playlist = __parse_media_playlist_raw(m3u8)
    origin_paths = [media_playlist.init_section_path] if media_playlist.init_section_path else []
    origin_paths.extend(media_playlist.segment_paths)

    segment_paths = []
    for path in origin_paths:
        if re.match(r"https?:", path):
            segment_paths.append(path)
        else:
            new_path = path
            if not new_path.startswith("/"):
                new_path = "/" + new_path
            segment = merge_intersected_strings(base_url, new_path)
            if qs is not None and qs != "":
                segment += f"?{qs}"
            segment_paths.append(segment)

    return MediaPaths(segment_paths=segment_paths, ext=media_playlist.ext)


def __parse_media_playlist_raw(m3u8: str) -> MediaPlaylist:
    elems = __parse_elems(m3u8)
    seg_elems = [elem for elem in elems if elem.header.startswith("#EXTINF")]

    segment_paths = []
    for elem in seg_elems:
        path = elem.value
        if path is None:
            raise M3u8ElemError(f"{elem}")
        segment_paths.append(path)

    init_section_path = None
    init_elem = next((elem for elem in elems if elem.header.startswith("#EXT-X-MAP")), None)
    if init_elem is not None:
        match = re.search(r'URI="(.*?)"', init_elem.header, re.IGNORECASE)
        if match and len(match.groups()) > 0:
            init_section_path = match.group(1)

    ext = "ts"
    if init_section_path:
        ext = get_ext(init_section_path)
    elif segment_paths:
        ext = get_ext(segment_paths[0])

    return MediaPlaylist(segment_paths=segment_paths, init_section_path=init_section_path, ext=ext)


def __parse_elems(m3u8: str) -> list[M3u8Element]:
    if len(m3u8) == 0:
        raise ValueError("empty m3u8")

    if m3u8[0] != "#":
        raise ValueError("invalid m3u8")

    raw_elems = []
    cur_idx = -1
    for ch in m3u8:
        if ch == "#":
            raw_elems.append("")
            cur_idx += 1
        raw_elems[cur_idx] += ch

    elems = []
    for raw in raw_elems:
        chunks = [chunk for chunk in raw.splitlines() if chunk]
        if len(chunks) not in [1, 2]:
            raise ValueError("invalid m3u8 element")
        header = chunks[0]
        value = chunks[1] if len(chunks) == 2 else None
        elems.append(M3u8Element(header=header, value=value))

    return elems
