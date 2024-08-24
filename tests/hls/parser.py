from stdl.hls.parser import parse_master_playlist, merge_intersected_strings, parse_media_playlist


def test_master_playlist():
    print()
    path = "../../dev/test_master.m3u8"
    with open(path, 'r') as f:
        m3u8 = f.read()
    p = parse_master_playlist(m3u8)
    for r in p.resolutions:
        print(r)


def test_media_playlist():
    print()
    path = "../../dev/test_media.m3u8"
    with open(path, 'r') as f:
        m3u8 = f.read()
    p = parse_media_playlist(m3u8, "https://hello/")
    print(p.ext)
    print(p.segment_paths)


def test_merge_intersected_strings():
    assert merge_intersected_strings("abc", "bcd") == "abcd"
    assert merge_intersected_strings("abc", "abc") == "abc"
    assert merge_intersected_strings("abc", "def") == "abcdef"
