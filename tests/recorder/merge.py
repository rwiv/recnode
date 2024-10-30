from stdl.downloaders.hls.merge import merge_hls_chunks


def test_recorder():
    print()
    channel_id = "hello"
    dirname = "a"
    out_dir_path = "./src_chunks"
    tmp_dir_path = f"./tmp_dir/{channel_id}/{dirname}"
    merge_hls_chunks(tmp_dir_path, out_dir_path, channel_id)
