from stdl.downloaders.streamlink.merge import merge_chunks


def test_recorder():
    print()
    src_chunks_path = "./src_chunks"
    tmp_dir_path = "./tmp_dir"
    channel_id = "hello"
    merge_chunks(src_chunks_path, tmp_dir_path, channel_id)
