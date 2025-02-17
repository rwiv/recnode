from stdl.utils.fs.fs_s3_utils import to_dir_path


def test():
    assert to_dir_path("") == ""
    assert to_dir_path("/") == ""
    assert to_dir_path("a/b") == "a/b/"
    assert to_dir_path("a/b/") == "a/b/"
    assert to_dir_path("/a/b") == "a/b/"
    assert to_dir_path("/a/b/") == "a/b/"
