from stdl.utils.path import dirname, path_join


def test_basename():
    assert dirname("a/b/c.txt") == "a/b"


def test_join():
    assert path_join("home", "user", "documents") == "home/user/documents"
    assert path_join("/home/", "/user/", "documents/", delimiter="\\") == "\\home\\user\\documents"
