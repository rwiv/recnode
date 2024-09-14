from stdl.utils.url import get_query_string


def test_get_query_string():
    assert get_query_string("https://example.com/path/to/file?query=1") == "query=1"
