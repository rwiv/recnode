import pytest
import rust_request
from pyutils import find_project_root, path_join

base_path = path_join(find_project_root(), "dev")


@pytest.mark.asyncio
async def test_rust_request():
    url = "https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64"
    file_path = path_join(base_path, "jq-linux-amd64")

    status, size, content = await rust_request.request_file(url, {}, None, False)  # type: ignore
    assert status >= 200
    assert size > 0
    assert content is None

    status, size, content = await rust_request.request_file(url, {}, None, True)  # type: ignore
    assert status >= 200
    assert size > 0
    assert content is not None
