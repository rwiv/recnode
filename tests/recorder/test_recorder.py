import requests
from pyutils import load_dotenv, path_join, find_project_root


load_dotenv(path_join(find_project_root(), "dev", ".env"))

uid = ""
worker_url = "http://localhost:9083/api/recordings"


def test_post_record():
    print()
    record_id = ""
    res = requests.post(f"{worker_url}:{record_id}")
    print(res.text)


def test_delete_record():
    print()
    record_id = ""
    res = requests.delete(f"{worker_url}:{record_id}")
    print(res.text)
