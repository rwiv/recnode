import json
import sys
import http.client


def create_req(req_type: str, userid: str, once: bool = True):
    if req_type == "chzzk_live":
        return {
            "reqType": req_type,
            "chzzkLive": {
                "uid": userid,
                "once": once,
            }
        }
    elif req_type == "afreeca_live":
        return {
            "reqType": req_type,
            "afreecaLive": {
                "userId": userid,
                "once": once,
            }
        }
    else:
        raise ValueError(f"Unknown request type: {req_type}")


def request(host: str, data: dict, path: str = "/stdl"):
    conn = http.client.HTTPConnection(host)
    conn.request(
        "POST", path, json.dumps(data),
        {"Content-Type": "application/json"}
    )
    return conn.getresponse()


if __name__ == "__main__":
    req = create_req(sys.argv[2], sys.argv[3], True)
    res = request(sys.argv[1], req)
    # res = request(sys.argv[1], req, "/stdl-proton")
    print(f"{res.status} {res.reason}")
