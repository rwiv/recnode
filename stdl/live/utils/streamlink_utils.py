import json
import logging

from streamlink.options import Options
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream

from ..spec.recording_arguments import StreamLinkSessionArgs


def get_session(args: StreamLinkSessionArgs) -> Streamlink:
    options = Options()
    options.set("stream-timeout", args.read_session_timeout_sec)
    if args.options is not None:
        for key, value in args.options.items():
            options.set(key, value)

    session = Streamlink(options=options)
    if args.cookies is not None:
        data: list[dict] = json.loads(args.cookies)
        for cookie in data:
            session.http.cookies.set(cookie["name"], cookie["value"])
    return session


def get_streams(url: str, args: StreamLinkSessionArgs) -> dict[str, HLSStream] | None:
    streams = get_session(args).streams(url)
    if streams is None or len(streams) == 0:
        return None
    return streams


def disable_streamlink_log():
    logging.getLogger("streamlink").setLevel(logging.CRITICAL)
