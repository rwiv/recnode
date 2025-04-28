import logging

from pyutils import to_cookie_dict, CookieDict
from streamlink.options import Options
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream

from ..schema.recording_arguments import StreamLinkSessionArgs
from ..schema.recording_constants import DEFAULT_STREAM_TIMEOUT_SEC


def get_session(args: StreamLinkSessionArgs) -> Streamlink:
    options = Options()
    stream_timeout_sec = args.stream_timeout_sec or DEFAULT_STREAM_TIMEOUT_SEC
    options.set("stream-timeout", stream_timeout_sec)
    if args.options is not None:
        for key, value in args.options.items():
            options.set(key, value)

    session = Streamlink(options=options)
    if args.cookie_header is not None:
        data: list[CookieDict] = to_cookie_dict(args.cookie_header)
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
