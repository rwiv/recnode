from aiohttp_socks import ProxyType
from pyutils import path_join
from redis.asyncio import Redis

from ..schema.recording_arguments import RecordingArgs
from ..stream.stream_recorder import StreamRecorder
from ..stream.stream_recorder_seg import SegmentedStreamRecorder
from ...common import PlatformType
from ...config import Env
from ...data.live import LocationType
from ...data.live import LiveState
from ...data.redis import create_redis_pool
from ...file import ObjectWriter
from ...utils import StreamLinkSessionArgs, ProxyConnectorConfig


class RecorderResolver:
    def __init__(self, env: Env, writer: ObjectWriter, my_public_ip: str):
        self.__env = env
        self.__writer = writer
        self.__my_public_ip = my_public_ip

    def create_recorder(self, state: LiveState) -> StreamRecorder:
        if state.platform == PlatformType.CHZZK:
            return self.__create_chzzk_recorder(state)
        elif state.platform == PlatformType.SOOP:
            return self.__create_soop_recorder(state)
        elif state.platform == PlatformType.TWITCH:
            return self.__create_twitch_recorder(state)
        else:
            raise ValueError("Invalid Request Type")

    def __create_chzzk_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://chzzk.naver.com/live/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_soop_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://play.sooplive.co.kr/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_twitch_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://www.twitch.tv/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_recorder(self, state: LiveState, url: str, cookie_header: str | None) -> StreamRecorder:
        return SegmentedStreamRecorder(
            live=state,
            args=RecordingArgs(
                live_url=url,
                session_args=StreamLinkSessionArgs(
                    cookie_header=cookie_header,
                    stream_timeout_sec=self.__env.stream.stream_timeout_sec,
                ),
                tmp_dir_path=self.__env.tmp_dir_path,
                seg_size_mb=self.__env.stream.seg_size_mb,
            ),
            incomplete_dir_path=path_join(self.__env.out_dir_path, "incomplete"),
            writer=self.__writer,
            redis_master=Redis(connection_pool=create_redis_pool(self.__env.redis_master)),
            redis_replica=Redis(connection_pool=create_redis_pool(self.__env.redis_replica)),
            redis_data_conf=self.__env.redis_data,
            req_conf=self.__env.req_conf,
            proxy=self.__create_proxy_connector_config(state.location),
        )

    def __create_proxy_connector_config(self, location: LocationType) -> ProxyConnectorConfig | None:
        if location == LocationType.LOCAL:
            return None

        host = self.__env.proxy.host
        if self.__env.proxy.use_my_ip:
            host = self.__my_public_ip
        if host is None:
            raise ValueError("Proxy host is not set")

        port = self.__env.proxy.port_overseas
        if location == LocationType.PROXY_DOMESTIC:
            port = self.__env.proxy.port_domestic

        return ProxyConnectorConfig(
            proxy_type=ProxyType.SOCKS5,
            host=host,
            port=port,
            username=self.__env.proxy.username,
            password=self.__env.proxy.password,
            rdns=self.__env.proxy.rdns,
        )
