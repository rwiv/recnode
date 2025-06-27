import asyncio
import threading
from abc import ABC, abstractmethod

from .stream_types import RecordingContext
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_schema import RecordingState, RecordingStatus
from ..stream.stream_helper import StreamHelper
from ...data.live import LiveState
from ...fetcher import PlatformFetcher
from ...file import ObjectWriter
from ...utils import AsyncHttpClient, ProxyConnectorConfig


class StreamRecorder(ABC):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        incomplete_dir_path: str,
        proxy: ProxyConnectorConfig | None,
    ):
        self._state = RecordingState()
        self._status: RecordingStatus = RecordingStatus.WAIT

        self._writer = writer
        fetcher_http = AsyncHttpClient(
            timeout_sec=30,
            retry_limit=3,
            retry_delay_sec=0.5,
            use_backoff=True,
            print_error=True,
            proxy=proxy,
        )
        self._fetcher = PlatformFetcher(fetcher_http)
        self._helper = StreamHelper(
            args=args,
            state=self._state,
            writer=writer,
            fetcher=self._fetcher,
            incomplete_dir_path=incomplete_dir_path,
        )

        self.ctx: RecordingContext = self._helper.get_ctx(live)
        self.is_done = False
        self.recording_thread: threading.Thread | None = None

    def record(self):
        self.recording_thread = threading.Thread(target=self.__record_with_thread)
        self.recording_thread.name = f"recording:{self.ctx.record_id}"
        self.recording_thread.start()

    def __record_with_thread(self):
        asyncio.run(self._record())

    def cancel(self):
        self._state.cancel()

    @abstractmethod
    async def _record(self):
        pass

    @abstractmethod
    async def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        pass
