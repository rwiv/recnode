import asyncio
import threading
from abc import ABC, abstractmethod

from .stream_types import RequestContext
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_schema import RecordingState, RecordingStatus
from ..stream.stream_helper import StreamHelper
from ...data.live import LiveState
from ...fetcher import PlatformFetcher
from ...file import ObjectWriter


class StreamRecorder(ABC):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        incomplete_dir_path: str,
    ):
        self.live = live
        self.writer = writer

        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.fetcher = PlatformFetcher()
        self.helper = StreamHelper(
            live=live,
            args=args,
            state=self.state,
            writer=writer,
            fetcher=self.fetcher,
            incomplete_dir_path=incomplete_dir_path,
        )
        self.ctx: RequestContext = self.helper.get_ctx(live)

        self.is_done = False
        self.recording_thread: threading.Thread | None = None

    def record(self):
        self.recording_thread = threading.Thread(target=self.__record_with_thread)
        self.recording_thread.name = f"recording:{self.live.id}"
        self.recording_thread.start()

    def __record_with_thread(self):
        asyncio.run(self._record())

    @abstractmethod
    async def _record(self):
        pass

    @abstractmethod
    async def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        pass

    async def check_tmp_dir(self):
        await self.helper.check_tmp_dir(self.ctx)
