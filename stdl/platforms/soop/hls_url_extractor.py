from stdl.downloaders.hls.hls_url_extractor import HlsUrlExtractor
from stdl.downloaders.hls.parser import Resolution
from stdl.utils.url import get_origin


class SoopHlsUrlExtractor(HlsUrlExtractor):
    def _get_base_url(self, m3u8_url: str, r: Resolution) -> str:
        return f"{get_origin(m3u8_url)}{r.name}"
