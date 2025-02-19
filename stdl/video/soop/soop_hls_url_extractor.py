from pyutils import get_origin_url

from ...utils.hls.hls_url_extractor import HlsUrlExtractor
from ...utils.hls.parser import Resolution


class SoopHlsUrlExtractor(HlsUrlExtractor):
    def _get_base_url(self, m3u8_url: str, r: Resolution) -> str:
        return f"{get_origin_url(m3u8_url)}{r.name}"
