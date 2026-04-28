from typing import Generator

from config import SPOTIFY_MARKET, SPOTIFY_SEARCH_QUERY
from extractors.base_extractor import BASE_URL, BaseExtractor


class TracksExtractor(BaseExtractor):
    resource_name = "tracks"

    def extract(self) -> Generator[dict, None, None]:
        yield from self.paginate(
            url=f"{BASE_URL}/search",
            params={
                "q": SPOTIFY_SEARCH_QUERY,
                "type": "track",
                "market": SPOTIFY_MARKET,
            },
        )
