from typing import Generator

from config import SPOTIFY_MARKET, SPOTIFY_SEARCH_QUERY
from extractors.base_extractor import BASE_URL, BaseExtractor


class AlbumsExtractor(BaseExtractor):
    resource_name = "albums"

    def extract(self) -> Generator[dict, None, None]:
        yield from self.paginate(
            url=f"{BASE_URL}/search",
            params={
                "q": SPOTIFY_SEARCH_QUERY,
                "type": "album",
                "market": SPOTIFY_MARKET,
            },
        )
