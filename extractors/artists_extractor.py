from typing import Generator

from config import SPOTIFY_MARKET, SPOTIFY_SEARCH_QUERY_ARTISTS
from extractors.base_extractor import BASE_URL, BaseExtractor


class ArtistsExtractor(BaseExtractor):
    resource_name = "artists"

    def extract(self) -> Generator[dict, None, None]:
        yield from self.paginate(
            url=f"{BASE_URL}/search",
            params={
                "q": SPOTIFY_SEARCH_QUERY_ARTISTS,
                "type": "artist",
                "market": SPOTIFY_MARKET,
            },
        )
