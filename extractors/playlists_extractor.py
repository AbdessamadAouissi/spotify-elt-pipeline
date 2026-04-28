from typing import Generator

from config import SPOTIFY_MARKET, SPOTIFY_SEARCH_QUERY_PLAYLISTS
from extractors.base_extractor import BASE_URL, BaseExtractor


class PlaylistsExtractor(BaseExtractor):
    resource_name = "playlists"

    def extract(self) -> Generator[dict, None, None]:
        yield from self.paginate(
            url=f"{BASE_URL}/search",
            params={
                "q": SPOTIFY_SEARCH_QUERY_PLAYLISTS,
                "type": "playlist",
                "market": SPOTIFY_MARKET,
            },
        )
