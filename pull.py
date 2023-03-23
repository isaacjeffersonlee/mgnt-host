import time
from rich.progress import Progress
from rich.console import Console
import pandas as pd
import requests
import aiohttp
import asyncio

BASE_URL = "https://yts.mx/api/v2/"
SAVE_DIR = "./output/"
console = Console()


def yts_request(endpoint: str, params: dict) -> dict:
    r = requests.get(url=BASE_URL + endpoint, params=params)
    assert r.status_code == 200, f"Error: status code {r.status_code}"
    return r.json()


def parse_magnet_link(torrent_hash: str, torrent_url: str) -> str:
    trackers = [
        "udp://open.demonii.com:1337/announce",
        "udp://tracker.openbittorrent.com:80",
        "udp://tracker.coppersurfer.tk:6969",
        "udp://glotorrents.pw:6969/announce",
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://torrent.gresille.org:80/announce",
        "udp://p4p.arenabg.com:1337",
        "udp://tracker.leechers-paradise.org:6969",
    ]
    magnet_link = f"magnet:?xt=urn:btih:{torrent_hash}&dn={torrent_url}"
    for tracker in trackers:
        magnet_link += f"&tr={tracker}"

    return magnet_link


async def get_movies_py_page(
    page_num: int, limit: int, session: aiohttp.ClientSession
) -> dict:
    r = await session.get(
        url=BASE_URL + "list_movies.json",
        params={"limit": limit, "query_term": "", "page": page_num},
    )
    r_json = await r.json()

    return r_json


async def get_all_movies(results_per_page: int = 20) -> list[dict]:
    tasks = []
    page_num = 1  # Index starts from 1
    # Use a normal synchronous request to get the total movie count
    r = requests.get(
        url=BASE_URL + "list_movies.json",
        params={"limit": results_per_page, "query_term": "", "page": page_num},
    )
    movie_count = r.json()["data"]["movie_count"]

    async with aiohttp.ClientSession() as session:
        results_count = 0
        while results_count < movie_count:
            tasks.append(get_movies_py_page(page_num, results_per_page, session))
            results_count += results_per_page
            page_num += 1

        with console.status("Waiting for request responses...", spinner="dots"):
            all_movies = await asyncio.gather(*tasks, return_exceptions=True)

    return all_movies


def update_database() -> None:
    start_time = time.perf_counter()
    all_pages = asyncio.run(get_all_movies(results_per_page=50))
    end_time = time.perf_counter()
    console.print("")
    console.log(
        f"[green]Finished requesting database in:[/green] {round(end_time-start_time, 2)}s"
    )
    all_movies = []
    with console.status("Unpacking and saving results...", spinner="dots10"):
        for r in all_pages:
            all_movies += r["data"]["movies"]

        console.log(f"Found {len(all_movies)} movies!")
        df = pd.DataFrame.from_dict(data=all_movies)
        cols_to_keep = [
            "imdb_code",
            "title",
            "year",
            "rating",
            "runtime",
            "genres",
            "summary",
            "language",
            "mpa_rating",
            "state",
            "date_uploaded",
        ]
        # df.drop(columns=["date_uploaded", "date_uploaded_unix", "url"], inplace=True)
        # torrents = df["torrents"].explode().dropna()
        # torrent_df = pd.DataFrame(torrents.tolist(), index=torrents.index)
        # df = torrent_df.join(df.drop(columns=["torrents"]))
        df[cols_to_keep].to_parquet(SAVE_DIR + "df.parquet.gzip", compression="gzip")

    console.log(f"Done! Output saved to {SAVE_DIR + 'df.parquet.gzip'}")


if __name__ == "__main__":
    update_database()
