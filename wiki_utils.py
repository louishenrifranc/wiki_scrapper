from bs4 import BeautifulSoup
import requests
import wptools
from urllib3 import util
import re
from cache import Cache as cacher


def get_all_urls_on_page(url_page: str):
    r = requests.get(url_page)
    if r.status_code == 404:
        return None

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.findAll("a")
    return [" http://" + util.parse_url(url_page).hostname + link["href"] for link in links if "href" in link.attrs]


@cacher
def _cached_get_all_urls_on_page(url_page: str):
    return get_all_urls_on_page(url_page)


def is_movie_wiki_page(url: str):
    if re.search(url, "_\(([0-9a-zA-Z]*_)?film\)$"):
        return True
    else:
        page_name = util.parse_url(url).path.split("/")[-1]
        try:
            page = wptools.page(page_name).get(show=False)
        except LookupError:
            return False
        info_box = page.infobox
        if info_box is None:
            return False
        elif "starring" and "director" in info_box:
            return True
    return False


@cacher
def _cached_is_movie_wiki_page(url: str):
    return is_movie_wiki_page(url)


def get_all_movie_wiki_url(base_url: str):
    base_links = get_all_urls_on_page(base_url)

    global movie_links
    movie_links = []
    # p = Pool()
    for base_link in base_links:
        potential_movie_links = get_all_urls_on_page(base_link)
        is_movie_links = list()
        for potential_movie_link in potential_movie_links:
            is_movie_links.append(is_movie_wiki_page(potential_movie_link))

        for is_movie, url in zip(is_movie_links, potential_movie_links):
            if is_movie:
                movie_links.append(url)
                print("Found movie:", url)


if __name__ == '__main__':
    # get_all_movie_wiki_url("https://en.wikipedia.org/wiki/Lists_of_films")


    print(is_movie_wiki_page("https://en.wikipedia.org/wiki/Transformers:_Age_of_Extinction"))
