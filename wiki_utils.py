from bs4 import BeautifulSoup
import requests
import wptools
from urllib3 import util
import re


def get_all_urls_on_page(url: str):
    try:
        r = requests.get(url)
    except:
        return list()
    if r.status_code == 404:
        return list()

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.findAll("a")
    return [" http://" + util.parse_url(url).hostname + link["href"] for link in links if "href" in link.attrs]


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
