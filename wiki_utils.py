from bs4 import BeautifulSoup
import requests
import wptools
from urllib3 import util
import re
from SPARQLWrapper import XML


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
        elif "starring" and "director" and "released" in info_box:
            return True
    return False


def get_movie_xml_info(sparql, offset, limit):
    sparql.setQuery("""
SELECT ?abstract ?title ?director ?link
        (group_concat(?starring;separator="\\n") as ?starrings) 
        (group_concat(?genre;separator="\\n")    as ?genres   )
        (group_concat(?subject;separator="\\n")  as ?subjects )
WHERE {
?title rdf:type             <http://dbpedia.org/ontology/Film> ;
       dbo:abstract         ?abstract ;
       dbo:director         ?director ;
       dbo:starring         ?starring ;
       dbp:genre            ?genre    
OPTIONAL{?title   foaf:primaryTopic    ?link    }
OPTIONAL{?title   dbp:subject          ?subject  }
FILTER (langMatches(lang(?abstract),"en"))
}  GROUP BY ?abstract ?title ?director ?link
LIMIT 1000 OFFSET """ + str(offset))

    sparql.setReturnFormat(XML)
    results = sparql.query().convert()

    return results.toxml()
