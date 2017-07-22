from SPARQLWrapper import XML


def get_movie_xml_info(sparql, offset, limit):
    """
        Retrieve a list of movie and their relative information using dbpedia online API
    Args:
        sparql: a SPARQLWrapper instance. Used to contact API
        offset (int): Movie list is retrieve sequentially by batch of {{ limit }}. {{ offset }} represent where the scrapper
            restart
        limit (int): number of movie to retrieve in a batch (i.e a call)

    Returns
        (str): Movie information formatted as a xml file
    """

    sparql.setQuery("""
        SELECT ?abstract ?title ?director
                (group_concat(?starring;separator="\\n") as ?starrings)
        WHERE {
        ?title rdf:type             <http://dbpedia.org/ontology/Film> ;
               dbo:abstract         ?abstract ;
               dbo:director         ?director ;
               dbo:starring         ?starring
        FILTER (langMatches(lang(?abstract),"en"))
        }  GROUP BY ?abstract ?title ?director
        LIMIT """ + str(limit) + """ OFFSET """ + str(offset))

    sparql.setReturnFormat(XML)
    results = sparql.query().convert()

    return results.toxml()
