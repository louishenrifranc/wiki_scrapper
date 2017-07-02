from SPARQLWrapper import XML


def get_movie_xml_info(sparql, offset, limit):
    """
        Retrieve movie information
    :param sparql: a SPARQLWrapper instance
    :param offset: from where to restart
    :param limit: number of movie to retrieve
    :return: movie information formatted as a xml file
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
