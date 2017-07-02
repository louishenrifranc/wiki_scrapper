from SPARQLWrapper import XML


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
LIMIT """ + str(limit) + """ OFFSET """ + str(offset))

    sparql.setReturnFormat(XML)
    results = sparql.query().convert()

    return results.toxml()
