import luigi
import os
from dbpedia_utils import get_movie_xml_info
from argparse import ArgumentParser
from SPARQLWrapper import SPARQLWrapper

# Each XML file will contain 1000 movie each
LIMIT_RETRIEVAL_DBPEDIA = 1000
sparql = SPARQLWrapper("http://dbpedia.org/sparql")


class Worker(luigi.Task):
    """
        Given a {{ current_index }}, return information about movies in DBPedia which are ranged from {{ current_index }}
                to {{ current_index }}+ {{ LIMIT_RETRIEVAL_DBPEDIA }}
    """
    current_index = luigi.IntParameter()

    def output(self):
        dir_name = os.path.join('.movie')
        os.makedirs(dir_name, exist_ok=True)

        file_path = os.path.join(dir_name, "movie_{}-to-{}.xml".format(self.current_index,
                                                                       self.current_index + LIMIT_RETRIEVAL_DBPEDIA))

        return luigi.LocalTarget(file_path)

    def run(self):
        movies_info = get_movie_xml_info(sparql=sparql,
                                         offset=self.current_index,
                                         limit=LIMIT_RETRIEVAL_DBPEDIA)
        with self.output().open("w") as f:
            f.write(movies_info)


class Scheduler(luigi.Task):
    """
        Given a starting index, return information about {{ num_retrieved_movie }} movies starting at {{ starting_index }}
    """
    starting_index = luigi.IntParameter()
    num_retrieved_movie = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget('final_file.txt')

    def requires(self):
        return [Worker(idx) for idx in range(self.starting_index, self.num_retrieved_movie, LIMIT_RETRIEVAL_DBPEDIA)]

    def run(self):
        with self.output().open('w') as f:
            f.write('Finish')


if __name__ == '__main__':
    args = ArgumentParser()
    args.add_argument('--num_retrieved_movie', type=int, default=80000, help='Number of movies to retrieve')
    args.add_argument('--starting_index', type=int, default=0,
                      help='Starting index in DBpedia database')
    args = args.parse_args()

    luigi.run(['Scheduler', '--starting-index', str(args.starting_index),
               '--num-retrieved-movie', str(args.num_retrieved_movie), '--local-scheduler'])
