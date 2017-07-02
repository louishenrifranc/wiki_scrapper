import luigi
import os
from wiki_utils import get_movie_xml_info
from argparse import ArgumentParser
from SPARQLWrapper import SPARQLWrapper

LIMIT_RETRIEVAL_DBPEDIA = 1000
sparql = SPARQLWrapper("http://dbpedia.org/sparql")


class Worker(luigi.Task):
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
        # If no error, save the info in a json file
        with self.output().open("w") as f:
            f.write(movies_info)


class Scheduler(luigi.Task):
    starting_index = luigi.IntParameter()
    num_retrieved_movie = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget('final_file.txt')

    def requires(self):
        """
        The Scheduler task will never be finished until all this requirement are done
        """
        return [Worker(index) for index in
                range(self.starting_index, self.num_retrieved_movie, LIMIT_RETRIEVAL_DBPEDIA)]

    def run(self):
        """
        """
        with self.output().open('w') as f:
            f.write('Finish')


if __name__ == '__main__':
    args = ArgumentParser()
    args.add_argument('--num_retrieved_movie', type=int, default=100000, help='Number of movies')
    args.add_argument('--starting_index', type=int, default=0,
                      help='Starting index')

    args = args.parse_args()

    luigi.run(['Scheduler', '--starting-index', str(args.starting_index),
               '--num-retrieved-movie', str(args.num_retrieved_movie), '--local-scheduler'])
