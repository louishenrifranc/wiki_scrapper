from wiki_utils import get_all_urls_on_page, is_movie_wiki_page
import urllib3
import luigi
import os
import logging


class LuigiTaskLinkParameter(luigi.Task):
    link = luigi.Parameter()

    def output(self):
        """
        Create a file for the work
        :return: Name of a file
        """
        path = "." + self.__class__.__name__.lower()
        os.makedirs(path, exist_ok=True)
        return luigi.LocalTarget(
            os.path.join(path, urllib3.util.parse_url(self.link).path.split("/")[-1].lower() + ".txt"))


class WorkerGetAllURL(LuigiTaskLinkParameter):
    def run(self):
        urls = get_all_urls_on_page(self.link)
        with self.output().open("w") as f:
            for url in urls:
                f.write("{}\n".format(url))


class WorkerFilterIsMoviePage(LuigiTaskLinkParameter):
    def run(self):
        is_movie_page = is_movie_wiki_page(self.link)
        with self.output().open("w") as f:
            f.write(self.link + " : " + str(is_movie_page))


class Scheduler(LuigiTaskLinkParameter):
    def requires(self):
        return WorkerGetAllURL(self.link)

    def run(self):
        all_links = self.input().open('r').read()
        for link in all_links.strip().split("/n"):
            try:
                potential_movie_links = yield WorkerGetAllURL(link)

                potential_movie_links = potential_movie_links.open('r').read()
                for link in potential_movie_links.strip().split():
                    yield WorkerFilterIsMoviePage(link)
            except Exception as e:
                logging.debug('Failed with message {}'.format(str(e)))
                continue


if __name__ == '__main__':
    os.makedirs(".log", exist_ok=True)
    logging.basicConfig(filename=os.path.join('.log', 'error.log'), level=logging.DEBUG)
    luigi.run(['Scheduler', '--link', "https://en.wikipedia.org/wiki/Lists_of_films", "--local-scheduler"])
