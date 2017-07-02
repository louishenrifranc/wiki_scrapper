from wiki_utils import get_all_urls_on_page, is_movie_wiki_page
import urllib3
import luigi
import os
import logging


class LuigiTaskWithLinkParameter(luigi.Task):
    link = luigi.Parameter()

    def output(self):
        path = "." + self.__class__.__name__.lower()
        os.makedirs(path, exist_ok=True)
        return luigi.LocalTarget(
            os.path.join(path, urllib3.util.parse_url(self.link).path.split("/")[-1].lower() + ".txt"))


class GetAllURLWorker(LuigiTaskWithLinkParameter):
    def run(self):
        try:
            urls = get_all_urls_on_page(self.link)
        except Exception as e:
            raise e

        with self.output().open("w") as f:
            for url in urls:
                f.write("{}\n".format(url))


class CheckIsMoviePageWorker(LuigiTaskWithLinkParameter):
    def run(self):
        try:
            is_movie_page = is_movie_wiki_page(self.link)
        except Exception as e:
            raise e

        with self.output().open("w") as f:
            f.write(self.link + " : " + str(is_movie_page))


class SearchMovieLinkWorker(luigi.Task):
    def output(self):
        file_name = "list_movie.txt"
        return luigi.LocalTarget(os.path.join("." + self.__class__.__name__.lower(), file_name))

    def requires(self):
        original_page_url = "https://en.wikipedia.org/wiki/Lists_of_films"
        return GetAllURLWorker(original_page_url)

    def run(self):
        initial_links = self.input().open('r').read()
        get_all_url_worker = [GetAllURLWorker(initial_link) for initial_link in initial_links.strip().split("/n")]
        yield get_all_url_worker

        all_urls = set()
        for worker in get_all_url_worker:
            for url in worker.input().open('r').read().strip().split("\n"):
                all_urls.add(url)

        all_urls = list(all_urls)
        check_movie_page_worker = [CheckIsMoviePageWorker(new_link) for new_link in all_urls]
        yield check_movie_page_worker

        movie_page_urls = list()
        for worker in get_all_url_worker:
            for output in worker.input().open('r').read().strip().split():
                if output[-1] == "True":
                    movie_page_urls.append(output[0])

        with self.output().open("w") as f:
            for url in movie_page_urls:
                f.write("{}\n".format(url))


if __name__ == '__main__':
    raise NotImplementedError("Not finished, neither fully working")
    os.makedirs(".log", exist_ok=True)
    logging.basicConfig(filename=os.path.join('.log', 'error.log'), level=logging.DEBUG)
    luigi.run(['SearchMovieLinkWorker'])
