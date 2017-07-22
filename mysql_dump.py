import pymysql
import os
import xml.etree.ElementTree as ET
from collections import namedtuple
import urllib.parse
Movie = namedtuple("Movie", "title abstract director starrings")


def get_movie_info_from_folder(folder):
    if not os.path.exists(folder):
        raise ValueError("folder does not exists")
    for file in os.listdir(folder):
        filename = os.path.join(folder, file)
        if os.path.isfile(filename):
            e = ET.parse(filename).getroot()
            for result in e[1]:
                attributes = dict()
                for bindings in result:
                    attributes[bindings.attrib['name']] = bindings[0].text
                yield Movie(**attributes)


def add_to_database(movie:  Movie, connection):
    def parse(x): return urllib.parse.urlparse(x).path.split('/')[-1]
    abstract = movie.abstract
    director = parse(movie.director)
    starrings = [parse(starring)
                 for starring in movie.starrings.split('\n')]
    title = parse(movie.title)
    import ipdb
    ipdb.set_trace()  # XXX BREAKPOINT

    def insert_into_tables(table_name, table_columns, entries):
        mysql_command = """
            INSERT INTO {}
                ({})
            VALUES
                {}
            """.format(
                table_name,
                ",".join(table_columns),
                "".join(["(" + entry + ")," for entry in entries])
        )
        return mysql_command

    insert_celebrities = insert_into_tables(
        table_name="celebrity",
        table_columns="celebrityName",
        entries=starrings + [director])

    insert_movie = insert_into_tables(
        table_name="movie",
        table_columns=["movieName", "movieYear", "movieLength", "movieSummary"],
        entries=[",".join([title, None, None, abstract])]
    )


if __name__ == "__main__":
    # connection = pymysql.cursors(db='dialoguedb',
    #                              user='root',
    #                              passwd='password',
    #                              host='localhost')

    movie_info_gen = get_movie_info_from_folder(".movie")
    for movie_info in movie_info_gen:
        print(movie_info)
        add_to_database(movie_info, None)
