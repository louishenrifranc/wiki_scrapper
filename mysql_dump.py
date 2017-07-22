import xml.etree.ElementTree as ET
from collections import namedtuple
import urllib.parse
import pymysql
import os

Movie = namedtuple("Movie", "title abstract director starrings")


def get_movie_info_from_folder(folder):
    """
        Iterate over all movies info saved in XML file in {{ folder }}
    Args:
        folder (str): Directory containing the xml files
    """
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
                if len(attributes['title']) > 0:
                    yield Movie(**attributes)


class DialogueDbHelper:
    def __init__(self, connection):
        self.connection = connection

        self.directed_relationship_type_id = self.add_entry("relationship_type", "directed")
        self.played_relationship_type_id = self.add_entry("relationship_type", "played")

    @property
    def insert_movie_command(self):
        return self.get_insert_mysql_command(
            table_name="'movie'",
            table_columns=["'movieName'", "'movieYear'", "'movieLength'", "'movieSummary'"],
            entry_type=["%s", "%d", "%d", "%s"]
        )

    @property
    def insert_celebrity_command(self):
        return self.get_insert_mysql_command(
            table_name="'celebrity'",
            table_columns=["'celebrityName'"],
            entry_type=["%s"])

    @property
    def insert_relationship_command(self):
        return self.get_insert_mysql_command(
            table_name="'relationship'",
            table_columns=["'movieId'", "'celebrityId'", "'relationshipTypeId'"],
            entry_type=["%d", "%d", "%d"]
        )

    @property
    def insert_relationship_type_command(self):
        return self.get_insert_mysql_command(
            table_name="'relationshipType'",
            table_columns=["'relationshipType'"],
            entry_type=["%s"]
        )

    @staticmethod
    def get_insert_mysql_command(table_name, table_columns, entry_type):
        mysql_command = """
              INSERT INTO `{}`
                ({})
              VALUES ({})
            """.format(
            table_name,
            ",".join(table_columns),
            ",".join(entry_type))
        return mysql_command

    def add_entry(self, type, values):
        """Add an entry to the database"""
        with self.connection.cursor() as cursor:
            if type == "relationship_type":
                cursor.execute(self.insert_relationship_type_command, values)
            elif type == "relation":
                cursor.execute(self.insert_relationship_command, values)
            elif type == "celebrity":
                cursor.execute(self.insert_celebrity_command, values)
            elif type == "movie":
                cursor.execute(self.insert_movie_command, values)
            else:
                raise NotImplementedError
            return cursor.lastrowid


def parse_movie_and_add_to_database(movie: Movie, db_helper: DialogueDbHelper):
    """
        Parse movie information (from url to real value) and insert them to the database
    Args:
        movie (namedtuple): Represent all the relevant information about a single movie
        db_helper (DialogueDbHelper): helper to insert into the database
    """

    def parse(x): return urllib.parse.urlparse(x).path.split('/')[-1]

    abstract = movie.abstract
    director = parse(movie.director)
    starrings = [parse(starring) for starring in movie.starrings.split('\n')]
    title = parse(movie.title)

    # Insert movie
    movie_id = db_helper.add_entry("movie", (title, None, None, abstract))
    # Insert actors
    for starring in starrings:
        celebrity_id = db_helper.add_entry("celebrity", (starring))
        db_helper.add_entry("relation", (movie_id, celebrity_id, db_helper.played_relationship_type_id))

    # Insert director
    celebrity_id = db_helper.add_entry("celebrity", (director))
    db_helper.add_entry("relation", (movie_id, celebrity_id, db_helper.directed_relationship_type_id))


if __name__ == "__main__":
    connection = pymysql.cursors(db='dialoguedb',
                                 user='root',
                                 passwd='password',
                                 host='localhost')
    insertor = DialogueDbHelper(connection=connection)

    movie_info_gen = get_movie_info_from_folder(".movie")
    for movie_info in movie_info_gen:
        parse_movie_and_add_to_database(movie_info, insertor)
