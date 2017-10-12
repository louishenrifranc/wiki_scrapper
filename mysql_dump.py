import pymysql
import os
import re

import xml.etree.ElementTree as ET

from collections import namedtuple
from urllib.parse import urlparse


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
    MYSQL_ARGUMENTS = {
            "movie": {
                "table_name": "movie",
                "table_columns": ["movieName", "movieYear", "movieLength", "movieSummary"],
                "entry_type": ["%s", "%s", "%s", "%s"]
            },
            "celebrity": {
                "table_name": "celebrity",
                "table_columns": ["celebrityName"],
                "entry_type": ["%s"]
            },
            "relationship": {
                "table_name": "relationship",
                "table_columns": ["movieId", "celebrityId", "relationshipTypeId"],
                "entry_type": ["%s", "%s", "%s"]
            },
            "relationship_type": {
                "table_name": "relationshipType",
                "table_columns": ["relationshipType"],
                "entry_type": ["%s"]
            }
        }

    def __init__(self, connection):
        self.connection = connection

        self.directed_relationship_type_id = self.add_entry("relationship_type", "directed")
        self.played_relationship_type_id = self.add_entry("relationship_type", "played")


    @staticmethod
    def get_select_mysql_command(table_name, table_columns, entry_type):
        mysql_command = "SELECT {} FROM {} WHERE {};".format(
                table_name+"Id",
                table_name,
                " AND ".join([tab+" = "+typ for tab, typ in zip(table_columns, entry_type)]))
        return mysql_command

    @staticmethod
    def get_insert_mysql_command(table_name, table_columns, entry_type):
        mysql_command = "INSERT INTO {} ({}) VALUES ({});""".format(
            table_name,
            ",".join(table_columns),
            ",".join(entry_type))
        return mysql_command

    def add_entry(self, table_name, values):
        """Add an entry to the database"""
        with self.connection.cursor() as cursor:
            result = cursor.execute(self.get_select_mysql_command(**DialogueDbHelper.MYSQL_ARGUMENTS[table_name]), values)
            if result is 0:
                cursor.execute(self.get_insert_mysql_command(**DialogueDbHelper.MYSQL_ARGUMENTS[table_name]), values)
                cursor.execute(self.get_select_mysql_command(**DialogueDbHelper.MYSQL_ARGUMENTS[table_name]), values)
            self.connection.commit()
            return cursor.fetchone()[0]


def parse_movie_and_add_to_database(movie, db_helper):
    """
        Parse movie information (from url to real value) and insert them to the database
    Args:
        movie (namedtuple): Represent all the relevant information about a single movie
        db_helper (DialogueDbHelper): helper to insert into the database
    """

    def parse(x): return re.sub('\(.*\)', '', " ".join(urlparse(x).path.split('/')[-1].split('_')))

    abstract = movie.abstract
    director = parse(movie.director)
    starrings = [parse(starring) for starring in movie.starrings.split('\n')]
    title = parse(movie.title)
    # Insert movie
    movie_id = db_helper.add_entry("movie", (title, 1, 1, abstract))
    # Insert actors
    for starring in starrings:
        celebrity_id = db_helper.add_entry("celebrity", (starring))
        db_helper.add_entry("relationship", (movie_id, celebrity_id, db_helper.played_relationship_type_id))

    # Insert director
    celebrity_id = db_helper.add_entry("celebrity", (director))
    db_helper.add_entry("relationship", (movie_id, celebrity_id, db_helper.directed_relationship_type_id))


if __name__ == "__main__":
    connection = pymysql.connect(db='dialoguedb',
                                 user='dialogueadmin',
                                 passwd='*********', # to set
                                 host='localhost',
                                 charset='utf8')
    dialoguedb_insertor = DialogueDbHelper(connection=connection)

    movie_info_gen = get_movie_info_from_folder(".movie")
    for i, movie_info in enumerate(movie_info_gen):
        parse_movie_and_add_to_database(movie_info, dialoguedb_insertor)
    dialoguedb_insertor.connection.close()
