## Scripts
* ```wiki_luigi.py```: Return a list of wikipeia page related to movies. In progress.
* ```dbpedia_luigi.py```: Return a bunch of XML file containing reference to a movie, starring actors, abstract, and movie director
* ```mysql_dump```: Dump the movies into the mysql database

## Usage
```
pip install -r requirements.txt
python3 dbpedia_luigi.py

python3 mysql_dump.py
# clean the xml files
rm -rf .movie
```
