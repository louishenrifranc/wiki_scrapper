import urllib
import array
import MySQLdb
from infobox import infobox
from bs4 import BeautifulSoup

dict = {}

url = 'http://en.wikipedia.org/wiki/List_of_Bollywood_films_of_2014'
page = urllib.urlopen(url)
soup = BeautifulSoup(page.read())
movieList = soup.findAll('table',{'class','wikitable'})

if movieList is None:
    print "movieList is empty \n"
else:
    for list in movieList:
        rows = list.findAll('tr')
        for row in rows[:3]:
            #print row
            cells = row.findAll('td')
            i = len(cells)
            print 'i = ',i
            if i>=4:
                if cells[i-4].find('a') != None:
                    print 'a : ',cells[i-4].find('a')['href']
                    dict = infobox('http://en.wikipedia.org'+cells[i-4].find('a')['href'])
                else:
                    dict['title'] = unicode(cells[i-4].find(text=True))
                    dict['genre'] = unicode(cells[i-3].find(text=True))
                    dict['Director'] = unicode(cells[i-2].find(text=True))
                    cast = cells[i-1].find(text=True)
                    if (i-4) >=1:
                        release = unicode(cells[i-5].find(text=True))
                        if(i-5) ==1:
                            month = unicode(cells[i-6].find(text=True))
                    print 'name: ',dict['title']
                    print 'director: ',unicode(director), 'genre: ',unicode(genre)
                    print 'month: ',unicode(month), 'release: ',unicode(release)
