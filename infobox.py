from bs4 import BeautifulSoup
import re
import urllib
import wikipedia
import array
import MySQLdb
import os

#db=MySQLdb.Connect(host="127.0.0.1",port=8888,user='myuser',passwd="12345",db="hollywood")
#cursor=db.cursor()

#movie_name="rang de basanti"
#m1=wikipedia.page(movie_name)
#title=m1.title
#summary=m1.summary
#url=m1.url
def infobox(url):
    print "in infobox \n"
    dict={}
    page=urllib.urlopen(url)
    soup=BeautifulSoup(page.read())
    infobox=soup.find('table',{'class':'infobox vevent'})
    if infobox is None:
        print "infobox is null"
    else:
        dict['title'] = unicode(infobox.find('th',{'class','summary'}).find(text=True))
        print dict['title'],'\n'
        image =infobox.find("a",{"class":"image"})
        if image is not None:
            imageUrl = image.find("img").get("src")
            urllib.urlretrieve("http:"+imageUrl ,dict['title']+".jpg")
            dict['imageUrl'] = dict['title']
        data=[]
        rows=infobox.findAll('tr')
        for row in rows[2:]:
            heads=row.findAll('th')
            cols=row.findAll('td')
            heads=[ele.get_text().strip() for ele in heads]
            #print heads, "\n"
            cols=[ele.get_text().strip() for ele in cols]
            #print cols, '\n'
            data.append([ele for ele in heads if ele])
            data.append([ele for ele in cols if ele])
            heads= ''.join(heads)
            #print heads
            cols = ''.join(cols)
            #print cols
            cols=cols.split('\n')
            dict[heads]= cols
    print dict
    return dict
    
