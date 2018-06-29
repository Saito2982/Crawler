from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import pymysql
import configparser
import time
import re
import sys
import mysql.connector
import numpy as np

sys.setrecursionlimit(10000)
config = configparser.ConfigParser()
config.read('config.ini')
setting = config['setting']
conn = pymysql.connect(
    host=setting['host'],
    #unix_socket=setting['socket'],
    user=setting['user'],
    passwd=setting['passwd'],
    db=setting['db'],
    charset='utf8'
)

#conn = mysql.connector.connect(user='root', password='hiroyuki417+', host='localhost', database='movie')
cur = conn.cursor()
cur.execute("USE kakakucom")
domain = "http://kakaku.com/"


# change string type to year type
def year(str):
    return re.sub(r'(年)', '', str)


# change string type to int type
def minute(str):
    return re.sub(r'(分)', '', str)


def insertMovie(id, title, maker, series, link):
    query = "INSERT INTO iteminfo (id, title, maker, series, link) "
    query += "VALUES (%s,%s,%s,%s,%s)"
    print("insert")
    try:
        cur.execute(query, (id, title, maker, series, link))
    except Exception as e:
        print("failed insert movie")
        print(id, title, maker, series, link)
        print(e)
        return 0
    else:
        conn.commit()


def insertIndex(data, table, movie_id):
    query = "SELECT id FROM " + table + " WHERE name = '" + data + "'"
    cur.execute(query)
    id = cur.fetchone()
    if id is None:
        insertRelation(data, table)
        cur.execute(query)
        id = cur.fetchone()
    query = "INSERT INTO info_" + table + " (movie_id, " + table + "_id)"
    query += "VALUES (%s,%s)"
    try:
        cur.execute(query, (movie_id, id[0]))
    except Exception as e:
        print("failed insert index")
        print(movie_id, id[0])
        print(e)
        return 0
    else:
        conn.commit()


def insertRelation(data, table):
    query = "INSERT INTO " + table + " (name) "
    query += "VALUES (%s)"
    try:
        cur.execute(query, (data))
    except Exception as e:
        print("failed insert relation")
        print(query)
        print(e)
        return 0
    else:
        conn.commit()


def mapInsert(movie_id, name, list):
    listtype = type(list).__name__
    if listtype == 'set':
        for x in list:
            insertIndex(x, name, movie_id)
    elif listtype == 'str':
        insertIndex(list, name, movie_id)


# insert movie info to database
def insert(movie):
    movie_id = movie["id"]
    insertMovie(movie_id, movie["製品名"], movie["メーカー"],movie["シリーズ"], movie["link"])
    #mapInsert(movie_id, "country", country)
    #mapInsert(movie_id, "genre", movie["ジャンル"])
    #mapInsert(movie_id, "director", movie["監督"])
    #mapInsert(movie_id, "producer", movie["製作総指揮"])
    #mapInsert(movie_id, "writter", movie["脚本"])
    #mapInsert(movie_id, "music", movie["音楽"])
    #mapInsert(movie_id, "originaltitle", movie["原作"])


# get movie info from movie page
def getMovie(url):
    time.sleep(2)
    print(url)
    try:
        html = urlopen(url)
    except HTTPError as e:
        print(e)
        return 0
    else:
        bsObj = BeautifulSoup(html.read(), "html.parser", from_encoding="utf-8")
        moviediv = bsObj.find("div", {"id": "itmBoxMax"})
        if moviediv is None:
            return 0
        maker = moviediv.find("li", {"class":"makerLabel"})
        series = moviediv.find("li", {"class":"seriesLabel"})
        movie = {
            'メーカー': None,
            'シリーズ': None,
            '製品名': None
        }

        try:
            movie['メーカー'] = maker.find("a").get_text()
            movie['シリーズ'] = series.find("a").get_text()
        except AttributeError as e:
            print(e)

        item_title = moviediv.find("h2", {"itemprop": "name"})
        movie['製品名'] = item_title.get_text()
        return movie


# get movie page from index page
def getPage(url, ids):
    time.sleep(2)
    try:
        html = urlopen(url)
    except HTTPError as e:
        print(e)
        time.sleep(60)
        getPage(url,ids)
    else:
        bsObj = BeautifulSoup(html.read(), "html.parser", from_encoding="utf-8")
        i=0
        for item in bsObj.findAll("tr", class_="tr-border"):
            if i < 2:
                i=i+1
                continue
            if item is not None:
                i=i+1
                lst = item.find("td", {"class": "sel alignC ckbtn"})
                #getID
                if lst is not None:
                    id = lst.find("input").attrs["value"]
                    print(id)
                    continue

                item2 = item.find("td", {"class" : "alignC"})
                # if not id in ids:
                if item2 is not None:
                    link = item2.find("a").attrs["href"]
                    movie = getMovie(link)
                    if movie is not 0:
                        movie["id"] = id
                        movie["link"] = link
                        insert(movie)
    # go to next index page
    _next = bsObj.findAll("li", {"class": "pageicon"})
    nexturl = _next[1].find("a").attrs["href"]
    return nexturl


# get movie id
cur.execute("SELECT id FROM iteminfo")
ids = [x[0] for x in cur.fetchall()]
count_movie = len(ids)
print("count movie:", count_movie)
# set start page
url = domain + "pc/mouse/itemlist.aspx?pdf_pg="
# run crawler
argvs = sys.argv
if len(argvs) == 2:
    start_page = int(argvs[1])
elif len(argvs) == 1:
    start_page = 1
end_page = 5000
for page in range(start_page, end_page):
    print("=" * 10, page, "=" * 10)
    nexturl = getPage(url + str(page), ids)
    if not nexturl:
        break

cur.close()
conn.close()
