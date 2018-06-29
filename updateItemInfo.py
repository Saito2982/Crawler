from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import pymysql
import configparser
import time
import sys
import re

config = configparser.ConfigParser()
config.read('config.ini')
setting = config['setting']
conn = pymysql.connect(
    host=setting['host'],
    unix_socket=setting['socket'],
    user=setting['user'],
    passwd=setting['password'],
    db=setting['db'],
    charset='utf8'
)
cur = conn.cursor()
cur.execute("USE kakakucom")
domain = "http://movies.yahoo.co.jp"


def update(url, id):
    print(id)
    try:
        html = urlopen(url)
    except HTTPError as e:
        print("access failed: movie story page")
        print(e)
    else:
        bsObj = BeautifulSoup(html.read(), "html.parser")
        image = bsObj.find("div", {"class": "thumbnail__figure"})
        if "style" in image.attrs:
            imagelink = image.attrs["style"]
            imagelink = imagelink[21:-1]
        else:
            imagelink = "no image"
        lst = bsObj.find("div", {"id": "story"}).findAll("section")
        description = ""
        story = ""
        for section in lst:
            h2 = section.find("h2").get_text()
            text = section.find("p", {"class": "text-readable"})
            if not text.find("p", {"class": "text-xsmall"}) is None:
                text.find("p", {"class": "text-xsmall"}).extract()
            text = text.get_text().strip()
            if h2 == "解説":
                description = text
            elif h2 == "あらすじ":
                story = text
        if not (description is "" or story is ""):
            try:
                cur.execute("UPDATE info SET description=%s , story=%s, imagelink=%s WHERE id = %s",
                            (description, story, imagelink, id))
            except Exception as e:
                print("update failed")
                print(e)
                return 0
            else:
                print("update success")
                conn.commit()


argvs = sys.argv
if len(argvs) == 2:
    id = argvs[1]
    cur.execute("SELECT link FROM info WHERE id = %s", (id))
    movie = cur.fetchone()
    if movie is None:
        print("not exists movie")
    else:
        link = movie[0]
        storyurl = domain + link + "story/"
        update(storyurl, id)
elif len(argvs) == 1:
    cur.execute(
        "SELECT id,link FROM info WHERE description IS NULL AND story IS NULL")
    movie_info = cur.fetchall()
    for movie in movie_info:
        id = movie[0]
        link = movie[1]
        storyurl = domain + link + "story/"
        update(storyurl, id)

cur.close()
conn.close()
