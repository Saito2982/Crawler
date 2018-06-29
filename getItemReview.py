from urllib.request import urlopen
from bs4 import BeautifulSoup
import pymysql
import configparser
import time
import sys
import re
import mysql.connector
import numpy as np

config = configparser.ConfigParser()
config.read('config.ini')
setting = config['setting']
conn = pymysql.connect(
    host=setting['host'],
    #unix_socket=setting['socket'],
    user=setting['user'],
    password=setting['passwd'],
    db=setting['db'],
    charset='utf8'
)

#conn = mysql.connector.connect(user='root', password='hiroyuki417+', host='localhost', database='movie')
cur = conn.cursor()
cur.execute("USE kakakucom")
domain = "http://kakaku.com/"


# change str type to datetime type
def datetime(str):
    date = re.sub(r'(日)', '', re.sub(r'(年|月)', '-', str))
    datetime = re.sub(r'(分)', '', re.sub(r'(時)', ':', date)) + ":00"
    return datetime


# change class name to score
def rate(str):
    if str == "rate5":
        return 5
    if str == "rate4":
        return 4
    if str == "rate3":
        return 3
    if str == "rate2":
        return 2
    if str == "rate1":
        return 1
    return 0


# insert movie info to database
def insert(review):
    query = "INSERT INTO review (id, item_id, user_id, title, text, total, "
    query += "d1, d2, d3, d4, d5, d6, d7, date, link) "
    query += "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cur.execute(query, (
            review["item_id"],
            review["user_id"],
            review["title"],
            review["text"],
            review["total"],
            review["d1"],
            review["d2"],
            review["d3"],
            review["d4"],
            review["d5"],
            review["d6"],
            review["d7"],
            review["date"],
            review["link"]
        ))
    except Exception as e:
        print(e)
        return 0
    else:
        conn.commit()


def searchUser(username, userpage):
    print(userpage)
    cur.execute("SELECT id FROM user WHERE link = '"+userpage+"'")
    user = cur.fetchone()
    if user is None:
        try:
            cur.execute(
                "INSERT INTO user (name, link) VALUES (%s,%s)", (username, userpage))
        except Exception as e:
            print(e)
        else:
            conn.commit()
            cur.execute("SELECT id FROM user WHERE link = '"+userpage+"'")
            user = cur.fetchone()
            if user is None:
                return 0 # set 0 on no link user's id
    return user[0]


# get review data from review page
def getReview(url):
    time.sleep(2)
    html = urlopen(url)
    bsObj = BeautifulSoup(html.read(), "html.parser")
    title = bsObj.find("div", {"class": "reviewTitle"}).get_text().strip()
    readabletext = bsObj.find("p", {"class": "revEntryCont"})
    text = ""
    if readabletext is not None:
        text = readabletext.get_text()
    review_attr = bsObj.find("div", {"class": "revRateBox"}).findAll("tr")
    for i, d in enumerate(review_attr):
        review_attr[i] = rate(review_attr.find("td").attrs["class"])
    user = bsObj.findAll("div", {"class": "userInfo"})[0].find("a")
    if user is not None:
        userpage = user.attrs["href"]
        username = user.get_text()
        user = searchUser(username, userpage)
    review = {
        'title': title,
        'text': text.strip(),
        'd1': review_attr[0],
        'd2': review_attr[1],
        'd3': review_attr[2],
        'd4': review_attr[3],
        'd5': review_attr[4],
        'd6': review_attr[5],
        'd7': review_attr[6],
        'd8': review_attr[7],
        'user_id': user
    }
    return review


# get review page from index page and go to next page
def getPage(url, movie_id):
    time.sleep(2)
    html = urlopen(url)
    bsObj = BeautifulSoup(html.read(), "html.parser")
    reviewlist = bsObj.findAll("div" : "reviewBoxWtInner")
    if reviewlist is None:
        return 0
    for item in reviewlist:
        link = item.find("div", "class":"reviewTitle").find(a).attrs["href"]

        #opacity = item.findAll("li", {"class": "opacity-60"})
        #created = datetime(opacity[1].get_text())
        #good = opacity[2].find("strong").get_text().strip()
        satis = item.findAll("th")[-1].get_text()
        if (satis == "満足度"):
            total = rate(satis)
        cur.execute("SELECT link FROM review WHERE link ='" +link+"'")
        print(cur.rowcount)
        if cur.rowcount == 0:
            review = getReview(link)
            review["item_id"] = movie_id
            #review["link"] = link
            #review["created"] = created
            #review["good"] = good
            review["total"] = total
            review["netabare"] = netabare
            print(review)
            insert(review)
        # elif cur.rowcount == 1:
            # update(review)
    pagination = bsObj.find("a", {"data-ylk": "slk:next;pos:0"})
    if pagination is None:
        return 0
    else:
        nexturl = pagination.attrs["href"]
        if nexturl is None:
            return 0
        else:
            getPage(domain + nexturl, movie_id)


# get review page from movie data
#for movieID in range(470,357341):
argvs = sys.argv
    #argvs = str(movieID)
print(argvs[1])
if len(argvs) == 2:
    id = argvs[1]
    cur.execute("SELECT link FROM iteminfo WHERE id = " + id)
    movie = cur.fetchone()
    if movie is None:
        print("not exists movie")
    else:
        link = movie[0]
        url = domain + link + "review/"
        cur.execute("SELECT COUNT(*) FROM review WHERE item_id ="  + id)
        count_review = cur.fetchone()[0]
        page = int(count_review / 10)
        if page <= 0:
            page = 1
        print("count review:", count_review)
        print("start page:", page)
        url += "?page=" + str(page)
        getPage(url, id)
elif len(argvs) == 1:
    cur.execute("SELECT id,link FROM iteminfo")
    movie_info = cur.fetchall()
    for movie in movie_info:
        id = movie[0]
        link = movie[1]
        starturl = domain + "review/" + id
    getPage(starturl, id)

cur.close()
conn.close()