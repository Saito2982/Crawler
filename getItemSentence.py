from urllib.request import urlopen
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
cur.execute("USE movie")


def splitReview(review_id, review):
    split_review = re.split(r'(!|\?|！|？|。|　)', review)
    sentences = []
    for word in split_review:
        if len(word) > 2:
            sentences.append(word)
        else:
            joinword = sentences.pop() + word
            sentences.append(joinword)
    for sentence in sentences:
        print(sentence)
        try:
            cur.execute(
                "INSERT INTO sentence (review_id, text) VALUES (%s,%s)", (review_id, sentence))
        except Exception as e:
            print(e)
        else:
            conn.commit()


# get review page from movie data
argvs = sys.argv
if len(argvs) == 2:
    id = argvs[1]
    cur.execute(
        "SELECT id,text FROM review WHERE movie_id = %s AND netabare = 0 ORDER BY good DESC LIMIT 10", (id))
elif len(argvs) == 1:
    cur.execute("SELECT id,text FROM review")
reviews = cur.fetchall()
for review in reviews:
    id = review[0]
    text = review[1]
    splitReview(id, text)

cur.close()
conn.close()
