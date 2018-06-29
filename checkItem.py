import pymysql
import configparser
import sys
import mysql.connector

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
cur.execute("USE movie")

argvs = sys.argv
if len(argvs) == 2:
    movie_id = argvs[1]

query = ("SELECT id,title FROM movie WHERE id = " + movie_id +";")
cur.execute(query)
movie = cur.fetchall()
print("ID:", movie[0][0])
print("TITLE:", movie[0][1])

def getOtherTable(movie_id, table_name):
    print(table_name)
    query = "SELECT id, name FROM movie." + table_name + " AS mt "
    query += "JOIN movie." + table_name + " AS t "
    query += "ON mt." + table_name + "_id = t.id " 
    query += "WHERE movie_id = %s"
    cur.execute(query, (movie_id))
    for result in cur.fetchall():
        print("ID:", result[0], "NAME:", result[1])

getOtherTable(movie_id, "country")
getOtherTable(movie_id, "director")
getOtherTable(movie_id, "genre")
getOtherTable(movie_id, "music")
getOtherTable(movie_id, "original")
getOtherTable(movie_id, "producer")
getOtherTable(movie_id, "writter")
getOtherTable(movie_id, "cast")

cur.close()
conn.close()
