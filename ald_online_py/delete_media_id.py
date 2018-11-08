# coding=utf-8
import MySQLdb
import dbconf

def db_connect():
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn


if __name__ == '__main__':
    conn = db_connect()
    conn.set_character_set('utf8')
    cur = conn.cursor()

    sql1 = """delete from aldstat_7days_single_media where app_key='64a7f2b033fb593a8598bbc48c3b8486' and media_id in (15,49)"""
    sql2 = """delete from aldstat_30days_single_media where app_key='64a7f2b033fb593a8598bbc48c3b8486' and media_id in (15,49)"""

    cur.execute(sql1)
    cur.execute(sql2)

    conn.commit()
    conn.close()

