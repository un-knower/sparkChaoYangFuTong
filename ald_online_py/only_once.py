# -*- coding: utf-8 -*-
import sys

import MySQLdb
import redis
import time
import datetime
import dbconf

"""
    modified by sunxiaowei on 2018-01-09
"""


# host='localhost'
# username='root'
# password='root'
# port='3306'
# db ='test'

# host = '10.0.0.61'
# username = 'aldwx'
# password = 'wxAld2016__$#'
# port = '3306'
# db = 'ald_xinen_test'
# def db_connect():
#     """ 连接 mysql """
#     conn = MySQLdb.connect(
#         host=host,
#         port=int(port),
#         user=username,
#         passwd=password,
#         db=db,
#         charset='utf8'
#     )
#     return conn
def db_connect():
    """ 连接 mysql """
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn


# 1表示昨天日期
def get_date(day):
    return (datetime.date.today() - datetime.timedelta(days=day)).strftime('%Y-%m-%d')


# 只运行一次，将今天之前的新用户数累加
def inser_tatal_user():
    sql = '''select app_key ak,sum(new_comer_count),max(day) from aldstat_trend_analysis where day<%s group by ak'''
    cur.execute(sql, (def_day,))
    res = cur.fetchall()
    in_sql = '''update aldstat_trend_analysis set total_visitor_count=%s WHERE app_key=%s AND day=%s'''
    for row in res:
        ak = row[0]
        total_visitor_count = row[1]
        max_day = row[2]
        cur.execute(in_sql, (total_visitor_count, ak, max_day))


if __name__ == '__main__':
    now_date = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天时间
    conn = db_connect()
    cur = conn.cursor()
    args = sys.argv
    # args[1]:只能传数字，重新刷新args[1]天的累加人数，当有小程序计算累加不正确时用这个做刷新，只运行一次，传1表示一天前
    for i in range(-int(args[1]), 1):
        def_day = get_date(abs(i))
        inser_tatal_user()

    cur.close()
    conn.commit()
    conn.close()
