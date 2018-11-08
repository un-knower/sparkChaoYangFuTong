# -*- coding:utf-8 -*-
import re
import sys
import time
from datetime import datetime, timedelta
import MySQLdb
import dbconf

"""
Create by:JetBrains PyCharm Community Edition 2016.1(64)
User:weilongsheng
Modify:zhangzhenwei
Date:2018-01-09
Time:28:04
"""


def db_connect():
    """连接数据库的函数"""
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn


def seven_time(timedate):
    """返回30天的开始和结束时间"""
    if (timedate == ""):
        today = datetime.now()  # 获取今天日期
        yesterday = today + timedelta(days=-1)  # 获取昨天日期
        thirtydays_ago = today + timedelta(days=-30)  # 获取30天前的日期
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
        thirtydays_ago_str = thirtydays_ago.strftime("%Y-%m-%d")  # 将日期转换成str类型
    elif (timedate != "" and re.search("\d{4}-\d{2}-\d{2}", timedate)):
        today = datetime.now()  # 获取今天日期
        timedate_stamp = time.mktime(time.strptime(timedate, '%Y-%m-%d'))  # 将字符串时间转换成时间戳
        thirty_start = time.localtime(timedate_stamp - 2592000)  # 获取30天的开始时间
        thirty_end = time.localtime(timedate_stamp - 86400)  # 获取30天的结束时间
        thirtydays_ago_str = time.strftime('%Y-%m-%d', thirty_start)  # 将日期转换成str类型
        yesterday_str = time.strftime('%Y-%m-%d', thirty_end)  # 将日期转换成str类型
    else:
        today = datetime.now()  # 获取今天日期
        yesterday = today + timedelta(days=-1)  # 获取昨天日期
        thirtydays_ago = today + timedelta(days=-30)  # 获取七天前的日期
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
        thirtydays_ago_str = thirtydays_ago.strftime("%Y-%m-%d")  # 将日期转换成str类型
    return (yesterday_str, thirtydays_ago_str, today)


def sum_terminal_all(timedate):
    """终端汇总"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = seven_time(timedate)
    sql = "SELECT app_key,type,type_value,SUM(new_comer_count),SUM(visitor_count),SUM(open_count)," \
          "SUM(total_page_count),SUM(total_stay_time),SUM(one_page_count)," \
          "SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(total_page_count) " \
          "from aldstat_terminal_analysis where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,type,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_terminal_analysis = """
    insert into aldstat_30days_terminal_analysis
    (app_key,day,type,type_value,new_comer_count,visitor_count,open_count,total_page_count,total_stay_time,one_page_count,avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),visitor_count=VALUES(visitor_count),open_count=VALUES(open_count),total_page_count=VALUES(total_page_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),avg_stay_time=VALUES(avg_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """

    args = []  # 定义批量入库集合
    for row in results:
        sst = 0  # 次均停留时长
        bounce_rate = 0  # 跳出率
        # 过滤掉次均停留时长和跳出率为None的情况
        if (row[9] == None):
            sst
        else:
            sst = row[9]
        if (row[10] == None):
            bounce_rate
        else:
            bounce_rate = row[10]
        if row[0] != "":
            sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], 0, row[5], row[6], row[7], row[8], float(sst),
                         float(bounce_rate), update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql_terminal_analysis, args)  # 批量入库终端汇总表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def args_main():
    """判断参数是否有值"""
    timeargs = ""
    dimension = ""
    if (len(sys.argv) > 1 and len(sys.argv) < 3):
        timeargs = sys.argv[1]
    elif (len(sys.argv) > 2 and len(sys.argv) < 4):
        timeargs = sys.argv[1]
        dimension = sys.argv[2]
    else:
        dimension = ""
        timeargs = ""
    return (dimension, timeargs)


if __name__ == '__main__':
    ald_start_time = int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 判断参数是否有值
    (dimension, timeargs) = args_main()
    if (timeargs == "terminal" or dimension == "terminal"):
        sum_terminal_all(timeargs)
    else:
        sum_terminal_all(timeargs)

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
