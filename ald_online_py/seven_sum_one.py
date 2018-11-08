# -*- coding:utf-8 -*-
import re
import sys
import time
from datetime import datetime, timedelta
import MySQLdb
import dbconf

# 连接数据库的函数
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

# 返回七天的开始和结束时间
def seven_time(timedate):
    today = ""
    seven_days_ago = ""
    d = ""
    d0 = ""
    if (timedate == ""):
        d = datetime.now()  # 获取今天日期
        d0 = d + timedelta(days=-1)
        d1 = d + timedelta(days=-7)  # 获取七天前的日期
        today = d0.strftime("%Y-%m-%d")  # 将日期转换成str类型
        seven_days_ago = d1.strftime("%Y-%m-%d")  # 将日期转换成str类型
    elif (timedate != "" and re.search("\d{4}-\d{2}-\d{2}", timedate)):
        d = datetime.now()
        a = time.mktime(time.strptime(timedate, '%Y-%m-%d'))
        x = time.localtime(a - 604800)
        y = time.localtime(a - 86400)
        seven_days_ago = time.strftime('%Y-%m-%d', x)
        today = time.strftime('%Y-%m-%d', y)
    else:
        d = datetime.now()  # 获取今天日期
        d0 = d + timedelta(days=-1)
        d1 = d + timedelta(days=-7)  # 获取七天前的日期
        today = d0.strftime("%Y-%m-%d")  # 将日期转换成str类型
        seven_days_ago = d1.strftime("%Y-%m-%d")  # 将日期转换成str类型
    return (today, seven_days_ago, d)

"""---------------------------单个场景值计算--------------------------------"""

def sum_scene_tmp(timedate):
    #     获取七天的开始和结束日期
    (today, seven_days_ago, update_at) = seven_time(timedate)
    #     拼接SQL语句
    sql = "select app_key, scene_id, sum(scene_open_count), sum(scene_page_count), sum(scene_newer_for_app),sum(one_page_count),sum(total_stay_time),sum(total_stay_time)/sum(scene_open_count) secondary_stay_time,sum(one_page_count)/sum(scene_page_count) bounce_rate from aldstat_scene_statistics where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " group by app_key,scene_id"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    for row in results:
        r7 = 0
        r8 = 0
        if(row[7] == None):
            r7
        else:
            r7 = row[7]
        if(row[8] == None):
            r8
        else:
            r8 = row[8]

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_single_scene
      (app_key, day, scene_id, scene_open_count, scene_page_count, scene_newer_for_app,one_page_count,total_stay_time,secondary_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE scene_open_count=VALUES(scene_open_count),scene_page_count=VALUES(scene_page_count),scene_newer_for_app=VALUES(scene_newer_for_app),one_page_count=VALUES(one_page_count),total_stay_time=VALUES(total_stay_time),secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at)
    """
    # 批量写入
    args = []
    for row in results:
        r7 = 0
        r8 = 0
        if(row[7] == None):
            r7
        else:
            r7 = row[7]
        if(row[8] == None):
            r8
        else:
            r8 = row[8]

        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], float(r7), float(r8), update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""---------------------------分组场景值计算--------------------------------"""


def sum_scene_group_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,scene_group_id,SUM(new_comer_count),SUM(page_count),SUM(open_count),SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count) secondary_stay_time,SUM(one_page_count)/SUM(page_count) bounce_rate from aldstat_daily_scene_group where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,scene_group_id"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        r7 = 0
        r8 = 0
        if(row[7] == None):
            r7
        else:
            r7 = row[7]
        if(row[8] == None):
            r8
        else:
            r8 = row[8]

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_scene_group (app_key,day,scene_group_id,new_comer_count,page_count,open_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),page_count=VALUES(page_count),open_count=VALUES(open_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at)
    """
    # 批量写入
    args = []
    for row in results:
        r7 = 0
        r8 = 0
        if(row[7] == None):
            r7
        else:
            r7 = row[7]
        if(row[8] == None):
            r8
        else:
            r8 = row[8]
        sql_data = (
            [row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], float(r7), float(r8), update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""----------------------------------二维码---------------------------------------"""


def sum_qr_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "select app_key,qr_key,SUM(total_scan_count),SUM(qr_new_comer_for_app) from aldstat_qr_code_statistics where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,qr_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_single_qr
(app_key,day,qr_key,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_scan_count=VALUES(qr_scan_count),qr_newer_count=VALUES(qr_newer_count),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""----------------------------------二维码组---------------------------------"""


def sum_qr_group_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "select app_key,qr_group_key,SUM(qr_scan_count),SUM(qr_newer_count) from aldstat_daily_qr_group where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,qr_group_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_single_qr_group
(app_key,day,qr_group_key,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_scan_count=VALUES(qr_scan_count),qr_newer_count=VALUES(qr_newer_count),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""--------------------------------二维码组（所有）------------------------------"""


def sum_qr_all(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "select app_key,COUNT(DISTINCT qr_key),SUM(total_scan_count),SUM(qr_new_comer_for_app) from aldstat_qr_code_statistics where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_all_qr
(app_key,day,qr_count,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_count=VALUES(qr_count),qr_scan_count=VALUES(qr_scan_count),qr_newer_count=VALUES(qr_newer_count),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


# 判断参数是否有值
def args_main():
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

# 趋势分析

def sum_trend_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)

    sql = "SELECT app_key,SUM(new_comer_count),SUM(open_count),SUM(total_page_count),SUM(total_stay_time),SUM(daily_share_count),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(total_page_count) from aldstat_trend_analysis where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_trend_analysis
(app_key,day,new_comer_count,open_count,total_page_count,total_stay_time,daily_share_count,one_page_count,secondary_avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),open_count=VALUES(open_count),total_page_count=VALUES(total_page_count),total_stay_time=VALUES(total_stay_time),daily_share_count=VALUES(daily_share_count),one_page_count=VALUES(one_page_count),secondary_avg_stay_time=VALUES(secondary_avg_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    (dimension, timeargs) = args_main()
    if (timeargs == "scene" or dimension == "scene"):
        sum_scene_tmp(timeargs)
    elif (timeargs == "scene_group_id" or dimension == "scene_group_id"):
        sum_scene_group_tmp(timeargs)
    elif (timeargs == "qr_key" or dimension == "qr_key"):
        sum_qr_tmp(timeargs)
    elif (timeargs == "qr_key_all" or dimension == "qr_key_all"):
        sum_qr_all(timeargs)
    elif (timeargs == "qr_group_key" or dimension == "qr_group_key"):
        sum_qr_group_tmp(timeargs)
    elif(timeargs == "trend" or dimension == "trend"):
        sum_trend_tmp(timeargs)
    else:
        sum_scene_tmp(timeargs)
        sum_scene_group_tmp(timeargs)
        sum_qr_tmp(timeargs)
        sum_qr_group_tmp(timeargs)
        sum_qr_all(timeargs)
        sum_trend_tmp(timeargs)


    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
