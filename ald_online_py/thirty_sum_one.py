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
Date:2018-01-08
Time:20:00
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


def thirty_time(timedate):
    """返回30天的开始和结束时间"""
    if (timedate == ""):
        today = datetime.now()  # 获取今天日期
        yesterday = today + timedelta(days=-1)  # 获取昨天日期
        thirtydays_ago = today + timedelta(days=-30)  # 获取30天前的日期
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
        thirtydays_ago_str = thirtydays_ago.strftime("%Y-%m-%d")  # 将日期转换成str类型
    elif (timedate != "" and re.search("\d{4}-\d{2}-\d{2}", timedate)):
        today = datetime.now()
        timedate_stamp = time.mktime(time.strptime(timedate, '%Y-%m-%d'))  # 将字符串时间转换成时间戳
        thirty_start = time.localtime(timedate_stamp - 2592000)  # 获取30天的开始时间
        thirty_end = time.localtime(timedate_stamp - 86400)  # 获取30天的结束时间
        thirtydays_ago_str = time.strftime('%Y-%m-%d', thirty_start)
        yesterday_str = time.strftime('%Y-%m-%d', thirty_end)
    else:
        today = datetime.now()  # 获取今天日期
        yesterday = today + timedelta(days=-1)  # 获取昨天日期
        thirtydays_ago = today + timedelta(days=-30)  # 获取30天前的日期
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
        thirtydays_ago_str = thirtydays_ago.strftime("%Y-%m-%d")  # 将日期转换成str类型
    return (yesterday_str, thirtydays_ago_str, today)


def sum_scene_tmp(timedate):
    """单个场景值计算"""
    #     获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    #     拼接SQL语句
    sql = "select app_key, scene_id, sum(scene_open_count), sum(scene_page_count), sum(scene_newer_for_app)," \
          "sum(one_page_count),sum(total_stay_time),sum(total_stay_time)/sum(scene_open_count) " \
          "secondary_stay_time,sum(one_page_count)/sum(scene_page_count) bounce_rate from " \
          "aldstat_scene_statistics where day >=" + "'" + thirtydays_ago_str \
          + "'" + " and day <=" + "'" + yesterday_str + "'" + " group by app_key,scene_id"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30日结果表中
    sql_inser30_single = """
        insert into aldstat_30days_single_scene
        (app_key, day, scene_id, scene_open_count, scene_page_count, scene_newer_for_app,one_page_count,
        total_stay_time,secondary_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE scene_open_count=VALUES(scene_open_count),scene_page_count=VALUES(scene_page_count),
        scene_newer_for_app=VALUES(scene_newer_for_app)
    """
    args = []  # 定义批量入库列表
    for row in results:
        sst = 0  # 次均停留时长
        bounce_rate = 0  # 跳出率
        if (row[7] == None):
            sst
        else:
            sst = row[7]
        if (row[8] == None):
            bounce_rate
        else:
            bounce_rate = row[8]

        sql_data = (
            [row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], float(sst), float(bounce_rate),
             update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_inser30_single, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_scene_group_tmp(timedate):
    """分组场景值计算"""
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)  # 获取30天开始和结束时间
    sql = "SELECT app_key,scene_group_id,SUM(new_comer_count),SUM(page_count),SUM(open_count)," \
          "SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count) " \
          "secondary_stay_time,SUM(one_page_count)/SUM(page_count) bounce_rate from " \
          "aldstat_daily_scene_group where day >=" + "'" + thirtydays_ago_str \
          + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,scene_group_id"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_inser30_group = """
        insert into aldstat_30days_scene_group (app_key,day,scene_group_id,new_comer_count,page_count,
        open_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),page_count=VALUES(page_count),
        open_count=VALUES(open_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),
        secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),
        update_at=VALUES(update_at)
        """
    # 批量写入
    args = []
    for row in results:
        sst = 0  # 次均停留时长
        bounce_rate = 0  # 跳出率
        if (row[7] == None):
            sst
        else:
            sst = row[7]
        if (row[8] == None):
            bounce_rate
        else:
            bounce_rate = row[8]
        sql_data = (
            [row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], float(sst), float(bounce_rate),
             update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_inser30_group, args)  # 批量入库30组
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_qr_tmp(timedate):
    """二维码计算"""
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql_qr = "select app_key,qr_key,SUM(total_scan_count),SUM(qr_new_comer_for_app) from " \
             "aldstat_qr_code_statistics where day >=" + "'" + thirtydays_ago_str \
             + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,qr_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql_qr)  # 查询30天数据指标
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql_qr_single = """
    insert into aldstat_30days_single_qr
(app_key,day,qr_key,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_scan_count=VALUES(qr_scan_count),qr_newer_count=VALUES(qr_newer_count),
    update_at=VALUES(update_at) """
    args = []  # 定义批量写入列表
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_qr_single, args)  # 批量入库二维码单个场景值
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

def sum_qr_group_tmp(timedate):
    """二维码组"""
    # 获取30天开始和结束时间
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql_qr_group = "select app_key,qr_group_key,SUM(qr_scan_count),SUM(qr_newer_count) from " \
                   "aldstat_daily_qr_group where day >=" + "'" + thirtydays_ago_str + "'" \
                   + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,qr_group_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql_qr_group)
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql_qr_singlegr = """
    insert into aldstat_30days_single_qr_group
(app_key,day,qr_group_key,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_scan_count=VALUES(qr_scan_count),qr_newer_count=VALUES(qr_newer_count),
    update_at=VALUES(update_at) """

    args = []  # 定义批量写入列表
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_qr_singlegr, args)  # 批量入库二维码单个场景值组
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_qr_all(timedate):
    """二维码组（所有）"""
    # 获取30天开始和结束时间
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "select app_key,COUNT(DISTINCT qr_key),SUM(total_scan_count),SUM(qr_new_comer_for_app) " \
          "from aldstat_qr_code_statistics where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询入二维码组所需要的指标
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql2 = """
    insert into aldstat_30days_all_qr
(app_key,day,qr_count,qr_scan_count,qr_newer_count ,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_count=VALUES(qr_count),qr_scan_count=VALUES(qr_scan_count),
    qr_newer_count=VALUES(qr_newer_count),update_at=VALUES(update_at) """

    args = []  # 定义批量入库列表
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)  # 批量入库二维码组汇总
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


def sum_trend_tmp(timedate):
    """趋势分析"""
    # 获取30天开始和结束时间
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql_trend = "SELECT app_key,SUM(new_comer_count),SUM(open_count),SUM(total_page_count),SUM(total_stay_time)," \
                "SUM(daily_share_count),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count)," \
                "SUM(one_page_count)/SUM(total_page_count) from aldstat_trend_analysis where day >=" + "'" \
                + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql_trend)
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql_trend_ana = """
    insert into aldstat_30days_trend_analysis
(app_key,day,new_comer_count,open_count,total_page_count,total_stay_time,daily_share_count,one_page_count,secondary_avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),open_count=VALUES(open_count),
    total_page_count=VALUES(total_page_count),total_stay_time=VALUES(total_stay_time),
    daily_share_count=VALUES(daily_share_count),one_page_count=VALUES(one_page_count),
    secondary_avg_stay_time=VALUES(secondary_avg_stay_time),bounce_rate=VALUES(bounce_rate),
    update_at=VALUES(update_at) """
    # 批量写入
    args = []
    # 循环将入库指标追加到批量入库集合中
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_trend_ana, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    # 获取当前时间
    ald_start_time = int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    (dimension, timeargs) = args_main()  # 判断参数集合是否为空
    # 根据维度和时间分别调用函数
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
    elif (timeargs == "trend" or dimension == "trend"):
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
