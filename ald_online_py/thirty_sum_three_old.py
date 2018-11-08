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
Time:17:27
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
    """返回30天的开始和结束时间，timedate默认为昨天的日期"""
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
        thirtydays_ago_str = time.strftime('%Y-%m-%d', thirty_start)  # 将日期转换成str类型
        yesterday_str = time.strftime('%Y-%m-%d', thirty_end)  # 将日期转换成str类型
    else:
        today = datetime.now()  # 获取今天日期
        yesterday = today + timedelta(days=-1)  # 获取昨天的日期
        thirtydays_ago = today + timedelta(days=-30)  # 获取30天前的日期
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
        thirtydays_ago_str = thirtydays_ago.strftime("%Y-%m-%d")  # 将日期转换成str类型
    return (yesterday_str, thirtydays_ago_str, today)


update_at_tmp = int(time.time())

"""------------------------------------------------------------------------"""


def sum_phone_brand_tmp(timedate):
    """手机品牌，timedate默认为昨天的日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,brand,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time)," \
          "SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count)" \
          " FROM aldstat_daily_phonebrand where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,brand"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_phonebrand = """
        insert into aldstat_30days_phonebrand (app_key,day,brand,new_user_count,open_count,page_count,
        total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),page_count=VALUES(page_count),
        open_count=VALUES(open_count),total_stay_time=VALUES(total_stay_time),
        one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),
        bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at)
        """
    args = []  # 定义批量入库集合
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
        cur.executemany(sql_phonebrand, args)  # 批量入库手机品牌日汇总表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_phone_model_tmp(timedate):
    """手机类型,timedate:默认为昨天的日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "select app_key,phone_model,SUM(new_user_count),SUM(open_count),SUM(page_count)," \
          "SUM(total_stay_time),SUM(one_page_count),SUM(one_page_count)/SUM(page_count)," \
          "SUM(total_stay_time)/SUM(open_count) from ald_device_statistics where " \
          "from_unixtime(date,'%Y-%m-%d') >=" + "'" + thirtydays_ago_str + "'" \
          + " and from_unixtime(date,'%Y-%m-%d') <=" + "'" + yesterday_str + "'" \
          + " GROUP BY phone_model,app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_device = """
        insert into ald_30days_device_statistics (app_key,date,phone_model,new_user_count,open_count,
        page_count,total_stay_time,one_page_count,bounce_rate,secondary_stay_time,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),
        page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),
        one_page_count=VALUES(one_page_count),bounce_rate=VALUES(bounce_rate),
        secondary_stay_time=VALUES(secondary_stay_time),update_at=VALUES(update_at)
        """
    args = []  # 定义批量入库集合
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
        cur.executemany(sql_device, args)  # 批量入库手机类型表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_entrance_tmp(timedate):
    """入口页,timedate:默认为昨天的日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,page_path,SUM(entry_page_count),SUM(one_page_count),SUM(page_count)," \
          "SUM(open_count),SUM(total_time),SUM(total_time)/SUM(open_count)," \
          "SUM(one_page_count)/SUM(page_count) FROM aldstat_entrance_page where day >=" + "'" \
          + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,page_path"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_entrance_page = """
        insert into aldstat_30days_single_entrance_page (app_key,day,page_path,entry_page_count,
        one_page_count,page_count,open_count,total_time,avg_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE entry_page_count=VALUES(entry_page_count),
        one_page_count=VALUES(one_page_count),page_count=VALUES(page_count),
        open_count=VALUES(open_count),total_time=VALUES(total_time),update_at=VALUES(update_at),
        avg_stay_time=VALUES(avg_stay_time),bounce_rate=VALUES(bounce_rate) """
    # 批量写入
    args = []
    # 循环results将入库指标添加到批量入库集合
    for row in results:
        sql_data = (
        [row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql_entrance_page, args)  # 批量入库入口页表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_page_tmp(timedate):
    """受访页,timedate:默认为昨天日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,page_path,SUM(page_count),SUM(open_count),SUM(total_stay_time)," \
          "SUM(abort_page_count),SUM(share_count),SUM(total_stay_time)/SUM(open_count)," \
          "SUM(abort_page_count)/SUM(page_count) FROM aldstat_page_view where day >=" + "'" \
          + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,page_path"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询批量入库的各项指标
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_page_view = """
        insert into aldstat_30days_single_page_view (app_key,day,page_path,page_count,open_count,
        total_time,abort_page_count,share_count,avg_stay_time,abort_ratio,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE page_count=VALUES(page_count),abort_page_count=VALUES(abort_page_count),
        open_count=VALUES(open_count),total_time=VALUES(total_time),share_count=VALUES(share_count),
        update_at=VALUES(update_at),avg_stay_time=VALUES(avg_stay_time),abort_ratio=VALUES(abort_ratio)
        """
    args = []  # 定义批量入库集合
    for row in results:
        sql_data = (
        [row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql_page_view, args)  # 批量入库受访页表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_province_tmp(timedate):
    """地域（安省）,timedatte:默认为昨天日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,province,SUM(new_user_count),SUM(open_count),SUM(page_count)," \
          "SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count)," \
          "SUM(one_page_count)/SUM(page_count) FROM aldstat_region_statistics where " \
          "from_unixtime(day,'%Y-%m-%d') >=" + "'" + thirtydays_ago_str + "'" \
          + " and from_unixtime(day,'%Y-%m-%d') <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,province"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询入库各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_province = """
        insert into aldstat_30days_province (app_key,day,province,new_user_count,open_count,page_count,
        total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),
        page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),
        one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),
        bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """

    args = []  # 定义批量入库集合
    for row in results:
        if (row[7] == None):
            sst = 0
        else:
            sst = row[7]
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], sst, row[8], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_province, args)  # 批量入库地域（安省）表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_city_tmp(timedate):
    """地域（安市）,timedate:默认为昨天日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,city,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time)," \
          "SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) " \
          "FROM aldstat_city_statistics where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,city"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询入库各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_city_statistics = """
        insert into aldstat_30days_city_statistics (app_key,day,city,new_user_count,open_count,
        page_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),
        page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),
        one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),
        bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """

    args = []  # 定义批量入库集合
    for row in results:
        if (row[7] == None):
            sst = 0
        else:
            sst = row[7]
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], sst, row[8], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_city_statistics, args)  # 批量入库地域（安市）表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_duration_tmp(timedate):
    """访问时长"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT a.app_key,a.visit_duration,a.one_count,a.one_count/b.day_sum_count " \
          "from (SELECT app_key,visit_duration,SUM(open_count) one_count " \
          "from aldstat_visit_duration where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,visit_duration) a LEFT JOIN " \
                                                        "(SELECT app_key,SUM(open_count) day_sum_count from aldstat_visit_duration where day >=" \
          + "'" + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" \
          + " GROUP BY app_key) b on a.app_key=b.app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询30天入库各项指标
    results = cur.fetchall()
    # 将7日汇总结果写入 7 日结果表中
    sql_visit_duration = """
           insert into aldstat_30days_visit_duration
           (app_key,day,visit_duration,open_count,open_ratio,update_at)
           values (%s,%s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE open_count=VALUES(open_count),open_ratio=VALUES(open_ratio),
           update_at=VALUES(update_at) """

    args = []  # 定义批量批量入库集合
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_visit_duration, args)  # 批量入库访问时长表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_depth_tmp(timedate):
    """访问深度,timedate:默认为昨天日期"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT a.app_key,a.visit_depth,a.one_count,a.one_count/b.sum_count from (SELECT app_key," \
          "visit_depth,SUM(open_count) one_count from aldstat_visit_depth where day >=" + "'" \
          + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key,visit_depth) " \
                                                                                   "a LEFT JOIN (SELECT app_key,SUM(open_count) sum_count from aldstat_visit_depth where day >=" \
          + "'" + thirtydays_ago_str + "'" + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key) b on " \
                                                                                         "a.app_key = b.app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询30天访问深度需要入库的各项指标，并计算打开率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_visit_depth = """
               insert into aldstat_30days_visit_depth
               (app_key,day,visit_depth,open_count,open_ratio,update_at)
               values (%s,%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE open_count=VALUES(open_count),open_ratio=VALUES(open_ratio),
               update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql_visit_depth, args)  # 批量入库访问深度表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_entrance_all(timedate):
    """入口页"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,SUM(entry_page_count),SUM(one_page_count),SUM(page_count),SUM(open_count)," \
          "SUM(total_time),SUM(total_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM " \
          "aldstat_daily_entrance_page where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_entrance_page = """
           insert into aldstat_30days_entrance_page
           (app_key,day,entry_page_count,one_page_count,page_count,open_count,total_time,avg_stay_time,
           bounce_rate,update_at)values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE entry_page_count=VALUES(entry_page_count),
           one_page_count=VALUES(one_page_count),page_count=VALUES(page_count),
           open_count=VALUES(open_count),total_time=VALUES(total_time),update_at=VALUES(update_at) """

    args = []  # 批量入库集合
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], row[7], update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql_entrance_page, args)  # 批量入库入口页表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_page_view_all(timedate):
    """受访页"""
    # 获取30天的开始和结束日期
    (yesterday_str, thirtydays_ago_str, update_at) = thirty_time(timedate)
    sql = "SELECT app_key,SUM(page_count),SUM(open_count),SUM(total_time),SUM(abort_page_count)," \
          "SUM(share_count),SUM(total_time)/SUM(page_count),SUM(abort_page_count)/SUM(page_count) " \
          "FROM aldstat_daily_page_view where day >=" + "'" + thirtydays_ago_str + "'" \
          + " and day <=" + "'" + yesterday_str + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询各项指标并计算次均停留时长和跳出率
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql2 = """
           insert into aldstat_30days_page_view
           (app_key,day,page_count,open_count,total_time,abort_page_count,share_count,avg_stay_time,
           abort_ratio,update_at)values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE page_count=VALUES(page_count),abort_page_count=VALUES(abort_page_count),
           open_count=VALUES(open_count),total_time=VALUES(total_time),share_count=VALUES(share_count),
           update_at=VALUES(update_at) """
    args = []  # 批量入库集合
    for row in results:
        sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], row[4], row[5], row[6], row[7], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)  # 批量入库受访页表
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
    # 根据指标类型分别调用函数
    if (timeargs == "respondents" or dimension == "respondents"):
        sum_page_tmp(timeargs)
    elif (timeargs == "respondents_all" or dimension == "respondents_all"):
        sum_page_view_all(timeargs)
    elif (timeargs == "entrance" or dimension == "entrance"):
        sum_entrance_tmp(timeargs)
    elif (timeargs == "entrance_all" or dimension == "entrance_all"):
        sum_entrance_all(timeargs)
    elif (timeargs == "province" or dimension == "province"):
        sum_province_tmp(timeargs)
    elif (timeargs == "city" or dimension == "city"):
        sum_city_tmp(timeargs)
    elif (timeargs == "brand" or dimension == "brand"):
        sum_phone_brand_tmp(timeargs)
    elif (timeargs == "model" or dimension == "model"):
        sum_phone_model_tmp(timeargs)
    elif (timeargs == "visit_duration" or dimension == "visit_duration"):
        sum_duration_tmp(timeargs)
    elif (timeargs == "visit_depth" or dimension == "visit_depth"):
        sum_depth_tmp(timeargs)
    else:
        sum_phone_brand_tmp(timeargs)
        sum_phone_model_tmp(timeargs)
        sum_entrance_tmp(timeargs)
        sum_province_tmp(timeargs)
        sum_city_tmp(timeargs)
        sum_duration_tmp(timeargs)
        sum_depth_tmp(timeargs)
        sum_page_tmp(timeargs)
        sum_entrance_all(timeargs)
        sum_page_view_all(timeargs)

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
