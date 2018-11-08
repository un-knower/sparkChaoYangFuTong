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


"""-----------------------------------手机品牌-------------------------------------"""


def sum_phone_brand_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,brand,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM aldstat_daily_phonebrand where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,brand"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        r7 = 0
        r8 = 0
        if (row[7] == None):
            r7
        else:
            r7 = row[7]
        if (row[8] == None):
            r8
        else:
            r8 = row[8]
    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_phonebrand (app_key,day,brand,new_user_count,open_count,page_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),page_count=VALUES(page_count),open_count=VALUES(open_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at)
    """
    # 批量写入
    args = []
    for row in results:
        r7 = 0
        r8 = 0
        if (row[7] == None):
            r7
        else:
            r7 = row[7]
        if (row[8] == None):
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


"""----------------------------------手机类型--------------------------------------"""


def sum_phone_model_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "select app_key,phone_model,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time),SUM(one_page_count),SUM(one_page_count)/SUM(page_count),SUM(total_stay_time)/SUM(open_count) from ald_device_statistics where from_unixtime(date,'%Y-%m-%d') >=" + "'" + seven_days_ago + "'" + " and from_unixtime(date,'%Y-%m-%d') <=" + "'" + today + "'" + " GROUP BY phone_model,app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into ald_7days_device_statistics (app_key,date,phone_model,new_user_count,open_count,page_count,total_stay_time,one_page_count,bounce_rate,secondary_stay_time,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),bounce_rate=VALUES(bounce_rate),secondary_stay_time=VALUES(secondary_stay_time),update_at=VALUES(update_at)
    """
    # 批量写入
    args = []
    for row in results:
        r7 = 0
        r8 = 0
        if (row[7] == None):
            r7
        else:
            r7 = row[7]
        if (row[8] == None):
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


"""-------------------------------入口页------------------------------------"""


def sum_entrance_tmp(timedate):
    update_at_tmp = int(time.time())
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,page_path,SUM(entry_page_count),SUM(one_page_count),SUM(page_count),SUM(open_count),SUM(total_time),SUM(total_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM aldstat_entrance_page where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,page_path"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_single_entrance_page (app_key,day,page_path,entry_page_count,one_page_count,page_count,open_count,total_time,avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE entry_page_count=VALUES(entry_page_count),one_page_count=VALUES(one_page_count),page_count=VALUES(page_count),open_count=VALUES(open_count),total_time=VALUES(total_time),update_at=VALUES(update_at) ,avg_stay_time=VALUES(avg_stay_time),bounce_rate=VALUES(bounce_rate)"""
    # 批量写入
    args = []
    for row in results:
        for row in results:
            r7 = 0
        r8 = 0
        if (row[7] == None):
            r7
        else:
            r7 = row[7]
        if (row[8] == None):
            r8
        else:
            r8 = row[8]
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], float(r7), float(r8), update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""--------------------------------受访页---------------------------------------"""


def sum_page_tmp(timedate):
    update_at_tmp = int(time.time())
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,page_path,SUM(page_count),SUM(open_count),SUM(total_time),SUM(abort_page_count),SUM(share_count),SUM(total_time)/SUM(page_count),SUM(abort_page_count)/SUM(page_count) FROM aldstat_page_view where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,page_path"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中abort_ratio
    sql2 = """
        insert into aldstat_7days_single_page_view (app_key,day,page_path,page_count,open_count,total_time,abort_page_count,share_count,avg_stay_time,abort_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE page_count=VALUES(page_count),abort_page_count=VALUES(abort_page_count),open_count=VALUES(open_count),total_time=VALUES(total_time),share_count=VALUES(share_count),update_at=VALUES(update_at),avg_stay_time=VALUES(avg_stay_time),abort_ratio=VALUES(abort_ratio)"""
    # 批量写入
    args = []
    for row in results:
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
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6],float(r7), float(r8), update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""----------------------------------地域（安省）----------------------------------"""


def sum_province_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,province,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM aldstat_region_statistics where from_unixtime(day,'%Y-%m-%d') >=" + "'" + seven_days_ago + "'" + " and from_unixtime(day,'%Y-%m-%d') <=" + "'" + today + "'" + " GROUP BY app_key,province"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_province (app_key,day,province,new_user_count,open_count,page_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
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
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6],float(r7), float(r8), update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""----------------------------------地域（安市）----------------------------------"""


def sum_city_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,city,SUM(new_user_count),SUM(open_count),SUM(page_count),SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM aldstat_city_statistics where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,city"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
        insert into aldstat_7days_city_statistics (app_key,day,city,new_user_count,open_count,page_count,total_stay_time,one_page_count,secondary_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_user_count=VALUES(new_user_count),open_count=VALUES(open_count),page_count=VALUES(page_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),secondary_stay_time=VALUES(secondary_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        for row in results:
            r7 = 0
        r8 = 0
        if (row[7] == None):
            r7
        else:
            r7 = row[7]
        if (row[8] == None):
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


"""----------------------------------访问时长(待修改)--------------------------------------"""


def sum_duration_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT a.app_key,a.visit_duration,a.one_count,a.one_count/b.day_sum_count from (SELECT app_key,visit_duration,SUM(open_count) one_count from aldstat_visit_duration where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,visit_duration) a LEFT JOIN (SELECT app_key,SUM(open_count) day_sum_count from aldstat_visit_duration where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_visit_duration
(app_key,day,visit_duration,open_count,open_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE open_ratio=VALUES(open_ratio),open_count=VALUES(open_count),update_at=VALUES(update_at) """
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


"""--------------------------------访问频次-----------------------------"""

# def sum_frequency_tmp(timedate):
#     (today, seven_days_ago, update_at) = seven_time(timedate)
#     #     where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" +  "
#     #     where from_unixtime(date,'%Y-%m-%d') >=" + "'" + seven_days_ago + "'" + " and from_unixtime(date,'%Y-%m-%d') <=" + "'" + today + "'" +  "
#     #     new_scan_user_count,total_scan_count,qr_new_comer_for_app
#     sql = "SELECT a.app_key,a.opne_frequency,a.one_count,a.one_count/b.day_sum_count from (SELECT app_key,opne_frequency,SUM(visits_count) one_count from aldstat_visit_frequency where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,opne_frequency) a LEFT JOIN (SELECT app_key,SUM(visits_count) day_sum_count from aldstat_visit_frequency where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
#     conn = db_connect()
#     cur = conn.cursor()
#     cur.execute(sql)
#     results = cur.fetchall()
#     for row in results:
#         #         sql_data = ([ row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], row[7],row[8], update_at])
#         print(row[0], today, row[1], row[2], row[3], update_at)
#     # 将7日汇总结果写入 7 日结果表中
#     sql2 = """
#     insert into aldstat_7days_visit_frequency
# (app_key,day,opne_frequency,visits_count,visits_ratio,update_at)
#     values (%s,%s,%s,%s,%s,%s)
#     ON DUPLICATE KEY UPDATE visits_count=VALUES(visits_count),visits_ratio=VALUES(visits_ratio),update_at=VALUES(update_at) """
#     # 批量写入
#     args = []
#     for row in results:
#         sql_data = ([row[0], today, row[1], row[2], row[3], update_at])
#         args.append(sql_data)
#     try:
#         cur.executemany(sql2, args)
#     except Exception, e:
#         print Exception, ":", e, ' sql insert error'
#     # 关闭数据库连接
#     cur.close()
#     conn.commit()
#     conn.close()


"""----------------------------------访问深度（待修改）--------------------------------------"""


def sum_depth_tmp(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT a.app_key,a.visit_depth,a.one_count,a.one_count/b.sum_count from (SELECT app_key,visit_depth,SUM(open_count) one_count from aldstat_visit_depth where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,visit_depth) a LEFT JOIN (SELECT app_key,SUM(open_count) sum_count from aldstat_visit_depth where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key) b on a.app_key = b.app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_visit_depth
(app_key,day,visit_depth,open_count,open_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE open_count=VALUES(open_count),open_ratio=VALUES(open_ratio),update_at=VALUES(update_at) """
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


"""--------------------------------入口页（所有）------------------------------"""


def sum_entrance_all(timedate):
    update_at_tmp = int(time.time())
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,SUM(entry_page_count),SUM(one_page_count),SUM(page_count),SUM(open_count),SUM(total_time),SUM(total_time)/SUM(open_count),SUM(one_page_count)/SUM(page_count) FROM aldstat_daily_entrance_page where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_entrance_page
(app_key,day,entry_page_count,one_page_count,page_count,open_count,total_time,avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE entry_page_count=VALUES(entry_page_count),one_page_count=VALUES(one_page_count),page_count=VALUES(page_count),open_count=VALUES(open_count),total_time=VALUES(total_time),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], row[7], update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


"""--------------------------------受访页（所有）------------------------------"""


def sum_page_view_all(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    sql = "SELECT app_key,SUM(page_count),SUM(open_count),SUM(total_time),SUM(abort_page_count),SUM(share_count),SUM(total_time)/SUM(page_count),SUM(abort_page_count)/SUM(page_count) FROM aldstat_daily_page_view where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_page_view
(app_key,day,page_count,open_count,total_time,abort_page_count,share_count,avg_stay_time,abort_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE page_count=VALUES(page_count),abort_page_count=VALUES(abort_page_count),open_count=VALUES(open_count),total_time=VALUES(total_time),share_count=VALUES(share_count),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], today, row[1], row[2], row[3], row[4], row[5], row[6], row[7], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


#     """趋势分析（trend），单个场景值（scene）、分组场景值（scene_group_id）、
#     二维码（qr_key）、二维码组（qr_group_key）、访问深度（visit_depth）、
#     访问时长（visit_duration）、访问频次（visit_frequency）、地域按省（province）
#     、品牌分析（brand）、机型分析（model）、入口页（entrance）、受访页（respondents）。"""

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

if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    (dimension, timeargs) = args_main()
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
