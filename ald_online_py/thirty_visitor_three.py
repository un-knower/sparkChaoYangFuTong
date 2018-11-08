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
Time:20:43
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


update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sum_thirty_user_tmp(yesterday_str, type_index, tablename, date, type_field, visitor_count):
    """第三期所有的 yesterday_str:昨天日期、type_index:指标类型、tablename:30天对应的数据库表、date:日期字段、type_field:类型字段、指标字段名"""
    type_tmp = type_index
    thirty_tables = tablename
    type_column = type_field
    visitor_column = visitor_count
    date_column = date
    sql = "select app_key,day,type_value,visitor_count from ald_thirty_user_tmp where " \
          "type = '" + type_tmp + "' and day = " + "'" + yesterday_str + "'" + "  " \
          "group by app_key,type_value"
    cur.execute(sql)
    results = cur.fetchall()  # 查询入库的各项指标
    # 将30日汇总结果写入 30 日结果表中
    sql_summary = '''insert into ''' + thirty_tables + '''(app_key, ''' + date_column + ''',
           ''' + type_column + ''', ''' + visitor_column + ''',update_at)values (%s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE ''' + visitor_column + '''=VALUES(''' + visitor_column + '''),
           update_at = VALUES (update_at)'''

    args = []  # 定义批量入库列表
    for row in results:
        # 按照入库表的不同分别将指标添加到批量入库集合中
        if row[0] != "":
            if thirty_tables == "aldstat_30days_single_page_view" or thirty_tables == "aldstat_30days_single_entrance_page":
                sql_data = ([row[0], row[1], row[2], row[3], int(time.time())])
            else:
                sql_data = ([row[0], row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql_summary, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


def sum_thirty_all_user_tmp(yesterday_str, type_index, tablename, date, type_field):
    """需要汇总的 yesterday_str:昨天日期、type_index:指标类型、tablename:30天对应的数据库表、date:日期字段、type_field:类型字段、指标字段名"""
    type_tmp = type_index
    seven_tables = tablename
    visitor_column = type_field
    date_column = date
    sql = "select app_key,day,visitor_count from ald_thirty_user_tmp where type = '" + type_tmp \
          + "' and day = " + "'" + yesterday_str + "'" + "  group by app_key"
    cur.execute(sql)
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_summary_user = '''
         insert into ''' + seven_tables + '''(app_key, ''' + date_column + ''',
         ''' + visitor_column + ''',update_at)values (%s,%s,%s,%s)
         ON DUPLICATE KEY UPDATE ''' + visitor_column + '''=VALUES(''' + visitor_column + '''),update_at = VALUES (update_at)
         '''

    args = []  # 定义批量入库集合
    for row in results:
        if row[0] != "":
            if seven_tables == "aldstat_30days_entrance_page":
                sql_data = ([row[0], row[1], row[2], int(time.time())])
            else:
                sql_data = ([row[0], row[1], row[2], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql_summary_user, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


def sum_thirty_user_ratio_tmp(yesterday_str):
    """访问占比需要单独计算/访问时长"""
    sql = "SELECT a.app_key,a.type_value,a.one_count,a.one_count/b.day_sum_count from " \
          "(SELECT app_key,type_value,SUM(visitor_count) one_count from ald_thirty_user_tmp where " \
          "type = 'visit_duration' and day = " + "'" + yesterday_str + "'" \
          + " GROUP BY app_key,type_value) a LEFT JOIN (SELECT app_key,SUM(visitor_count) " \
            "day_sum_count from ald_thirty_user_tmp where type = 'visit_duration' and day = " \
            "" + "'" + yesterday_str + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
    cur.execute(sql)  # 查询入库的各项指标并计算访问时长
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_summary_ratio = '''
        insert into aldstat_30days_visit_duration
        (app_key,day,visit_duration ,visitor_count ,visitor_ratio,update_at)values (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count),update_at =
        VALUES (update_at),visitor_ratio = VALUES (visitor_ratio)
        '''

    args = []  # 定义批量入库集合
    for row in results:
        if row[0] != "":
            sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql_summary_ratio, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


def sum_thirty_user_ratio_depth(yesterday_str):
    """访问占比需要单独计算/访问深度"""
    sql = "SELECT a.app_key,a.type_value,a.one_count,a.one_count/b.day_sum_count from " \
          "(SELECT app_key,type_value,SUM(visitor_count) one_count from ald_thirty_user_tmp where " \
          "type = 'visit_depth' and day = " + "'" + yesterday_str + "'" + " GROUP BY app_key,type_value)" \
          " a LEFT JOIN (SELECT app_key,SUM(visitor_count) day_sum_count from " \
          "ald_thirty_user_tmp where type = 'visit_depth' and day = " + "'" + yesterday_str \
          + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
    cur.execute(sql)  # 查询入库的各项指标并计算访问深度
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_summary_depth = '''
        insert into aldstat_30days_visit_depth
        (app_key,day,visit_depth ,visitor_count ,visitor_ratio,update_at)values (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count),update_at = VALUES (update_at),visitor_ratio = VALUES (visitor_ratio)
        '''

    args = []  # 定义批量入库集合
    for row in results:
        if row[0] != "":
            sql_data = ([row[0], yesterday_str, row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql_summary_depth, args)  # 批量入库访问深度表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


if __name__ == '__main__':
    ald_start_time = int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = db_connect()
    cur = conn.cursor()

    today = datetime.now()  # 获取今天日期
    yesterday = today + timedelta(days=-1)  # 获取昨天日期
    yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型

    # 手机品牌 参数： 日期      type为什么    七天对应的数据库表    日期字段(t)      对应的类型     指标字段名
    sum_thirty_user_tmp(yesterday_str, "brand", "aldstat_30days_phonebrand", "day", "brand", "visitor_count")
    # 手机型号
    sum_thirty_user_tmp(yesterday_str, "model", "ald_30days_device_statistics", "date", "phone_model", "visitor_count")
    # 入口页
    sum_thirty_user_tmp(yesterday_str, "entrance", "aldstat_30days_single_entrance_page", "day", "page_path",
                        "visitor_count")
    # 省份
    sum_thirty_user_tmp(yesterday_str, "province", "aldstat_30days_province", "day", "province", "visitor_count")
    # 城市
    sum_thirty_user_tmp(yesterday_str, "city", "aldstat_30days_city_statistics", "day", "city", "visitor_count")
    # 访问时长
    sum_thirty_user_ratio_tmp(yesterday_str)
    # 访问深度
    sum_thirty_user_ratio_depth(yesterday_str)
    # 受访页分析
    sum_thirty_user_tmp(yesterday_str, "respondents", "aldstat_30days_single_page_view", "day", "page_path",
                        "visitor_count")

    # 所有入口页
    sum_thirty_all_user_tmp(yesterday_str, "entrance_all", "aldstat_30days_entrance_page", "day", "visitor_count")
    # 所有受访页
    sum_thirty_all_user_tmp(yesterday_str, "respondents_all", "aldstat_30days_page_view", "day", "visitor_count")

    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
