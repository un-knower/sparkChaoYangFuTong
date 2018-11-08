# -*- coding:utf-8 -*-
import re
import sys
import time
from datetime import datetime, timedelta
import MySQLdb
import dbconf

def db_connect():
    """ 数据库连接函数 """
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn

def sum_seven_user(yesterday, type_tmp, table_name, date_column, type_column, visitor_column):
    """ 各指标最近 7 日访问人数(去重) 入库"""

    update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间
    # 从 ald_seven_user_tmp  最近 7 日人数(去重)表中 select 出各指标的访问人数
    sql = "select app_key,day,type_value,visitor_count from ald_seven_user_tmp where type = '" + type_tmp + "' and day = " + "'" + yesterday + "'" + "  group by app_key,type_value"
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句， 将最近7日访问人数(去重)的结果写入 7 日结果表中
    sql2 = '''
        insert into ''' + table_name + '''
      (app_key, ''' + date_column + ''', ''' + type_column + ''', ''' + visitor_column + ''',update_at)
    values (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE ''' + visitor_column + '''=VALUES(''' + visitor_column + '''),update_at = VALUES (update_at)
        '''

    # 批量写入
    args = []
    for row in results:
        if row[0] != "":
            if table_name in ['aldstat_7days_single_page_view', 'aldstat_7days_single_entrance_page', 'aldstat_7days_single_page_view']:
                # 如果数据库表是这三个, 则写入 app_key,day,type_value,visitor_count和更新时间
                sql_data = ([row[0], row[1], row[2], row[3], int(time.time())])
            else:
                # 否则更新时间的格式是 YYMMDD HH:MM:SS
                sql_data = ([row[0], row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 访问人数批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

def sum_seven_all_user(yesterday, type_tmp, table_name, date_column, visitor_column):
    """ 最近7日所有用户(去重) 入库"""

    # 从 ald_seven_user_tmp  最近 7 日人数(去重)表中 select 出每个小程序的访问人数
    update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间, YYMMDD HH:MM:SS 格式
    sql = "select app_key,day,visitor_count from ald_seven_user_tmp where type = '" + type_tmp + "' and day = " + "'" + yesterday + "'" + "  group by app_key"
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将7日汇总结果写入 7 日结果表中
    sql2 = '''
        insert into ''' + table_name + '''
      (app_key, ''' + date_column + ''', ''' + visitor_column + ''',update_at)
    values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE ''' + visitor_column + '''=VALUES(''' + visitor_column + '''),update_at = VALUES (update_at)
        '''
    # 批量写入
    args = []
    for row in results:
        if row[0] != "":
            if table_name == "aldstat_7days_entrance_page":
                # aldstat_7days_entrance_page 表的更新时间为 unixtime
                sql_data = ([row[0], row[1], row[2], int(time.time())])
            else:
                # 其余数据库表的更新时间为 YYMMDD HH:MM:SS 格式
                sql_data = ([row[0], row[1], row[2], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 所有用户 7 日访问人数(去重)入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

def sum_seven_user_ratio(yesterday):
    """ 访问占比需要单独计算/访问时长 """

    update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间, YYMMDD HH:MM:SS 格式
    sql = "SELECT a.app_key,a.type_value,a.one_count,a.one_count/b.day_sum_count from (SELECT app_key,type_value,SUM(visitor_count) one_count from ald_seven_user_tmp where type = 'visit_duration' and day = " + "'" + yesterday + "'" + " GROUP BY app_key,type_value) a LEFT JOIN (SELECT app_key,SUM(visitor_count) day_sum_count from ald_seven_user_tmp where type = 'visit_duration' and day = " + "'" + yesterday + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将7日汇总结果写入 7 日结果表中
    sql2 = '''
        insert into aldstat_7days_visit_duration
      (app_key,day,visit_duration ,visitor_count ,visitor_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count),update_at = VALUES (update_at),visitor_ratio = VALUES (visitor_ratio)
        '''
    # 批量写入
    args = []
    for row in results:
        if row[0] != "":
            sql_data = ([row[0], yesterday,row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 最近7日每个小程序访问人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

def sum_seven_user_ratio_depth(yesterday):
    """ 访问占比需要单独计算/访问深度 """

    # 计算访问深度和访问深度占比
    update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间, YYMMDD HH:MM:SS 格式
    sql = "SELECT a.app_key,a.type_value,a.one_count,a.one_count/b.day_sum_count from (SELECT app_key,type_value,SUM(visitor_count) one_count from ald_seven_user_tmp where type = 'visit_depth' and day = " + "'" + yesterday + "'" + " GROUP BY app_key,type_value) a LEFT JOIN (SELECT app_key,SUM(visitor_count) day_sum_count from ald_seven_user_tmp where type = 'visit_depth' and day = " + "'" + yesterday + "'" + " GROUP BY app_key) b on a.app_key=b.app_key"
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将7日汇总结果写入 7 日结果表中
    sql2 = '''
        insert into aldstat_7days_visit_depth
      (app_key,day,visit_depth ,visitor_count ,visitor_ratio,update_at)
    values (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count),update_at = VALUES (update_at),visitor_ratio = VALUES (visitor_ratio)
        '''
    # 批量写入
    args = []
    for row in results:
        if row[0] != "":
            sql_data = ([row[0], yesterday,row[1], row[2], row[3], update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = db_connect()
    cur = conn.cursor()

    # 获取今天日期
    now = datetime.now()
    # 获取昨天的日期
    yesterday = (now + timedelta(days=-1)).strftime("%Y-%m-%d")

    # 日期      type为什么    七天对应的数据库表    日期字段(t)      对应的类型     指标字段名
    # 手机品牌
    sum_seven_user(yesterday, "brand", "aldstat_7days_phonebrand", "day", "brand", "visitor_count")
    # 手机型号
    sum_seven_user(yesterday, "model", "ald_7days_device_statistics", "date", "phone_model", "visitor_count")
    # 入口页
    sum_seven_user(yesterday, "entrance", "aldstat_7days_single_entrance_page", "day", "page_path", "visitor_count")
    # 省份
    sum_seven_user(yesterday, "province", "aldstat_7days_province", "day", "province", "visitor_count")
    # 城市
    sum_seven_user(yesterday, "city", "aldstat_7days_city_statistics", "day", "city", "visitor_count")
    # 访问时长
    sum_seven_user_ratio(yesterday)
    # 访问深度
    sum_seven_user_ratio_depth(yesterday)
    # 受访页分析
    sum_seven_user(yesterday, "respondents", "aldstat_7days_single_page_view", "day", "page_path", "visitor_count")

    # 所有入口页
    sum_seven_all_user(yesterday, "entrance_all", "aldstat_7days_entrance_page", "day", "visitor_count")
    # 所有受访页
    sum_seven_all_user(yesterday, "respondents_all", "aldstat_7days_page_view", "day", "visitor_count")

    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
