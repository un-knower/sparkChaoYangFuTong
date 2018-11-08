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
Time:19:44
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


def sum_scene_tmp(yesterday_str):
    """场景值"""
    sql = "select app_key,day,type_value,visitor_count from ald_thirty_user_tmp where " \
          "type = 'scene' and day = " + "'" + yesterday_str + "'" + "  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询ak、date、scene_id、vc
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_single_scene = """
        insert into aldstat_30days_single_scene
        (app_key, day, scene_id, scene_visitor_count)
        values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE scene_visitor_count=VALUES(scene_visitor_count)"""
    # 批量写入
    args = []
    for row in results:
        # 将入库指标加入集合中，过滤掉scene_id为null和值大于10000的情况
        sql_data = (
            [row[0], row[1], 0 if row[2] == 'null' else (int(float(row[2])) if int(float(row[2])) < 10000 else 0),
             row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql_single_scene, args)  # 批量入库单个场景值表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_scene_group_tmp(yesterday_str):
    """场景值组"""
    sql = "select app_key,day,type_value,visitor_count from ald_thirty_user_tmp where " \
          "type = 'scene_group_id' and day = " + "'" + yesterday_str + "'" + "  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询场景值组的各项指标 ak,day,scene_group_id,vc
    results = cur.fetchall()
    # 将30日汇总结果写入 30 日结果表中
    sql_scene_group = """
        insert into aldstat_30days_scene_group
        (app_key, day, scene_group_id, visitor_count)
        values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count)"""

    args = []  # 定义批量入库集合
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql_scene_group, args)  # 批量入库场景值组
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_qr_tmp(yesterday_str):
    """二维码计算"""
    sql = "select app_key,day,type_value,visitor_count from ald_thirty_user_tmp where " \
          "type = 'qr_key' and day = " + "'" + yesterday_str + "'" + "  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询二维码的各项指标
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql_single_qr = """
        insert into aldstat_30days_single_qr
        (app_key, day, qr_key, qr_visitor_count)
        values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count)"""

    args = []  # 定义批量入库集合
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3]])
        args.append(sql_data)  # 将入库指标加入批量入库集合中
    try:
        cur.executemany(sql_single_qr, args)  # 批量入库单个二维码表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_qr_group_tmp(yesterday_str):
    """二维码组计算"""
    sql = "select app_key,day,type_value,visitor_count from ald_thirty_user_tmp " \
          "where type = 'qr_group_key' and day = " + "'" + yesterday_str + "'" + "  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询二维码入库的各项指标
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql2 = """
        insert into aldstat_30days_single_qr_group
        (app_key, day, qr_group_key, qr_visitor_count)
        values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count)"""

    args = []  # 定义批量入库集合
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)  # 批量入库二维码组
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_qr_all(yesterday_str):
    """所有二维码"""
    sql = "select app_key,day,visitor_count from ald_thirty_user_tmp where " \
          "type = 'qr_key_all' and day = " + "'" + yesterday_str + "'" + "  group by app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 汇总一个小程序下的所有二维码
    results = cur.fetchall()

    # 将30日汇总结果写入30日结果表中
    sql_all_qr = """
        insert into aldstat_30days_all_qr
        (app_key, day, qr_visitor_count)
        values (%s,%s,%s)
        ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count)"""

    args = []  # 定义批量入库集合
    for row in results:
        sql_data = ([row[0], row[1], row[2]])
        args.append(sql_data)
    try:
        cur.executemany(sql_all_qr, args)  # 一个小程序下30天的所有二维码批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def sum_trend_all(yesterday_str):
    """趋势分析"""
    sql = "select app_key,day,visitor_count from ald_thirty_user_tmp where " \
          "type = 'trend' and day = " + "'" + yesterday_str + "'" + "  group by app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)  # 查询趋势分析中需要入库的各项指标
    results = cur.fetchall()

    # 将30日汇总结果写入 30 日结果表中
    sql_trend_analysis = """
        insert into aldstat_30days_trend_analysis
        (app_key, day, visitor_count)
        values (%s,%s,%s)
        ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count)"""
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], row[1], row[2]])
        args.append(sql_data)
    try:
        cur.executemany(sql_trend_analysis, args)  # 批量入库趋势分析表
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    ald_start_time = int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    today = datetime.now()  # 获取今天日期
    yesterday = today + timedelta(days=-1)  # 获取昨天的日期
    yesterday_str = yesterday.strftime("%Y-%m-%d")  # 将日期转换成str类型
    sum_scene_tmp(yesterday_str)
    sum_scene_group_tmp(yesterday_str)
    sum_qr_tmp(yesterday_str)
    sum_qr_group_tmp(yesterday_str)
    sum_qr_all(yesterday_str)
    sum_trend_all(yesterday_str)

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
