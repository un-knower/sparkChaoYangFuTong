# -*- coding:utf-8 -*-
import re
import sys
import time
from datetime import datetime, timedelta
import MySQLdb
import dbconf

"""
    created by weilongsheng on 2017-11-09
"""

def db_connect():
    """ 连接数据库的函数 """
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn

def sum_scene_tmp(yesterday):
    """ 单个场景值 7 日访问人数(去重) 入库"""


#=============================================================1
    #gcs:这是怎样计算出来的7天的人数呢？ 从数据中将昨天的数据读取出来

    #  从七日人数去重表 ald_seven_user_tmp 中 select 出最近7天的每个小程序下各场景值的访问人数(去重)
    sql = "select app_key,day,type_value,visitor_count from ald_seven_user_tmp where type = 'scene' and day = " + "'" + yesterday + "'" +"  group by app_key,type_value"
    conn = db_connect() #gcs:获得conn的连接
    cur = conn.cursor()
    cur.execute(sql)#gcs:使用conn进行连接之后执行这一条sql语句。
    results = cur.fetchall()  #gcs:将所有的sql的执行的结果返还回来

#=============================================================2
    """gcs:\n
    将昨天的数据插入到 aldstat_7days_single_scene 这个表当中。
    这个数据的插入的方式让我感觉到很疑惑。results是昨天的一天的结果，为什么把昨天一天的结果，覆盖到场景值的数据库当中呢?
    这里似乎并没有采取让day >=yesterday的方式将数据进行累加啊?
    """

#=============================================================3
    """gcs:\n
    将最终的结果插入到mySql数据库中
    批量执行mySql当中的数据集的插入到数据库当中
    """
    # 准备入库的 sql 语句, 将7日汇总结果写入 aldstat_7days_single_scene
    sql2 = """
        insert into aldstat_7days_single_scene
      (app_key, day, scene_id, scene_visitor_count)
    values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE scene_visitor_count=VALUES(scene_visitor_count) """

    # 批量写入
    args = []
    for row in results:
        # row[0] -> app_key,  row[1] -> day, row[2] -> 场景值 ID, row[3] -> visitor_count
        sql_data = ([row[0], row[1], 0 if row[2] == 'null' else (int(float(row[2])) if int(float(row[2])) < 10000 else 0 ) , row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 单个场景值7日人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

def sum_scene_group_tmp(yesterday):
    """ 场景值组最近 7 日访问人数(去重)入库 """

    #  从 7日人数(去重) 表中 select 出最近 7 天每个小程序下各场景值的访问人数
    sql = "select app_key,day,type_value,visitor_count from ald_seven_user_tmp where type = 'scene_group_id' and day = " + "'" + yesterday + "'" +"  group by app_key,type_value"
    conn = db_connect()      # 获取数据库连接
    cur = conn.cursor()      # 获取游标
    cur.execute(sql)         # 执行 sql 语句
    results = cur.fetchall() # 获取 sql 查询的结果

    # 准备 sql 语句, 将场景值组的7日人数(去重)结果写入 aldstat_7days_scene_group
    sql2 = """
        insert into aldstat_7days_scene_group
      (app_key, day, scene_group_id, visitor_count)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count) """

    # 批量写入
    args = []
    for row in results:
        # row[0] -> app_key,  row[1] -> day, row[2] -> type_value, row[3] -> visitor_count
        sql_data = ([row[0],  row[1], row[2], row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 场景值组的 7 日人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()   # 关闭游标
    conn.commit() # 提交 sql
    conn.close()  # 关闭数据库连接

def sum_qr_tmp(yesterday):
    """ 单个二维码 7 日扫码人数(去重) 入库 """

    # 从 7日人数(去重) 表中 select 出最近 7 天每个小程序下各二维码的扫码人数
    sql = "select app_key,day,type_value,visitor_count from ald_seven_user_tmp where type = 'qr_key' and day = " + "'" + yesterday + "'" +"  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将单个二维码7日扫码人数(去重)的结果写入 aldstat_7days_single_qr
    sql2 = """
        insert into aldstat_7days_single_qr
      (app_key, day, qr_key, qr_visitor_count)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count)"""

    # 批量写入
    args = []
    for row in results:
        # row[0] -> app_key,  row[1] -> day, row[2] -> type_value, row[3] -> visitor_count
        sql_data = ([row[0],  row[1], row[2], row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 单个二维码最近7日扫码人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

def sum_qr_group_tmp(yesterday):
    """ 二维码组最近 7 日扫码人数(去重)入库 """

    # 从 7日人数(去重) 表中 select 出最近 7 天每个小程序下各二维码组的扫码人数
    sql = "select app_key,day,type_value,visitor_count from ald_seven_user_tmp where type = 'qr_group_key' and day = " + "'" + yesterday + "'" +"  group by app_key,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将二维码组7日扫码人数(去重)结果写入 aldstat_7days_single_qr_group
    sql2 = """
        insert into aldstat_7days_single_qr_group
      (app_key, day, qr_group_key, qr_visitor_count)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count) """

    # 批量写入
    args = []
    for row in results:
        # row[0] -> app_key,  row[1] -> day, row[2] -> type_value, row[3] -> visitor_count
        sql_data = ([row[0],  row[1], row[2], row[3]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 二维码组 7 日扫码人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

# 所有二维码
def sum_qr_all(yesterday):
    """ 每个小程序下所有二维码最近 7 日的扫码人数(去重)入库 """

    # 从 7日人数(去重) 表中 select 出最近 7 天每个小程序下所有二维码的扫码人数
    sql = "select app_key,day,visitor_count from ald_seven_user_tmp where type = 'qr_key_all' and day = " + "'" + yesterday + "'" +"  group by app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将每个小程序下的最近7日扫码人数(去重)结果写入 aldstat_7days_all_qr
    sql2 = """
        insert into aldstat_7days_all_qr
      (app_key, day, qr_visitor_count)
    values (%s,%s,%s)
    ON DUPLICATE KEY UPDATE qr_visitor_count=VALUES(qr_visitor_count)"""

    # 批量写入
    args = []
    for row in results:
        # row[0] -> app_key,  row[1] -> day, row[2] -> visitor_count
        sql_data = ([row[0],  row[1], row[2]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 各小程序下所有二维码扫码人数的批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

# 趋势分析
def sum_trend_all(yesterday):
    """ 趋势分析最近 7 日访问人数(去重) 入库 """

    # 从 7日人数(去重) 表中 select 出最近 7 天每个小程序的访问人数(去重)
    sql = "select app_key,day,visitor_count from ald_seven_user_tmp where type = 'trend' and day = " + "'" + yesterday + "'" +"  group by app_key"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # 准备 sql 语句, 将最近 7日 访问人数(去重)的结果写入 aldstat_7days_trend_analysis
    sql2 = """
        insert into aldstat_7days_trend_analysis
      (app_key, day, visitor_count)
    values (%s,%s,%s)
    ON DUPLICATE KEY UPDATE visitor_count=VALUES(visitor_count) """

    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0],  row[1], row[2]])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args) # 各小程序最近 7 日访问人数(去重)批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    #=============================================================1
    #gcs:获得当前的时间。并且将当前的时间转换为int类型的时间
    ald_start_time =  int(time.time()) #gcs:获得当前的以秒设定的时间
    #=============================================================2
    #gcs:将今天的时间转换为Y-m-d H:M:S的时间格式
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 获取今天日期
    now = datetime.now()
    # 将日期转换成str类型
    yesterday = (now + timedelta(days=-1)).strftime("%Y-%m-%d")  #gcs:昨天的时间为now-1 天的时间，并且将时间的格式设定为Y-m-d的格式

    #=============================================================3
    #gcs:这是计算场景值的7日的代码
    # 最近 7 日场景值访问人数
    sum_scene_tmp(yesterday)
    # 最近 7 日场景值人数
    sum_scene_group_tmp(yesterday)
    # 最近 7 日二维码扫码人数
    sum_qr_tmp(yesterday)
    # 最近 7 日二维码组扫码人数
    sum_qr_group_tmp(yesterday)
    # 最近 7 日二维码扫码次数
    sum_qr_all(yesterday)
    # 最近 7 日趋势分析访问人数
    sum_trend_all(yesterday)

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
