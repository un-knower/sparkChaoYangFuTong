# -*- coding: utf-8 -*-
import datetime
import time
from itertools import groupby

import MySQLdb
import redis
import dbconf

"""
Created by:JetBrains PyCharm Community Edition 2016.1(64)
User:zhangzhenwei
Date:2018-01-06
Time:14:32
"""
def redis_connect():
    """连接 Redis"""

    # pool = redis.ConnectionPool(host='10.0.0.200', port=6379, password='crs-1qen55zy:aldwx!23test', db=6)
    # r = redis.Redis(connection_pool=pool)
    pool = redis.ConnectionPool(host='10.0.0.156', port=6379, password='crs-ezh65tiq:aldwxredis123', db=9)
    r = redis.Redis(connection_pool=pool)
    return r


def prepare_sql(table, insert_fields, update_fields):
    """准备 sql 语句模版"""
    on_duplicate_key = []
    placeholder = []

    [on_duplicate_key.append(item + '=' + 'VALUES(' + item + ')') for item in update_fields]
    [placeholder.append('%s') for item in insert_fields]

    insert_columns = '(' + ','.join(insert_fields) + ')'
    values = 'values(' + ','.join(placeholder) + ')'
    on_duplicate_key_update = ','.join(on_duplicate_key)

    return "insert into %s %s %s ON DUPLICATE KEY UPDATE %s" % (table, insert_columns, values, on_duplicate_key_update)


def db_connect():
    """连接mysql"""
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn


def hll_daysum_link_vc(items):
    """日汇总外链访问人数计算方法，items为redis_key列表，类似于：HLL_ZongLink_vc_day_ak_linkkey"""
    insert_fields = ['app_key', 'day', 'link_visitor_count', 'update_at']  # 插入的字段
    update_fields = ['link_visitor_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_link_summary', insert_fields, update_fields) #gcs:准备sql语句

    args = []  # 定义批量入库集合
    dic = {}  # 字典，去掉link_key带来的影响
    for ite in items:
        spt = ite.split("_")
        link_key = spt[5]
        ak = spt[4]
        day = spt[3]
        vc_count = r.pfcount(ite)  # 访问人数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s''' % (ak, day)  # 拼接字典key
            if (dickey in dic):
                temp = int(dic.get(dickey))  # temp临时存储字典value
                if (temp < vc_count):
                    dic[dickey] = vc_count
                else:
                    dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = vc_count
    # k:字典key,由ak、day字段组成； v:字典value，访问人数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], v, update_at)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, 'HLL_DailySum_link_vc 日汇总外链访问人数写入异常!', '============'


def hll_daysum_link_oc(items):
    """外链日汇总打开次数计算方法，items为redis_key列表，类似于：HLL_ZongLink_oc_day_ak_linkkey """
    insert_fields = ['app_key', 'day', 'link_open_count', 'update_at']  # 插入的字段
    update_fields = ['link_open_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_link_summary', insert_fields, update_fields) #gcs:准备mysql的语句

    args = []  # 定义批量入库集合
    dic = {}  # 字典，去掉link_key带来的影响
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        link_key = spt[5]
        oc_count = r.pfcount(ite)  # 打开次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s''' % (ak, day)  # 使用这个三引号将字符串进行拼接字典key
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(oc_count)  # 取出字典value值累加操作
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = oc_count
    # k:字典key，由ak,day组成； v:字典value，打开次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], v, update_at)
        args.append(sql_data)

    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, 'HLL_DailySum_link_oc 外链日汇总打开次数写入异常!', '============'


def hll_daysum_link_tpc(items):
    """外链日汇总访问次数计算方法，items为redis_key列表，类似于：STR_ZongLink_tpc_day_ak_linkkey """
    insert_fields = ['app_key', 'day', 'link_page_count', 'update_at']  # 插入的字段
    update_fields = ['link_page_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_link_summary', insert_fields, update_fields)

    args = []  # 定义批量入库集合
    dic = {}  # 字典，去掉link_key带来的影响
    for ite in items:  # ite:单个redis_key
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        link_key = spt[5]
        tpc_count = r.get(ite)  # 访问次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s''' % (ak, day)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(tpc_count)
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = tpc_count
    # k:字典key,由ak、day字段组成； v:字典value，访问次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], v, update_at)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, 'HLL_DailySum_link_tpc 外链日汇总访问次数写入异常!', '============'


def hll_daysum_link_ncc(items):
    """外链日汇总新增人数计算方法，items为redis_key列表，类似于：HLL_ZongLink_ncc_day_ak_linkkey """
    insert_fields = ['app_key', 'day', 'link_newer_for_app', 'update_at']  # 插入的字段
    update_fields = ['link_newer_for_app']  # 需要更新的字段
    sql = prepare_sql('aldstat_link_summary', insert_fields, update_fields)

    args = []  # 定义批量入库集合
    dic = {}  # 字典，去掉link_key带来的影响
    for ite in items:  # 单个redis_key
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        link_key = spt[5]
        nucount = r.pfcount(ite)  # 新增用户人数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s''' % (ak, day)  # 字典key
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(nucount)  # 获取字典的value累加新增用户
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = nucount
    # k:字典key，由ak、day组成 v:字典value，新增用户数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], v, update_at)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, 'HLL_DailySum_link_ncc 外链日汇总新增人数写入异常!', '============'


def hll_daysum_link_bc(items):
    """外链日汇总 跳出页个数、跳出率计算方法，items为redis_key列表，类似于：HS_ZongLink_bc_day_ak_linkkey """
    insert_fields = ['app_key', 'day', 'one_page_count', 'bounce_rate', 'update_at']  # 插入的字段
    update_fields = ['one_page_count', 'bounce_rate']  # 需要更新的字段
    sql = prepare_sql('aldstat_link_summary', insert_fields, update_fields)
    args = []  # 定义批量入库集合
    dic = {}  # 字典，去掉link_key带来的影响
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        link_key = spt[5]
        bc_count = 0  # 跳出页个数
        for value in r.hvals(ite):  # value：获取rediskey的value值
            if value == "1":
                bc_count += 1

        if (str(NOTDElKEYS).__contains__(link_key)):
            pc_sql = """select link_page_count from aldstat_link_summary where app_key='%s' and day = '%s'""" % (
                ak, day)  # 从外链日汇总表中查询访问次数
            cur.execute(pc_sql)
            pc = cur.fetchone()[0]  # pc:访问次数
            dickey = '''%s_%s_%s''' % (ak, day, pc)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + bc_count  # 取出字典的value，累加跳出页个数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = bc_count
    # k:字典key，由ak、day、pc组成 v:字典value,跳出页个数
    for (k, v) in dic.items():
        splits = k.split("_")
        if (v == 0):
            bounce_rate = 0
        else:
            bounce_rate = v / float(splits[2])  # 计算跳出率
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], v, bounce_rate, update_at)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, 'HLL_DailySum_link_bc 外链日汇总 跳出页个数、跳出率写入异常!', args, '==========='


def hll_daydaily_vc(items, insertfields, updatefields, tablename):
    """日详情访问人数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，类似于：HLL_Link_vc_day_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])  # 调用拼接sql的方法
    args = []  # 定义批量入库集合
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        id = spt[5]
        link_key = spt[6]
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        vccount = r.pfcount(ite)
        if (str(NOTDElKEYS).__contains__(link_key)):
            sql_data = (ak, day, update_at, id, vccount)  # ak, day, update_at固定
            args.append(sql_data)

    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情访问人数写入异常!', args, '============'


def hll_hourdaily_vc(items, insertfields, updatefields, tablename):
    """时详情访问人数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，类似于：HLL_HourLink_vc_day_hour_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    for ite in items:
        spt = ite.split("_")
        ak = spt[5]
        day = spt[3]
        hour = spt[4]
        id = spt[6]
        link_key = spt[7]
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hvccount = r.pfcount(ite)  # 小时访问人数
        sql_data = (ak, day, update_at, hour, id, hvccount)  # ak, day, update_at固定
        if (str(NOTDElKEYS).__contains__(link_key)):
            args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的时详情访问人数写入异常!', '============'


def hll_daydaily_oc(items, insertfields, updatefields, tablename):
    """日详情打开次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，类似于：HLL_Link_oc_day_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        id = spt[5]
        link_key = spt[6]
        occount = r.pfcount(ite)  # 统计打开次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s''' % (ak, day, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(occount)  # 获取字典key累加打开次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = occount
    # k:字典key，由ak、day、id组成 v:字典value，打开次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情打开次数写入异常!', args, '============'


def hll_hourdaily_oc(items, insertfields, updatefields, tablename):
    """日详情打开次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，
    类似于：HLL_HourLink_oc_day_hour_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}  # 字典
    for ite in items:
        spt = ite.split("_")
        ak = spt[5]
        day = spt[3]
        hour = spt[4]
        id = spt[6]
        link_key = spt[7]
        hoccount = r.pfcount(ite)  # 时详情打开次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s_%s''' % (ak, day, hour, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(hoccount)  # 获取字典value累加打开次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = hoccount
    # k:字典key，由ak,day,hour,id字段组成 v:字典value,打开次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], splits[3], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的时详情打开次数写入异常!', args, '============'


# 日详情访问次数方法
def hll_daydaily_tpc(items, insertfields, updatefields, tablename):
    """日详情访问次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，
        类似于：HLL_Link_tpc_day_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}  # 字典
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        id = spt[5]
        link_key = spt[6]
        tpccount = r.get(ite)  # 统计访问次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s''' % (ak, day, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(tpccount)  # 获取字典value,累加访问次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = tpccount
    # k:字典key，由ak,day,id字段组成 v:字典value，访问次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情访问次数写入异常!', args, '============'


def hll_hourdaily_pc(items, insertfields, updatefields, tablename):
    """时详情访问次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，
        类似于：HLL_HourLink_pc_day_hour_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}
    for ite in items:
        spt = ite.split("_")
        ak = spt[5]
        day = spt[3]
        hour = spt[4]
        id = spt[6]
        link_key = spt[7]
        hpccount = r.get(ite)  # 统计访问次数
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s_%s''' % (ak, day, hour, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(hpccount)  # 获取字典value,累加访问次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = hpccount

    # k:字典key，由ak,day,hour,id字段组成 v:字典value，访问次数
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], splits[3], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的时详情访问次数方法写入异常!', '============'


# 日详情新增用户方法
def hll_daydaily_ncc(items, insertfields, updatefields, tablename):
    """时详情访问次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，
        类似于：HLL_Link_ncc_day_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        id = spt[5]
        link_key = spt[6]
        nucount = r.pfcount(ite)  # 统计新增用户
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s''' % (ak, day, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(nucount)  # 获取字典value,累加新增用户
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = nucount
    # k:字典key，由ak,day,id字段组成 v:字典value，新增用户
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情新增人数写入异常!', '============'


# 时详情新增用户方法
def hll_hourdaily_ncc(items, insertfields, updatefields, tablename):
    """时详情访问次数方法，items为redis_key列表,insertfields:插入字段,updatefields:更新字段，
        类似于：HLL_HourLink_ncc_day_hour_ak_linkkey """
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}
    for ite in items:
        spt = ite.split("_")
        ak = spt[5]
        day = spt[3]
        hour = spt[4]
        id = spt[6]
        link_key = spt[7]
        nucount = r.pfcount(ite)  # 统计时详情新增用户
        if (str(NOTDElKEYS).__contains__(link_key)):
            dickey = '''%s_%s_%s_%s''' % (ak, day, hour, id)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + int(nucount)  # 获取字典value,累加新增用户
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = nucount
    # k:字典key，由ak,day，hour,id字段组成 v:字典value，新增用户
    for (k, v) in dic.items():
        splits = k.split("_")
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], splits[3], v)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的时详情新增人数写入异常!', args, '============'


def hll_daydaily_bc(items, insertfields, updatefields, tablename, pckey, keyid):
    """日详情跳出页个数、跳出率，items为redis_key列表，类似于：HLL_Link_ncc_day_ak_linkkey，insertfields:插入字段,updatefields:更新字段，
    pckey:要查询数据库的条件，keyid:要查询数据库的值。"""
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}  # 字典
    for ite in items:
        spt = ite.split("_")
        ak = spt[4]
        day = spt[3]
        id = spt[5]
        link_key = spt[6]
        bccount = 0  # 跳出页个数
        for value in r.hvals(ite):  # qq:rediskey的value值
            if value == "1":
                bccount += 1
        if (str(NOTDElKEYS).__contains__(link_key)):
            pc_sql = """select `%s` from `%s` where app_key='%s' and day = '%s' and `%s`='%s'""" % (
                pckey, tablename, ak, day, keyid, id)
            cur.execute(pc_sql)
            pc = cur.fetchone()[0]  # 查询数据库获取访问次数
            dickey = '''%s_%s_%s_%s''' % (ak, day, id, pc)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + bccount  # 获取字典value累加跳出次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = bccount
    # k:字典key，由ak,day,id,pc字段组成 v:字典value，跳出次数
    for (k, v) in dic.items():
        splits = k.split("_")
        if (v == 0):
            bounce_rate = 0
        else:
            bounce_rate = v / float(splits[3])  # 计算跳出率
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], v, bounce_rate)
        args.append(sql_data)
    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情跳出页个数、跳出率写入异常!', '============'


# 时详情跳出页个数、跳出率
def hll_hourdaily_bc(items, insertfields, updatefields, tablename, pckey, keyid):
    """日详情跳出页个数、跳出率，items为redis_key列表，类似于：HLL_HourLink_bc_day_hour_ak_linkkey，insertfields:插入字段,updatefields:更新字段，
        pckey:要查询数据库的条件，keyid:要查询数据库的值。"""
    sql = combinSql(insertfields, updatefields, tablename, items[0])
    args = []  # 定义批量入库集合
    dic = {}
    for ite in items:
        spt = ite.split("_")
        ak = spt[5]
        day = spt[3]
        hour = spt[4]
        id = spt[6]
        link_key = spt[7]
        hbccount = 0  # 小时跳出页个数
        for qq in r.hvals(ite):
            if qq == "1":
                hbccount += 1
        if (str(NOTDElKEYS).__contains__(link_key)):
            pc_sql = """select `%s` from `%s` where app_key='%s' and day = '%s' and hour='%s' and `%s`='%s'""" % (
                pckey, tablename, ak, day, hour, keyid, id)
            cur.execute(pc_sql)
            pc = cur.fetchone()[0]
            dickey = '''%s_%s_%s_%s_%s''' % (ak, day, hour, id, pc)
            if (dickey in dic):
                temp = int(dic.get(dickey)) + hbccount  # 获取字典value累加跳出页次数
                dic[dickey] = temp
            else:
                dic[dickey] = 0
                dic[dickey] = hbccount
    # k:字典key，由ak,day,hour,id,pc字段组成 v:字典value，跳出次数
    for (k, v) in dic.items():
        splits = k.split("_")
        if (v == 0):
            bounce_rate = 0
        else:
            bounce_rate = v / float(splits[4])  # 计算跳出率
        update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_data = (splits[0], splits[1], update_at, splits[2], splits[3], v, bounce_rate)
        args.append(sql_data)

    try:
        cur.executemany(sql, args, )  # 批量入库
    except Exception, e:
        print Exception, ':', e, '_', items[0] + '的日详情跳出页个数、跳出率写入异常!', '============'


def combinSql(insertfields, updatefields, tablename, tag):
    '''组合sql,insertfields:需插入数据库的字段 updatefields：需跟新数据库的字段  tablename：入库的表明 tag:rediskey类型 '''
    sql_insert_fields = ['app_key', 'day', 'update_at']
    sql_update_fields = []
    count = 0  # 统计insertfields字段for循环的次数，当循环的次数大于updatefields的长度时，停止往updatefields中append
    try:
        for i in range(0, len(insertfields)):

            sql_insert_fields.append(insertfields[i])
            if (count < len(updatefields)):
                sql_update_fields.append(updatefields[i])
            count += 1
        sql = prepare_sql(tablename, sql_insert_fields, sql_update_fields)
        return sql
    except Exception, e:
        print Exception, ':', e, '_', tag + '的SQL拼接异常!', '============'


# 表驱动, 指标到索引
def ald_index(argument):

    switcher = {
        1: "HLL_ZongLink_vc",    # 外链日汇总  访问人数
        2: "HLL_ZongLink_oc",    # 外链日汇总  打开次数
        3: "STR_ZongLink_tpc",   # 外链日汇总  访问次数
        4: "HLL_ZongLink_ncc",   # 外链日汇总  新增用户
        6: "HS_ZongLink_bc",     # 外链日汇总  跳出次数
        7: "HLL_Link_vc",        # 外链日详情    访问人数
        8: "HLL_Link_oc",        # 外链日详情    打开次数
        9: "STR_Link_tpc",       # 外链日详情    访问次数
        10: "HLL_Link_ncc",      # 外链日详情    新增用户
        12: "HS_Link_bc",        # 外链日详情    跳出次数
        13: "HLL_HourLink_vc",   # 外链时详情    访问人数
        14: "HLL_HourLink_oc",   # 外链时详情    打开次数
        15: "STR_HourLink_pc",   # 外链时详情    访问次数
        16: "HLL_HourLink_ncc",  # 外链时详情    新增用户
        18: "HS_HourLink_bc",    # 外链时详情    跳出次数
        19: "HLL_Media_vc",      # 媒体日详情    访问人数
        20: "HLL_Media_oc",      # 媒体日详情    打开次数
        21: "STR_Media_tpc",     # 媒体日详情    访问次数
        22: "HLL_Media_ncc",     # 媒体日详情    新增用户
        24: "HS_Media_bc",       # 媒体日详情    跳出次数
        25: "HLL_HourMedia_vc",  # 媒体时详情    访问人数
        26: "HLL_HourMedia_oc",  # 媒体时详情    打开次数
        27: "STR_HourMedia_pc",  # 媒体时详情    访问次数
        28: "HLL_HourMedia_ncc", # 媒体时详情    新增用户
        30: "HS_HourMedia_bc",   # 媒体时详情    跳出次数
        31: "HLL_Position_vc",   # 位置日详情    访问人数
        32: "HLL_Position_oc",   # 位置日详情    打开次数
        33: "STR_Position_tpc",  # 位置日详情    访问次数
        34: "HLL_Position_ncc",  # 位置日详情    新增用户
        36: "HS_Position_bc",    # 位置日详情    跳出次数
        37: "HLL_HourPosition_vc",  # 位置时详情    访问人数
        38: "HLL_HourPosition_oc",  # 位置时详情    打开次数
        39: "STR_HourPosition_pc",  # 位置时详情    访问次数
        40: "HLL_HourPosition_ncc", # 位置时详情    新增用户
        42: "HS_HourPosition_bc",   # 位置时详情    跳出次数
    }
    return switcher.get(argument, "nothing")


if __name__ == '__main__':

    #=============================================================1
    #gcs:获得当前的时间
    # 获取main方法开始时间
    ald_start_time = int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #gcs:将当前的时间化成Y-m-d H:M:S的时间的格式

    nowdate = datetime.datetime.now().strftime("%Y-%m-%d")  #gcs:获取当天的时间

    conn = db_connect()
    conn.set_character_set('utf8')
    cur = conn.cursor()  #gcs:进行MySql数据库的连接


#=============================================================2
    # 从管理表中查询未删除的link_key
    link_sql = "select link_key from ald_link_trace where is_del=0"
    cur.execute(link_sql)



    link_keys = cur.fetchall()  #gcs:获得MySql当中的数据。将Sql语句的link_sql语句的查询结果返还回来
    #=============================================================3
    #gcs:将link_keys当中的内容依次地进行遍历，将link当中的内容都添加到NOTDElKEYS当中
    NOTDElKEYS = []
    for link in link_keys:
        NOTDElKEYS.append(link)

    #=============================================================4
    #gcs:获得redis的连接
    # 连接 redis
    r = redis_connect()

    #=============================================================5
    #gcs:将redis的key取出来
    redisKeys = r.keys("*" + nowdate + "*")  # 获取今天的所有的redis的 key  nowdate当前时间


    #=============================================================6
    #gcs:这是定义了一些什么样的数组啊
    hll_zonglink_vc_list = []
    hll_zonglink_oc_list = []
    str_zonglink_tpc_list = []
    hll_zonglink_ncc_list = []
    hs_zonglink_bc_list = []
    hll_link_vc_list = []
    hll_link_oc_list = []
    str_link_tpc_list = []
    hll_link_ncc_list = []
    hs_link_bc_list = []
    hll_hourlink_vc_list = []
    hll_hourlink_oc_list = []
    str_hourlink_pc_list = []
    hll_hourlink_ncc_list = []
    hs_hourlink_bc_list = []
    hll_media_vc_list = []
    hll_media_oc_list = []
    str_media_tpc_list = []
    hll_media_ncc_list = []
    hs_media_bc_list = []
    hll_hourmedia_vc_list = []
    hll_hourmedia_oc_list = []
    str_hourmedia_pc_list = []
    hll_hourmedia_ncc_list = []
    hs_hourmedia_bc_list = []
    hll_position_vc_list = []
    hll_position_oc_list = []
    str_position_tpc_list = []
    hll_position_ncc_list = []
    hs_position_bc_list = []
    hll_hourposition_vc_list = []
    hll_hourposition_oc_list = []
    str_hourposition_pc_list = []
    hll_hourposition_ncc_list = []
    hs_hourposition_bc_list = []

    hll_zonglink_vc_insertKeys = []
    hll_zonglink_oc_insertKeys = []
    str_zonglink_tpc_insertKeys = []
    hll_zonglink_ncc_insertKeys = []
    hs_zonglink_bc_insertKeys = []
    hll_link_vc_insertKeys = ['link_key', 'link_visitor_count']
    hll_link_oc_insertKeys = ['link_key', 'link_open_count']
    str_link_tpc_insertKeys = ['link_key', 'link_page_count']
    hll_link_ncc_insertKeys = ['link_key', 'link_newer_for_app']
    hs_link_bc_insertKeys = ['link_key', 'one_page_count', 'bounce_rate']
    hll_hourlink_vc_insertKeys = ['hour', 'link_key', 'link_visitor_count']
    hll_hourlink_oc_insertKeys = ['hour', 'link_key', 'link_open_count']
    str_hourlink_pc_insertKeys = ['hour', 'link_key', 'link_page_count']
    hll_hourlink_ncc_insertKeys = ['hour', 'link_key', 'link_newer_for_app']
    hs_hourlink_bc_insertKeys = ['hour', 'link_key', 'one_page_count', 'bounce_rate']
    hll_media_vc_insertKeys = ['media_id', 'media_visitor_count']
    hll_media_oc_insertKeys = ['media_id', 'media_open_count']
    str_media_tpc_insertKeys = ['media_id', 'media_page_count']
    hll_media_ncc_insertKeys = ['media_id', 'media_newer_for_app']
    hs_media_bc_insertKeys = ['media_id', 'one_page_count', 'bounce_rate']
    hll_hourmedia_vc_insertKeys = ['hour', 'media_id', 'media_visitor_count']
    hll_hourmedia_oc_insertKeys = ['hour', 'media_id', 'media_open_count']
    str_hourmedia_pc_insertKeys = ['hour', 'media_id', 'media_page_count']
    hll_hourmedia_ncc_insertKeys = ['hour', 'media_id', 'media_newer_for_app']
    hs_hourmedia_bc_insertKeys = ['hour', 'media_id', 'one_page_count', 'bounce_rate']
    hll_position_vc_insertKeys = ['position_id', 'position_visitor_count']
    hll_position_oc_insertKeys = ['position_id', 'position_open_count']
    str_position_tpc_insertKeys = ['position_id', 'position_page_count']
    hll_position_ncc_insertKeys = ['position_id', 'position_newer_for_app']
    hs_position_bc_insertKeys = ['position_id', 'one_page_count', 'bounce_rate']
    hll_hourposition_vc_insertKeys = ['hour', 'position_id', 'position_visitor_count']
    hll_hourposition_oc_insertKeys = ['hour', 'position_id', 'position_open_count']
    str_hourposition_pc_insertKeys = ['hour', 'position_id', 'position_page_count']
    hll_hourposition_ncc_insertKeys = ['hour', 'position_id', 'position_newer_for_app']
    hs_hourposition_bc_insertKeys = ['hour', 'position_id', 'one_page_count', 'bounce_rate']

    hll_zongLink_vc_updatekeys = []
    hll_zongLink_oc_updatekeys = []
    str_zonglink_tpc_updatekeys = []
    hll_zonglink_ncc_updatekeys = []
    hs_zonglink_bc_updatekeys = []
    hll_link_vc_updatekeys = ['link_visitor_count']
    hll_link_oc_updatekeys = ['link_open_count']
    str_link_tpc_updatekeys = ['link_page_count']
    hll_link_ncc_updatekeys = ['link_newer_for_app']
    hs_link_bc_updatekeys = ['one_page_count', 'bounce_rate']
    hll_hourlink_vc_updatekeys = ['link_visitor_count', 'hour']
    hll_hourlink_oc_updatekeys = ['link_open_count', 'hour']
    str_hourlink_pc_updatekeys = ['link_page_count', 'hour']
    hll_hourlink_ncc_updatekeys = ['link_newer_for_app', 'hour']
    hs_hourlink_bc_updatekeys = ['one_page_count', 'bounce_rate', 'hour']
    hll_media_vc_updatekeys = ['media_visitor_count']
    hll_media_oc_updatekeys = ['media_open_count']
    str_media_tpc_updatekeys = ['media_page_count']
    hll_media_ncc_updatekeys = ['media_newer_for_app']
    hs_media_bc_updatekeys = ['one_page_count', 'bounce_rate']
    hll_hourmedia_vc_updatekeys = ['media_visitor_count', 'hour']
    hll_hourmedia_oc_updatekeys = ['media_open_count', 'hour']
    str_hourmedia_pc_updatekeys = ['media_page_count', 'hour']
    hll_hourmedia_ncc_updatekeys = ['media_newer_for_app', 'hour']
    hs_hourmedia_bc_updatekeys = ['one_page_count', 'bounce_rate', 'hour']
    hll_position_vc_updatekeys = ['position_visitor_count']
    hll_position_oc_updatekeys = ['position_open_count']
    str_position_tpc_updatekeys = ['position_page_count']
    hll_position_ncc_updatekeys = ['position_newer_for_app']
    hs_position_bc_updatekeys = ['one_page_count', 'bounce_rate']
    hll_hourposition_vc_updatekeys = ['position_visitor_count', 'hour']
    hll_hourposition_oc_updatekeys = ['position_open_count', 'hour']
    str_hourposition_pc_updatekeys = ['position_page_count', 'hour']
    hll_hourposition_ncc_updatekeys = ['position_newer_for_app', 'hour']
    hs_hourposition_bc_updatekeys = ['one_page_count', 'bounce_rate', 'hour']


#=============================================================7
    #gcs:定义一个匿名函数。

    # 匿名函数
    # 三元表达式s
    keyf = lambda s: "_".join(s.split("_", 3)[0:3]) # 获取redis_key关键词

    #=============================================================8
    """gcs:\n
    将redis的key按照这个匿名函数来进行分组。我不知道这个程序的作用是什么啊!!
    """

    for category, items in groupby(redisKeys, key=keyf): #按照 rediskey、key分组。之后才可以对分组之后的数据进行统计
        #gcs:这个for语句写的很有意思。category==items
        t = list(items)
        # 调到所执行的代码
        if category == ald_index(1): hll_zonglink_vc_list += t #gcs:这个ald_index是一个映射表。会将映射标号下的内容都提取出来
        if category == ald_index(2): hll_zonglink_oc_list += t
        if category == ald_index(3): str_zonglink_tpc_list += t
        if category == ald_index(4): hll_zonglink_ncc_list += t
        if category == ald_index(6): hs_zonglink_bc_list += t
        if category == ald_index(7): hll_link_vc_list += t
        if category == ald_index(8): hll_link_oc_list += t
        if category == ald_index(9): str_link_tpc_list += t
        if category == ald_index(10): hll_link_ncc_list += t
        if category == ald_index(12): hs_link_bc_list += t
        if category == ald_index(13): hll_hourlink_vc_list += t
        if category == ald_index(14): hll_hourlink_oc_list += t
        if category == ald_index(15): str_hourlink_pc_list += t
        if category == ald_index(16): hll_hourlink_ncc_list += t
        if category == ald_index(18): hs_hourlink_bc_list += t
        if category == ald_index(19): hll_media_vc_list += t
        if category == ald_index(20): hll_media_oc_list += t
        if category == ald_index(21): str_media_tpc_list += t
        if category == ald_index(22): hll_media_ncc_list += t
        if category == ald_index(24): hs_media_bc_list += t
        if category == ald_index(25): hll_hourmedia_vc_list += t
        if category == ald_index(26): hll_hourmedia_oc_list += t
        if category == ald_index(27): str_hourmedia_pc_list += t
        if category == ald_index(28): hll_hourmedia_ncc_list += t
        if category == ald_index(30): hs_hourmedia_bc_list += t
        if category == ald_index(31): hll_position_vc_list += t
        if category == ald_index(32): hll_position_oc_list += t
        if category == ald_index(33): str_position_tpc_list += t
        if category == ald_index(34): hll_position_ncc_list += t
        if category == ald_index(36): hs_position_bc_list += t
        if category == ald_index(37): hll_hourposition_vc_list += t
        if category == ald_index(38): hll_hourposition_oc_list += t
        if category == ald_index(39): str_hourposition_pc_list += t
        if category == ald_index(40): hll_hourposition_ncc_list += t
        if category == ald_index(42): hs_hourposition_bc_list += t


    #=============================================================9
    #gcs:查看hll_zonglink_vc_list 的长度是否是大于0的。如果是大于0的，就会调用相应的函数
    # 外链日汇总方法调用
    if len(hll_zonglink_vc_list) > 0: hll_daysum_link_vc(hll_zonglink_vc_list)

    #=============================================================10
    #gcs:外联汇总的计算的方法
    if len(hll_zonglink_oc_list) > 0: hll_daysum_link_oc(hll_zonglink_oc_list)
    if len(str_zonglink_tpc_list) > 0: hll_daysum_link_tpc(str_zonglink_tpc_list)
    if len(hll_zonglink_ncc_list) > 0: hll_daysum_link_ncc(hll_zonglink_ncc_list)
    if len(hs_zonglink_bc_list) > 0: hll_daysum_link_bc(hs_zonglink_bc_list)

    # 外链日详情方法调用
    if len(hll_link_vc_list) > 0: hll_daydaily_vc(hll_link_vc_list, hll_link_vc_insertKeys, hll_link_vc_updatekeys,
                                                  'aldstat_daily_link')
    if len(hll_link_oc_list) > 0: hll_daydaily_oc(hll_link_oc_list, hll_link_oc_insertKeys, hll_link_oc_updatekeys,
                                                  'aldstat_daily_link')
    if len(str_link_tpc_list) > 0: hll_daydaily_tpc(str_link_tpc_list, str_link_tpc_insertKeys, str_link_tpc_updatekeys,
                                                    'aldstat_daily_link')
    if len(hll_link_ncc_list) > 0: hll_daydaily_ncc(hll_link_ncc_list, hll_link_ncc_insertKeys, hll_link_ncc_updatekeys,
                                                    'aldstat_daily_link')
    if len(hs_link_bc_list) > 0: hll_daydaily_bc(hs_link_bc_list, hs_link_bc_insertKeys, hs_link_bc_updatekeys,
                                                 'aldstat_daily_link', 'link_page_count', 'link_key')

    # #外链时详情方法调用
    if len(hll_hourlink_vc_list) > 0: hll_hourdaily_vc(hll_hourlink_vc_list, hll_hourlink_vc_insertKeys,
                                                       hll_hourlink_vc_updatekeys, 'aldstat_hourly_link')
    if len(hll_hourlink_oc_list) > 0: hll_hourdaily_oc(hll_hourlink_oc_list, hll_hourlink_oc_insertKeys,
                                                       hll_hourlink_oc_updatekeys, 'aldstat_hourly_link')
    if len(str_hourlink_pc_list) > 0: hll_hourdaily_pc(str_hourlink_pc_list, str_hourlink_pc_insertKeys,
                                                       str_hourlink_pc_updatekeys, 'aldstat_hourly_link')
    if len(hll_hourlink_ncc_list) > 0: hll_hourdaily_ncc(hll_hourlink_ncc_list, hll_hourlink_ncc_insertKeys,
                                                         hll_hourlink_ncc_updatekeys, 'aldstat_hourly_link')
    if len(hs_hourlink_bc_list) > 0: hll_hourdaily_bc(hs_hourlink_bc_list, hs_hourlink_bc_insertKeys,
                                                      hs_hourlink_bc_updatekeys, 'aldstat_hourly_link',
                                                      'link_page_count', 'link_key')

    # #媒体日详情方法调用
    if len(hll_media_vc_list) > 0: hll_daydaily_vc(hll_media_vc_list, hll_media_vc_insertKeys, hll_media_vc_updatekeys,
                                                   'aldstat_daily_media')
    if len(hll_media_oc_list) > 0: hll_daydaily_oc(hll_media_oc_list, hll_media_oc_insertKeys, hll_media_oc_updatekeys,
                                                   'aldstat_daily_media')
    if len(str_media_tpc_list) > 0: hll_daydaily_tpc(str_media_tpc_list, str_media_tpc_insertKeys,
                                                     str_media_tpc_updatekeys, 'aldstat_daily_media')
    if len(hll_media_ncc_list) > 0: hll_daydaily_ncc(hll_media_ncc_list, hll_media_ncc_insertKeys,
                                                     hll_media_ncc_updatekeys, 'aldstat_daily_media')
    if len(hs_media_bc_list) > 0: hll_daydaily_bc(hs_media_bc_list, hs_media_bc_insertKeys, hs_media_bc_updatekeys,
                                                  'aldstat_daily_media', 'media_page_count', 'media_id')

    # #媒体时详情方法调用
    if len(hll_hourmedia_vc_list) > 0: hll_hourdaily_vc(hll_hourmedia_vc_list, hll_hourmedia_vc_insertKeys,
                                                        hll_hourmedia_vc_updatekeys, 'aldstat_hourly_media')
    if len(hll_hourmedia_oc_list) > 0: hll_hourdaily_oc(hll_hourmedia_oc_list, hll_hourmedia_oc_insertKeys,
                                                        hll_hourmedia_oc_updatekeys, 'aldstat_hourly_media')
    if len(str_hourmedia_pc_list) > 0: hll_hourdaily_pc(str_hourmedia_pc_list, str_hourmedia_pc_insertKeys,
                                                        str_hourmedia_pc_updatekeys, 'aldstat_hourly_media')
    if len(hll_hourmedia_ncc_list) > 0: hll_hourdaily_ncc(hll_hourmedia_ncc_list, hll_hourmedia_ncc_insertKeys,
                                                          hll_hourmedia_ncc_updatekeys, 'aldstat_hourly_media')
    if len(hs_hourmedia_bc_list) > 0: hll_hourdaily_bc(hs_hourmedia_bc_list, hs_hourmedia_bc_insertKeys,
                                                       hs_hourmedia_bc_updatekeys, 'aldstat_hourly_media',
                                                       'media_page_count', 'media_id')

    # # 位置日详情方法调用
    if len(hll_position_vc_list) > 0: hll_daydaily_vc(hll_position_vc_list, hll_position_vc_insertKeys,
                                                      hll_position_vc_updatekeys, 'aldstat_daily_position')
    if len(hll_position_oc_list) > 0: hll_daydaily_oc(hll_position_oc_list, hll_position_oc_insertKeys,
                                                      hll_position_oc_updatekeys, 'aldstat_daily_position')
    if len(str_position_tpc_list) > 0: hll_daydaily_tpc(str_position_tpc_list, str_position_tpc_insertKeys,
                                                        str_position_tpc_updatekeys, 'aldstat_daily_position')
    if len(hll_position_ncc_list) > 0: hll_daydaily_ncc(hll_position_ncc_list, hll_position_ncc_insertKeys,
                                                        hll_position_ncc_updatekeys, 'aldstat_daily_position')
    if len(hs_position_bc_list) > 0: hll_daydaily_bc(hs_position_bc_list, hs_position_bc_insertKeys,
                                                     hs_position_bc_updatekeys, 'aldstat_daily_position',
                                                     'position_page_count', 'position_id')

    # 位置时详情方法调用
    if len(hll_hourposition_vc_list) > 0: hll_hourdaily_vc(hll_hourposition_vc_list, hll_hourposition_vc_insertKeys,
                                                           hll_hourposition_vc_updatekeys, 'aldstat_hourly_position')
    if len(hll_hourposition_oc_list) > 0: hll_hourdaily_oc(hll_hourposition_oc_list, hll_hourposition_oc_insertKeys,
                                                           hll_hourposition_oc_updatekeys, 'aldstat_hourly_position')
    if len(str_hourposition_pc_list) > 0: hll_hourdaily_pc(str_hourposition_pc_list, str_hourposition_pc_insertKeys,
                                                           str_hourposition_pc_updatekeys, 'aldstat_hourly_position')
    if len(hll_hourposition_ncc_list) > 0: hll_hourdaily_ncc(hll_hourposition_ncc_list, hll_hourposition_ncc_insertKeys,
                                                             hll_hourposition_ncc_updatekeys, 'aldstat_hourly_position')
    if len(hs_hourposition_bc_list) > 0: hll_hourdaily_bc(hs_hourposition_bc_list, hs_hourposition_bc_insertKeys,
                                                          hs_hourposition_bc_updatekeys, 'aldstat_hourly_position',
                                                          'position_page_count', 'position_id')

    # 统计main方法运行时间
    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
    cur.close()
    conn.commit()
    conn.close()
