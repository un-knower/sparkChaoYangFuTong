# -*- coding: utf-8 -*-
import MySQLdb
import redis
import time
import datetime
import dbconf
from itertools import groupby
from collections import defaultdict

"""
    Created by sunxiaowei on 2017-10-21
"""



def redis_connect():
    """ 连接 Redis """
    pool = redis.ConnectionPool(host='10.0.0.156', port=6379, password='crs-ezh65tiq:aldwxredis123', db=9)
    r = redis.Redis(connection_pool=pool)
    return r


def prepare_sql(table, insert_fields, update_fields):
    """
        准备 sql 语句模版, 返回一个 sql 语句:
        insert into table_name (...) values ('xxx', 'xxx', 'xxx') ON DUPLICATE KEY UPDATE ...
    """
    on_duplicate_key = []
    placeholder = []

    [on_duplicate_key.append(item + '=' + 'VALUES(' + item + ')') for item in update_fields]
    [placeholder.append('%s') for item in insert_fields]

    insert_columns = '(' + ','.join(insert_fields) + ')'  # 准备要插入的字段
    values = 'values(' + ','.join(placeholder) + ')'  # 准备 values 值
    on_duplicate_key_update = ','.join(on_duplicate_key)  # 准备需要更新的字段 

    # 将各部分拼接为字符串 insert into table (...) values(...) ON DUPLICATE KEY UPDATE ... 并返回
    return "insert into %s %s %s ON DUPLICATE KEY UPDATE %s" % (table, insert_columns, values, on_duplicate_key_update)


def db_connect():
    """ 数据库连接 """
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port),
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn


def hll_qr_sc(items, qr_dict, default_qr_group):
    """ 单个二维码每天的扫码次数 """
    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间

    insert_fields = ['app_key', 'day', 'total_scan_count', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['total_scan_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_qr_code_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'qr_key', 'qr_group_key', 'qr_scan_count', 'redis_key']  # 插入的字段
    update_fields_group = ['qr_scan_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_qr_between', insert_fields_group, update_fields_group)  # 准备 sql 语句

    insert_qr_group = ['app_key', 'day', 'qr_group_key', 'qr_scan_count', 'update_at']  # 插入的字段
    update_qr_group = ['qr_scan_count']  # 需要更新的字段
    qr_sql_group = prepare_sql('aldstat_daily_qr_group', insert_qr_group, update_qr_group)  # 准备 sql 语句

    args = []
    args_group = []
    for item in items:
        # item 格式为: HLL_HourQr_ncg_2017-11-20_16_b2a13b2134f3ef03dc8556d678ad0f0a_
        # 01b318c8aa87304e5e30fa9fcec40ebd
        (ak, qr_key) = item.strip().split('_')[4:6]  # 获取 app_key 和 qr_key
        qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
        count = r.pfcount(item)  # 单个二维码每天的扫码次数
        sql_data = (ak, day, count, qr_key, update_at)  # 单个二维码 sql 数据
        sql_data_group = (ak, day, qr_key, qr_group_key, count, item)  # 二维码组 sql 数据
        args.append(sql_data)  # 将 sql 数据追加到 args 列表
        args_group.append(sql_data_group)  # 将 二维码组数据追加到 args_group 列表

    try:
        cur.executemany(sql, args)  # 二维码统计结果批量入库
        cur.executemany(sql_group, args_group)  # 将redis的key批量添加到对应的中间表里
    except Exception, e:
        print Exception, ':', e, 'aldstat_qr_code_statistics (组)扫码次数写入异常!'

    # 二维码组扫码次数入库
    qr_scan_count_sql = '''select app_key, qr_group_key, sum(qr_scan_count) from aldstat_qr_between
                        where qr_key !='' and day='%s' group by app_key, qr_group_key''' % day
    cur.execute(qr_scan_count_sql)
    qr_scan_count_sql_result = cur.fetchall()

    my_args = []
    for row in qr_scan_count_sql_result:
        sql_data = (row[0], day, row[1], row[2], day_hh_mm_ss)
        my_args.append(sql_data)
    try:
        cur.executemany(qr_sql_group, my_args)
    except Exception, e:
        print Exception, ':', e, 'aldstat_daily_qr_group 表扫码次数写入异常!'


def hll_hourqr_sc(items, qr_dict, default_qr_group):
    ''' 单个二维码每小时的扫码次数 '''
    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间
    insert_fields = ['app_key', 'day', 'hour', 'qr_scan_count', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['qr_scan_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_qr', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'qr_key', 'qr_group_key', 'qr_scan_count',
                           'redis_key']  # 插入的字段
    update_fields_group = ['qr_scan_count']  # 需要更新的字段
    # 准备 sql 语句
    sql_group = prepare_sql('aldstat_hour_qr_between', insert_fields_group, update_fields_group)
    # 插入的字段
    insert_qr_group = ['app_key', 'day', 'hour', 'qr_group_key', 'qr_scan_count', 'update_at']
    update_qr_group = ['qr_scan_count']  # 需要更新的字段
    # 准备 sql 语句
    qr_sql_group = prepare_sql('aldstat_hourly_qr_group', insert_qr_group, update_qr_group)

    # HLL_HourQr_sc_2017-10-18_10_64a7f2b033fb593a8598bbc48c3b8486_3eb665f1960ef372f86e07632098d92d0
    # 单个二维码每小时扫码次数入库
    args = []
    args_group = []
    for item in items:
        (hour, ak, qr_key) = item.strip().split('_')[4:7]  # 获取 hour, app_key 和 qr_key
        qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
        count = r.pfcount(item)  # 单个二维码每小时的扫码次数
        sql_data = (ak, day, hour, count, qr_key, day_hh_mm_ss)  # 单个二维码 sql 数据
        sql_data_group = (ak, day, hour, qr_key, qr_group_key, count, item)  # 二维码组 sql 数据
        args.append(sql_data)  # 将 sql 数据追加到 args 列表
        args_group.append(sql_data_group)  # 将二维码组 sql 追加到 args_group 列表

    try:
        cur.executemany(sql, args)  # 单个二维码每小时的扫码次数的批量入库
        cur.executemany(sql_group, args_group)  # 将redis的key添加到对应的中间表里
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr (组)扫码次数写入异常!', args, '============'

    # 二维码组每小时扫码次数入库
    qr_scan_count_sql = '''select app_key, hour, qr_group_key, sum(qr_scan_count) from
                        aldstat_hour_qr_between where  qr_key !='' and day='%s' group by app_key, hour,
                        qr_group_key''' % day
    cur.execute(qr_scan_count_sql)
    qr_scan_count_sql_result = cur.fetchall()

    my_args = []
    for row in qr_scan_count_sql_result:
        # 将 app_key, day, hour, qr_group_key, sum(qr_scan_count), 更新时间作为 sql_data
        sql_data = (row[0], day, row[1], row[2], row[3], day_hh_mm_ss)
        my_args.append(sql_data)
    try:
        cur.executemany(qr_sql_group, my_args)  # 二维码组每小时扫码次数的批量入库
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr_group 表扫码次数写入异常!'


def hll_qr_vc(items, qr_dict, default_qr_group):
    """ 单个二维码每天的扫码人数 """

    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间

    # 二维码日表
    insert_fields = ['app_key', 'day', 'total_scan_user_count', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['total_scan_user_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_qr_code_statistics', insert_fields, update_fields)  # 准备 sql 语句

    # 二维码中间表
    # 插入的字段
    insert_fields_group = ['app_key', 'day', 'qr_key', 'qr_group_key', 'qr_visitor_count', 'redis_key']
    update_fields_group = ['qr_visitor_count']  # 需要更新的字段
    # 准备 sql 语句
    sql_group = prepare_sql('aldstat_qr_between', insert_fields_group, update_fields_group)

    # 二维码组结果表
    insert_qr_group = ['app_key', 'day', 'qr_group_key', 'qr_visitor_count', 'update_at']  # 插入的字段
    update_qr_group = ['qr_visitor_count']  # 需要更新的字段
    qr_sql_group = prepare_sql('aldstat_daily_qr_group', insert_qr_group, update_qr_group)  # 准备 sql 语句

    args = []
    args_group = []

    for item in items:
        if item.count('_') == 5:
            (ak, qr_key) = item.strip().split('_')[4:6]  # 获取 app_key 和 qr_key
            qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
            count = r.pfcount(item)  # 单个二维码每天的扫码人数
            sql_data = (ak, day, count, qr_key, update_at)  # 单个二维码 sql 数据
            sql_data_group = (ak, day, qr_key, qr_group_key, count, item)  # 二维码组 sql 数据
            if default_qr_group.get(ak): args.append(sql_data)  # qr_key 不为空的时候才写入
            args_group.append(sql_data_group)  # 二维码组 sql 数据追加到 args_group 列表
    try:
        cur.executemany(sql, args)  # 单个二维码每天的扫码人数的批量入库
        cur.executemany(sql_group, args_group)  # 将redis的key添加到对应的中间表里
    except Exception, e:
        print Exception, ':', e, 'aldstat_qr_code_statistics (组)扫码人数写入异常!', args_group

    # 获取redis的key  （计算天指标 二维码组）
    qr_visitor_sql = ''' select app_key, qr_key, qr_group_key, redis_key from  aldstat_qr_between
                     where qr_key !='' and day = '%s' ''' % day
    cur.execute(qr_visitor_sql)
    qr_visitor_result = cur.fetchall()

    qr_key_list = []
    for row in qr_visitor_result:
        # 如果 redis key 中包含 HLL_Qr_vc_ 字符串
        if 'HLL_Qr_vc_' in row[3]:
            # 则为二维码组分配一个键 HLL_QrGroup_vc
            dest="""HLL_QrGroup_vc_%s_%s_%s|%s"""%(day,row[0],row[2],row[3])
            qr_key_list.append(dest)  # 并把这种类别的 redis key 追加到 qr_key_list 列表

    qr_group_args = []  # 保存二维码组结果表的数据

    # 分组函数, 根据 `HLL_QrGroup_vc_`day`_`app_key`_`qr_group_key` 分组
    keyf = lambda s: s.split('|')[0]
    for category, a_items in groupby(qr_key_list, key=keyf):
        # print category # HLL_QrGroup_vc_2017-10-17_64a7f2b033fb593a8598bbc48c3b8486_
        # c332a13dfebb72318a260634cf4f7ec5
        # 获取每个分组下面的所有 redis key 的列表
        category_items = []
        for row in list(a_items):
            # 将同一个二维码组的 redis key 追加到 category_items 列表
            category_items.append(row.split('|')[1])

        st = str(category_items)  # 将这个列表转为字符串
        # 准备 pfmerge 函数需要的 dest
        exec_string = """r.pfmerge(\"%s",%s)""" % (category, st[1:len(st) - 1])
        # r.pfmerge("HLL_QrGroup_vc_2017-10-17_64a7f2b033fb593a8598bbc48c3b8486_
        # c332a13dfebb72318a260634cf4f7ec5",
        # u'HLL_QrGroup_vc_2017-10-17_64a7f2b033fb593a8598bbc48c3b8486_3eb665f1960ef372f86e07632098d92d')
        r.expire(category, 86400 * 1)  # 设置 redis key 的过期时间

        exec (exec_string)  # 执行 pfmerge
        value = r.pfcount(category)  # 获取值

        # 二维码组的扫码人数批量入库
        (ak, qr_group_key) = category.split('_')[4:6]  # 获取 ak, qr_group_key
        sql_qr_group_data = (ak, day, qr_group_key, value, day_hh_mm_ss)  # 二维码组扫码人数的 sql 数据
        qr_group_args.append(sql_qr_group_data)

    try:
        cur.executemany(qr_sql_group, qr_group_args)  # 将二维码组的扫码人数写入数据库
    except Exception, e:
        print Exception, ':', e, 'aldstat_daily_qr_group (组)扫码人数写入异常!'


def hll_hourqr_vc(items, qr_dict, default_qr_group):
    """ 单个二维码每小时的扫码人数 """

    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间
    insert_fields = ['app_key', 'day', 'hour', 'qr_visitor_count', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['qr_visitor_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_qr', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'qr_key', 'qr_group_key', 'qr_visitor_count',
                           'redis_key']  # 插入的字段
    update_fields_group = ['qr_visitor_count']  # 需要更新的字段
    # 准备 sql 语句
    sql_group = prepare_sql('aldstat_hour_qr_between', insert_fields_group, update_fields_group)
    # 插入的字段
    insert_qr_group = ['app_key', 'day', 'hour', 'qr_group_key', 'qr_visitor_count', 'update_at']
    update_qr_group = ['qr_visitor_count']  # 需要更新的字段
    # 准备 sql 语句
    qr_sql_group = prepare_sql('aldstat_hourly_qr_group', insert_qr_group, update_qr_group)

    args = []
    args_group = []

    for item in items:
        (hour, ak, qr_key) = item.strip().split('_')[4:7]  # 获取 hour, app_key 和 qr_key
        qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
        count = r.pfcount(item)  # 单个二维码每小时的扫码人数
        sql_data = (ak, day, hour, count, qr_key, day_hh_mm_ss)  # 单个二维码每小时的 sql 数据
        sql_data_group = (ak, day, hour, qr_key, qr_group_key, count, item)  # 二维码组每小时的 sql 数据
        args.append(sql_data)
        args_group.append(sql_data_group)
    try:
        cur.executemany(sql, args)  # 单个二维码每小时的扫码人数的批量入库
        cur.executemany(sql_group, args_group)  # 将redis的key添加到对应的中间表里
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr (组)扫码人数写入异常!', args, "<<<<<<<<<<<<<"

    # 获取redis的key  （计算小时指标 二维码组）
    # HLL_HourQr_vc_2017-10-17_19_88ce58bd7191f478f1cf382407a29563_263
    qr_visitor_sql = ''' select app_key, hour, qr_key, qr_group_key, redis_key from
                     aldstat_hour_qr_between where  qr_key !='' and day = '%s' ''' % day
    cur.execute(qr_visitor_sql)
    qr_visitor_result = cur.fetchall()

    qr_key_list = []
    for row in qr_visitor_result:
        # 如果 redis key 中包含字符串 HLL_HourQr_vc_
        if 'HLL_HourQr_vc_' in row[4]:
            # 则为二维码组添加一个组 HLL_HourQrGroup_vc 的类别
            dest="""HLL_QrGroup_vc_%s_%s_%s_%s|%s"""%(day,row[1],row[0],row[3],row[4])
             # HLL_HourQrGroup_vc_2017-10-17_19_appkey_qrgroupkey
            qr_key_list.append(dest)

    qr_group_args = []  # 保存二维码组结果表的数据

    # 分组函数, 根据 HLL_HourQrGroup_vc + day + hour + app_key + qr_group_key 分组
    keyf = lambda s: s.split('|')[0]
    for category, a_items in groupby(qr_key_list, key=keyf):
        # print category # HLL_HourQrGroup_vc_2017-10-17_19_appkey_qrgroupkey
        # 获取每个分组下面的所有 redis key 的列表
        category_items = []
        for row in list(a_items):
            # 将同一个二维码组的 redis key 追加到 category_items 列表
            category_items.append(row.split('|')[1])

        st = str(category_items)  # 将这个列表转为字符串
        # 准备 pfmerge 函数需要的 dest
        exec_string = """r.pfmerge(\"%s",%s)""" % (category, st[1:len(st) - 1])
        # r.pfmerge("HLL_HourQrGroup_vc_2017-10-17_13_64a7f2b033fb593a8598bbc48c3b8486_
        # c332a13dfebb72318a260634cf4f7ec5",u'HLL_HourQrGroup_vc_2017-10-17_14_
        # 64a7f2b033fb593a8598bbc48c3b8486_3eb665f1960ef372f86e07632098d92d')
        r.expire(category, 86400 * 1)  # 设置 redis key 的过期时间

        exec (exec_string)  # 执行 pfmerge
        value = r.pfcount(category)  # 获取值

        # 二维码组的扫码人数批量入库
        (hour, ak, qr_group_key) = category.split('_')[4:7]  # 获取 hour, ak, qr_group_key
        sql_qr_group_data = (ak, day, hour, qr_group_key, value, day_hh_mm_ss) # 二维码组扫码人数的 sql 数据
        qr_group_args.append(sql_qr_group_data)

    try:
        cur.executemany(qr_sql_group, qr_group_args)  # 将二维码组的扫码人数写入数据库
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr_group (组)扫码人数写入异常!'


def hll_qr_nc(items, qr_dict, default_qr_group):
    """ 单个二维码每天扫码带来新增 """

    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间

    # 二维码日表
    insert_fields = ['app_key', 'day', 'qr_new_comer_for_app', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['qr_new_comer_for_app', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_qr_code_statistics', insert_fields, update_fields)  # 准备 sql 语句

    # 二维码组结果表
    insert_qr_group = ['app_key', 'day', 'qr_group_key', 'qr_newer_count', 'update_at']  # 插入的字段
    update_qr_group = ['qr_newer_count']  # 需要更新的字段
    # 准备 sql 语句
    qr_sql_group = prepare_sql('aldstat_daily_qr_group', insert_qr_group, update_qr_group)

    args = []
    args_group = []

    # 某小程序下每个二维码对应的二维码组
    qr_group_new_dict = defaultdict(dict)

    for item in items:
        (ak, qr_key) = item.strip().split('_')[4:6]  # 获取 app_key 和 qr_key
        qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
        count = r.pfcount(item)  # 单个二维码每天的扫码带来新增

        # 某小程序下某二维码组的扫码带来新增, 键为ak和qr_group_key, 键值每次增加 count
        qr_group_new_dict[ak][qr_group_key] = qr_group_new_dict[ak].get(qr_group_key, 0) + count # 二维码组新增
        sql_data = (ak, day, count, qr_key, update_at)  # 单个二维码 sql 数据
        args.append(sql_data)
    try:
        cur.executemany(sql, args)
    except Exception, e:
        print Exception, ':', e, 'aldstat_qr_code_statistics (组)扫码带来新增写入异常!'

    args_group_daily = []

    # 遍历 qr_group_new_dict 字典, 
    for app_key in qr_group_new_dict:
        for qr_group_key in qr_group_new_dict[app_key]:
            # 获取字典中的 app_key, qr_group_key, 和二维码组的扫码带来新增
            group_data = (app_key, day, qr_group_key, qr_group_new_dict[app_key][qr_group_key],
                          day_hh_mm_ss)
            args_group_daily.append(group_data)
    try:
        cur.executemany(qr_sql_group, args_group_daily)  # 二维码组每日带来新增
    except Exception, e:
        print Exception, ':', e, 'aldstat_daily_qr_group (组)扫码带来新增写入异常!', args_group_daily


def hll_hourqr_nc(items, qr_dict, default_qr_group):
    """ 单个二维码每小时的扫码带来新增 """

    day = str(datetime.datetime.now().strftime("%Y-%m-%d"))  # 今日时间
    day_hh_mm_ss = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 今日时间（带小时）
    update_at = int(time.time())  # 更新时间
    insert_fields = ['app_key', 'day', 'hour', 'qr_newer_count', 'qr_key', 'update_at']  # 插入的字段
    update_fields = ['qr_newer_count', 'update_at']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_qr', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'qr_key', 'qr_group_key', 'qr_newer_count',
                           'redis_key']  # 插入的字段
    update_fields_group = ['qr_newer_count']  # 需要更新的字段
    # 准备 sql 语句
    sql_group = prepare_sql('aldstat_hour_qr_between', insert_fields_group, update_fields_group)
    # 插入的字段
    insert_qr_group = ['app_key', 'day', 'hour', 'qr_group_key', 'qr_newer_count', 'update_at']
    update_qr_group = ['qr_newer_count']  # 需要更新的字段
    # 准备 sql 语句
    qr_sql_group = prepare_sql('aldstat_hourly_qr_group', insert_qr_group, update_qr_group)

    args = []
    args_group = []

    for item in items:
        (hour, ak, qr_key) = item.strip().split('_')[4:7]  # 获取 hour, app_key 和 qr_key
        qr_group_key = qr_dict[ak].get(qr_key, default_qr_group.get(ak, ''))  # 获取二维码组的 key
        count = r.pfcount(item)  # 单个二维码每小时的扫码带来新增
        sql_data = (ak, day, hour, count, qr_key, day_hh_mm_ss)  # 单个二维码每小时的 sql 数据
        sql_data_group = (ak, day, hour, qr_key, qr_group_key, count, item)  # 二维码组每小时的 sql 数据
        args.append(sql_data)
        args_group.append(sql_data_group)
    try:
        cur.executemany(sql, args)  # 单个二维码每小时的扫码带来新增批量入库
        cur.executemany(sql_group, args_group)  # 将redis的key添加到对应的中间表里
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr (组)扫码带来新增写入异常!', args, '-------'

    # 获取redis的key  （计算每小时的指标 二维码组）
    qr_visitor_sql = ''' select app_key, hour, qr_key, qr_group_key, redis_key from
                     aldstat_hour_qr_between where qr_key !='' and day = '%s' ''' % day
    cur.execute(qr_visitor_sql)
    qr_visitor_result = cur.fetchall()

    qr_key_list = []
    for row in qr_visitor_result:
        # 如果 redis key 中包含字符串 HLL_HourQr_nc_
        if 'HLL_HourQr_nc_' in row[4]:
            # 则为二维码组添加一个组 HLL_HourQrGroup_nc_ 的类别
            dest="""HLL_QrGroup_vc_%s_%s_%s_%s|%s"""%(day,row[1],row[0],row[3],row[4])
            # HLL_HourQrGroup_nc_2017-10-17_19_appkey_qrgroupkey
            qr_key_list.append(dest)

    qr_group_args = []  # 保存二维码组结果表的数据
    keyf = lambda s: s.split('|')[0]  # 分组函数, 根据 HLL_HourQrGroup_vc + day + app_key + qr_group_key 分组
    for category, a_items in groupby(qr_key_list, key=keyf):
        # print category # HLL_HourQrGroup_nc_2017-10-17_15_64a7f2b033fb593a8598bbc48c3b8486_
        # c332a13dfebb72318a260634cf4f7ec5
        # 获取每个分组下面的所有 redis key 的列表
        category_items = []
        for row in list(a_items):
            # 将同一个二维码组的 redis key 追加到 category_items 列表
            category_items.append(row.split('|')[1])

        # 将这个列表转为字符串
        st = str(category_items)

        # 准备 pfmerge 函数需要的 dest
        exec_string = """r.pfmerge(\"%s",%s)""" % (category, st[1:len(st) - 1])
        # r.pfmerge("HLL_HourQrGroup_nc_2017-10-17_64a7f2b033fb593a8598bbc48c3b8486_
        # c332a13dfebb72318a260634cf4f7ec5",u'HLL_HourQrGroup_vc_2017-10-17_
        # 64a7f2b033fb593a8598bbc48c3b8486_3eb665f1960ef372f86e07632098d92d')
        r.expire(category, 86400 * 1)  # 设置 redis key 的过期时间

        exec (exec_string)  # 执行 pfmerge
        value = r.pfcount(category)  # 获取值

        # 二维码组的扫码人数批量入库
        (hour, ak, qr_group_key) = category.split('_')[4:7]  # 获取 ak, qr_group_key
        sql_qr_group_data = (ak, day, hour, qr_group_key, value, day_hh_mm_ss) # 二维码组扫码人数的 sql 数据
        qr_group_args.append(sql_qr_group_data)

    try:
        cur.executemany(qr_sql_group, qr_group_args)  # 将二维码组的扫码人数写入数据库
    except Exception, e:
        print Exception, ':', e, 'aldstat_hourly_qr_group (组)扫码带来新增写入异常!'


def ald_index(argument):
    """ 表驱动, 指标到索引的映射, 方便查找 """
    switcher = {
        1: "HLL_HourQr_sc",  # 单个二维码每小时的扫码次数
        2: "HLL_HourQr_vc",  # 单个二维码每小时的扫码人数
        3: "HLL_HourQr_nc",  # 单个二维码每小时的新用户人数
        4: "HLL_Qr_sc",  # 单个二维码每天的扫码次数
        5: "HLL_Qr_vc",  # 单个二维码每天的扫码人数
        6: "HLL_Qr_nc",  # 单个二维码每天的新用户人数
        7: "HLL_ZongQr_sc",  # 总的二维码扫码次数
        8: "ST_ZongQr_vc",  # 总的二维码扫码人数
        9: "HLL_ZongQr_nc",  # 总的二维码扫码新增
    }
    return switcher.get(argument, "nothing")


if __name__ == '__main__':
    """ 程序入口, 调用各函数完成计算 """
    ald_start_time = int(time.time())  # 程序开始运行时间
    # 程序开始运行时间, YYMMDD HHMMSS 格式
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    nowdate = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天时间
    ts = int(time.time())

    # 获取数据库连接
    conn = db_connect()
    # 获取数据库游标
    cur = conn.cursor()

    # 获取每个小程序下每个二维码所属的组
    qr_sql = "select app_key,qr_key, qr_group_key from ald_code"
    cur.execute(qr_sql)

    # app_key, qr_key, qr_group_key 的列表
    qr_results = cur.fetchall()

    # 某小程序下每个二维码对应的二维码组
    qr_dict = defaultdict(dict)
    for row in qr_results:
        qr_dict[row[0]][row[1]] = row[2]

    # 每个小程序对应的默认二维码组的 key
    qr_group_sql = "select app_key, qr_group_key from ald_qr_group where qr_group_name = '默认组'"
    cur.execute(qr_group_sql)
    qr_group_results = cur.fetchall()  # 默认组 app_key, qr_group_key

    # 某小程序对应对默认二维码组的 key
    default_qr_group = defaultdict(dict)
    for row in qr_group_results:
        default_qr_group[row[0]] = row[1]

    r = redis_connect()  # 连接 redis
    rediskeys = r.keys("*" + "Qr" + "*" + nowdate + "*")  # 获取 redis 中所有的二维码 key

    # 定义一组空数组, 把各个类别下的 redis_key 都放在该大列表中
    hll_hourqr_sc_list = []
    hll_hourqr_vc_list = []
    hll_hourqr_nc_list = []
    hll_qr_sc_list = []
    hll_qr_vc_list = []
    hll_qr_nc_list = []

    # 分组函数, 根据前 3 个下划线组成的字符串将 redis key 分成不同组
    keyf = lambda s: "_".join(s.split("_", 3)[0:3])
    for category, items in groupby(rediskeys, key=keyf):
        redis_key_list = list(items)  # 二维码 redis key 的列表

        # 根据二维码 redis key 的类别, 将 redis key 归类到不同的列表中
        if category == ald_index(1): hll_hourqr_sc_list += redis_key_list
        if category == ald_index(2): hll_hourqr_vc_list += redis_key_list
        if category == ald_index(3): hll_hourqr_nc_list += redis_key_list
        if category == ald_index(4): hll_qr_sc_list += redis_key_list
        if category == ald_index(5): hll_qr_vc_list += redis_key_list
        if category == ald_index(6): hll_qr_nc_list += redis_key_list

    # 如果 redis key 列表的长度大于 0, 则执行对应的计算函数
    if len(hll_hourqr_sc_list) > 0: hll_hourqr_sc(hll_hourqr_sc_list, qr_dict, default_qr_group)
    if len(hll_hourqr_vc_list) > 0: hll_hourqr_vc(hll_hourqr_vc_list, qr_dict, default_qr_group)
    if len(hll_hourqr_nc_list) > 0: hll_hourqr_nc(hll_hourqr_nc_list, qr_dict, default_qr_group)
    if len(hll_qr_sc_list) > 0: hll_qr_sc(hll_qr_sc_list, qr_dict, default_qr_group)
    if len(hll_qr_vc_list) > 0: hll_qr_vc(hll_qr_vc_list, qr_dict, default_qr_group)
    if len(hll_qr_nc_list) > 0: hll_qr_nc(hll_qr_nc_list, qr_dict, default_qr_group)

    cur.close()  # 关闭数据库游标
    conn.commit()  # 提交 sql
    conn.close()  # 关闭数据库连接

    ald_end_time = int(time.time())  # 程序运行结束时间

    # 程序总耗费时长
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
