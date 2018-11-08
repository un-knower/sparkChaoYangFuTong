# -*- coding: utf-8 -*-
import multiprocessing

import MySQLdb
import redis
import time
import datetime
from itertools import groupby

import sys

import dbconf

"""
Create by:JetBrains PyCharm Community Edition 2016.1(64)
User:wangtaiyang
Modify:zhangzhenwei
Date:2018-01-06
Time:18:00
"""


def redis_connect():
    """连接 Redis"""
    pool = redis.ConnectionPool(host='10.0.0.156', port=6379, password='crs-ezh65tiq:aldwxredis123', db=9)
    r = redis.Redis(connection_pool=pool)
    return r


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


def hll_hourscene_nu(items, groupidlist):
    """单个场景小时新增用户计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'hour', 'scene_id', 'new_comer_count', 'update_at']  # 插入的字段
    update_fields = ['new_comer_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_scene', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'scene_group_id', 'new_comer_count', 'update_at']  # 插入的字段
    update_fields_group = ['new_comer_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_hourly_scene_group', insert_fields_group, update_fields_group)  # 准备 sql 语句

    args = []  # 定义批量入表aldstat_hourly_scene集合
    args_group = []  # 定义批量入表aldstat_hourly_scene_group集合
    dic = {}  # 定义字典用于统计场景值组新增用户数量
    try:
        for i in items:  # i:redis_key
            splits = i.split("_")
            date = splits[3]
            hour = splits[4]
            ak = splits[5]
            scene_id = splits[6]
            groupid = groupidlist.get(int(scene_id), str(11))
            hnucount = r.pfcount(i)  # 从redis中统计新增用户数量
            if (hnucount is not 'null' and hnucount is not None and hnucount is not 'None'):
                sql_data = (ak, date, hour, scene_id, hnucount, nowtime)
                args.append(sql_data)
                dickey = '''%s_%s_%s_%s''' % (ak, date, hour, groupid)  # 拼接字典key
                try:
                    dic[dickey] += ("|" + i)
                except KeyError:
                    dic[dickey] = i
            else:
                print '单个场景小时新增用户', hnucount
        # k:字典key,包含ak, date, hour, groupid。v:组新增用户数
        for (k, v) in dic.items():
            splits = k.split("_")
            st = str(v.split("|"))
            exec (
                "r.pfmerge(\"HLL_HourSceneGroup_nu_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(
                    splits[3]) + "\"," + st[
                                         1:len(
                                             st) - 1] + ")")
            r.expire("HLL_HourSceneGroup_nu_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(splits[3]),
                     86400)  # 过期时间
            value = r.pfcount(
                "HLL_HourSceneGroup_nu_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(splits[3]))

            sql_data_group = (splits[0], splits[1], splits[2], splits[3], value, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景小时新增用户入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景小时新增用户计算异常------------'


def hll_hourscene_vc(items, groupidlist):
    """单个场景小时访问用户计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'hour', 'scene_id', 'visitor_count', 'update_at']  # 插入的字段
    update_fields = ['visitor_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_scene', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'scene_group_id', 'visitor_count', 'update_at']  # 插入的字段
    update_fields_group = ['visitor_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_hourly_scene_group', insert_fields_group, update_fields_group)  # 准备 sql 语句

    args = []  # 定义批量入表aldstat_hourly_scene集合
    args_group = []  # 定义批量入表aldstat_hourly_scene_group集合
    dic = {}  # 定义字典用于统计场景值组访问用户数量
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            hour = splits[4]
            scene_id = splits[6]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[5]
            hvccount = r.pfcount(i)  # 从redis中统计访问用户数量
            if (hvccount is not 'null' and hvccount is not None and hvccount is not 'None'):
                sql_data = (ak, date, hour, scene_id, hvccount, nowtime)
                args.append(sql_data)
                dickey = '''%s_%s_%s_%s''' % (ak, date, hour, groupid)
                try:
                    dic[dickey] = ("|" + i)
                except KeyError:
                    dic[dickey] = i
            else:
                print '单个场景小时访问用户', hvccount
        # k:字典key,包含ak, date, hour, groupid。v:组访问用户数
        for (k, v) in dic.items():
            splits = k.split("_")
            st = str(v.split("|"))

            # 脚本运算
            exec (
                "r.pfmerge(\"HLL_HourSceneGroup_vc_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(
                    splits[3]) + "\"," + st[1:len(st) - 1] + ")")
            r.expire("HLL_HourSceneGroup_vc_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(splits[3]),
                     86400)  # 过期时间
            value = r.pfcount(
                "HLL_HourSceneGroup_vc_" + splits[0] + "_" + splits[1] + "_" + splits[2] + "_" + str(splits[3]))

            sql_data_group = (splits[0], splits[1], splits[2], splits[3], value, nowtime)
            args_group.append(sql_data_group)

        return (sql, args, sql_group, args_group, '单个场景小时访问用户入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景小时访问用户计算异常------------'


def hll_hourscene_oc(items, groupidlist):
    """单个场景小时打开次数计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'hour', 'scene_id', 'open_count', 'update_at']  # 插入的字段
    update_fields = ['open_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_scene', insert_fields, update_fields)  # 准备 sql 语句

    # 插入的字段
    insert_fields_group = ['app_key', 'day', 'hour', 'scene_group_id', 'open_count', 'update_at']
    update_fields_group = ['open_count']  # 需要更新的字段
    # 准备 sql 语句
    sql_group = prepare_sql('aldstat_hourly_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_hourly_scene集合
    args_group = []  # 定义批量入表aldstat_hourly_scene_group集合
    dic = {}  # 定义字典用于统计场景值组打开次数数量
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            hour = splits[4]
            scene_id = splits[6]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[5]
            hoccount = int(r.pfcount(i))  # 从redis中统计打开次数数量
            if (hoccount is not 'null' and hoccount is not None and hoccount is not 'None'):
                sql_data = (ak, date, hour, scene_id, int(hoccount), nowtime)
                args.append(sql_data)
                dickey = '''%s_%s_%s_%s''' % (ak, date, hour, groupid)
                try:
                    dic[dickey] += int(hoccount)
                except KeyError:
                    dic[dickey] = hoccount
            else:
                print '单个场景小时打开次数', hoccount
        # k:字典key,包含ak, date, hour, groupid。v:组打开次数
        for (k, v) in dic.items():
            splits = k.split("_")
            sql_data_group = (splits[0], splits[1], splits[2], splits[3], v, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景小时打开次数入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景小时打开次数计算异常------------'


def str_hourscene_pc(items, groupidlist):
    """单个场景小时新访问次数计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'hour', 'scene_id', 'page_count', 'update_at']  # 插入的字段
    update_fields = ['page_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_scene', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'scene_group_id', 'page_count', 'update_at']  # 插入的字段
    update_fields_group = ['page_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_hourly_scene_group', insert_fields_group, update_fields_group)  # 准备 sql 语句
    args = []  # 定义批量入表aldstat_hourly_scene集合
    args_group = []  # 定义批量入表aldstat_hourly_scene_group集合
    dic = {}  # 定义字典用于统计场景值组访问次数
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            hour = splits[4]
            scene_id = splits[6]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[5]
            hpccount = int(r.get(i))  # 从redis中统计访问次数
            if (hpccount is not 'null' and hpccount is not None and hpccount is not 'None'):
                sql_data = (ak, date, hour, scene_id, hpccount, nowtime)
                args.append(sql_data)
                dickey = '''%s_%s_%s_%s''' % (ak, date, hour, groupid)
                try:
                    dic[dickey] += int(hpccount)
                except KeyError:
                    dic[dickey] = hpccount
            else:
                print '单个场景小时新访问次数', hpccount
        # k:字典key,包含ak, date, hour, groupid。v:组访问次数
        for (k, v) in dic.items():
            splits = k.split("_")
            sql_data_group = (splits[0], splits[1], splits[2], splits[3], v, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景小时访问次数入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景小时新访问次数计算异常------------'


def hs_hourscene_opc(items, groupidlist):
    """单个场景小时单个场景小时跳出页计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'hour', 'scene_id', 'one_page_count', 'bounce_rate', 'update_at']  # 插入的字段
    update_fields = ['one_page_count', 'bounce_rate']  # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_scene', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'hour', 'scene_group_id', 'one_page_count', 'bounce_rate',
                           'update_at']  # 插入的字段
    update_fields_group = ['one_page_count', 'bounce_rate']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_hourly_scene_group', insert_fields_group, update_fields_group)  # 准备 sql 语句

    args = []  # 定义批量入表aldstat_hourly_scene集合
    args_group = []  # 定义批量入表aldstat_hourly_scene_group集合
    dic_opc = {}  # 定义字典用于统计场景值组跳出页数
    dic_pc = {}  # 定义字典用于统计场景值组访问次数
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            hour = splits[4]
            scene_id = splits[6]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[5]
            count_opc = 0  # 统计跳出页个数
            count_pc = 0  # 统计访问次数
            bounce_rate = 0.0
            for value in r.hvals(i):  # value：获取rediskey的value值
                if value == "1":
                    count_opc += 1
                count_pc += int(value)
                if (count_pc == 0):
                    bounce_rate = 0
                else:
                    bounce_rate = count_opc / float(count_pc)  # 计算跳出率
            sql_data = (ak, date, hour, scene_id, count_opc, bounce_rate, nowtime)
            args.append(sql_data)

            dickey = '''%s_%s_%s_%s''' % (ak, date, hour, groupid)
            # 计算场景值组的跳出页个数
            try:
                dic_opc[dickey] += count_opc
            except KeyError:
                dic_opc[dickey] = count_opc
            # 计算场景值组的访问次数
            try:
                dic_pc[dickey] += count_pc
            except KeyError:
                dic_pc[dickey] = count_pc
        # k:字典key,包含ak, date, hour, groupid。v:组跳出页个数
        for (k, v) in dic_opc.items():
            pc = dic_pc.get(k)  # 获取访问次数
            if pc is 0:
                group_bounce_rate = 0
            else:
                group_bounce_rate = v / float(pc)  # 计算跳出率
            spt = k.split("_")
            sql_data3 = (spt[0], spt[1], spt[2], spt[3], v, group_bounce_rate, nowtime)
            args_group.append(sql_data3)
        return (sql, args, sql_group, args_group, '单个场景小时单个场景小时跳出页入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景小时单个场景小时跳出页计算异常------------'


def hll_scene_nu(items, groupidlist):
    """单个场景 天 新增用户计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'scene_id', 'scene_newer_for_app', 'update_at']  # 插入的字段
    update_fields = ['scene_newer_for_app']  # 需要更新的字段
    sql = prepare_sql('aldstat_scene_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'scene_group_id', 'new_comer_count', 'update_at']  # 插入的字段
    update_fields_group = ['new_comer_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_daily_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_scene_statistics集合
    args_group = []  # 定义批量入表aldstat_daily_scene_group集合
    dic = {}
    try:
        for i in items:  # i:reids_key
            splits = i.split("_")
            date = splits[3]
            ak = splits[4]
            scene_id = splits[5]
            groupid = groupidlist.get(int(scene_id), str(11))
            nucount = int(r.pfcount(i))  # 从redis中统计新增用户数
            if (nucount is not 'null' and nucount is not None and nucount is not 'None'):
                sql_data = (ak, date, scene_id, nucount, int(time.time()))
                args.append(sql_data)
                dickey = '''%s_%s_%s''' % (ak, date, groupid)
                # 计算组新增人数
                try:
                    dic[dickey] += ("|" + i)
                except KeyError:
                    dic[dickey] = i
            else:
                print '单个场景 天 新增用户', nucount
        # k:字典key,包含ak, date, hour, groupid。v:组新增人数
        for (k, v) in dic.items():
            spt = k.split("_")
            st = str(v.split("|"))
            # 脚本计算  （场景值组 的天新增人数指标）
            exec (
                "r.pfmerge(\"HLL_SceneGroup_nu_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]) + "\"," + st[1:len(
                    st) - 1] + ")")
            r.expire("HLL_SceneGroup_nu_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]), 86400)  # 过期时间
            value = r.pfcount("HLL_SceneGroup_nu_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]))
            sql_data_group = (spt[0], spt[1], spt[2], value, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景 天 新增用户入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景 天 新增用户计算异常------------'


def hll_scene_vc(items, groupidlist):
    """单个场景 天 访问人数计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'scene_id', 'scene_visitor_count', 'update_at']  # 插入的字段
    update_fields = ['scene_visitor_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_scene_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'scene_group_id', 'visitor_count', 'update_at']  # 插入的字段
    update_fields_group = ['visitor_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_daily_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_scene_statistics集合
    args_group = []  # 定义批量入表aldstat_daily_scene_group集合
    dic = {}
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            scene_id = splits[5]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[4]
            vccount = r.pfcount(i)  # 从redis中统计访问人数
            if (vccount is not 'null' and vccount is not None and vccount is not 'None'):
                sql_data = (ak, date, scene_id, vccount, int(time.time()))
                args.append(sql_data)

                dickey = '''%s_%s_%s''' % (ak, date, groupid)
                # 计算组访问人数
                try:
                    dic[dickey] += ("|" + i)
                except KeyError:
                    dic[dickey] = i
            else:
                print '单个场景 天 访问人数', vccount
        # k:字典key,包含ak, date, hour, groupid。v:组访问人数
        for (k, v) in dic.items():
            spt = k.split("_")
            st = str(v.split("|"))
            # 脚本计算  （场景值组 的天新增人数指标）
            exec (
                "r.pfmerge(\"HLL_SceneGroup_vc_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]) + "\"," + st[1:len(
                    st) - 1] + ")")
            r.expire("HLL_SceneGroup_vc_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]), 86400)  # 过期时间
            value = r.pfcount("HLL_SceneGroup_vc_" + spt[0] + "_" + spt[1] + "_" + str(spt[2]))
            sql_data_group = (spt[0], spt[1], spt[2], value, nowtime)
            args_group.append(sql_data_group)

        return (sql, args, sql_group, args_group, '单个场景 天 访问人数入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景 天 访问人数计算异常------------'


def hll_scene_oc(items, groupidlist):
    """单个场景 天 打开次数计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'scene_id', 'scene_open_count', 'update_at']  # 插入的字段
    update_fields = ['scene_open_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_scene_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'scene_group_id', 'open_count', 'update_at']  # 插入的字段
    update_fields_group = ['open_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_daily_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_scene_statistics集合
    args_group = []  # 定义批量入表aldstat_daily_scene_group集合
    dic = {}  # 定义字典，用于计算场景值组的打开次数
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            scene_id = splits[5]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[4]
            occount = r.pfcount(i)  # 从redis中统计打开次数
            if (occount is not 'null' and occount is not None and occount is not 'None'):
                sql_data = (ak, date, scene_id, occount, int(time.time()))
                args.append(sql_data)
                dickey = '''%s_%s_%s''' % (ak, date, groupid)
                # 计算天组的打开次数
                try:
                    dic[dickey] += int(occount)
                except KeyError:
                    dic[dickey] = occount
            else:
                print '单个场景 天 打开次数', occount
        # k:字典key,由ak_date_groupid组成，v:打开次数
        for (k, v) in dic.items():
            splits = k.split("_")
            sql_data_group = (splits[0], splits[1], splits[2], v, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景 天 打开次数入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景 天 打开次数计算异常------------'


def str_scene_pc(items, groupidlist):
    """单个场景 天 访问页面计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'scene_id', 'scene_page_count', 'update_at']  # 插入的字段
    update_fields = ['scene_page_count']  # 需要更新的字段
    sql = prepare_sql('aldstat_scene_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'scene_group_id', 'page_count', 'update_at']  # 插入的字段
    update_fields_group = ['page_count']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_daily_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_scene_statistics集合
    args_group = []  # 定义批量入表aldstat_daily_scene_group集合
    dic = {}  # 定义字典，用于计算场景值组的访问次数
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            scene_id = splits[5]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[4]
            pccount = int(r.get(i))
            if (pccount is not 'null' and pccount is not None and pccount is not 'None'):
                sql_data = (ak, date, scene_id, pccount, int(time.time()))
                args.append(sql_data)
                dickey = '''%s_%s_%s''' % (ak, date, groupid)
                try:
                    dic[dickey] += int(pccount)
                except KeyError:
                    dic[dickey] = pccount
            else:
                print '单个场景 天 访问页面', pccount
        # k:字典key,由ak_date_groupid组成，v:访问次数
        for (k, v) in dic.items():
            splits = k.split("_")
            sql_data3 = (splits[0], splits[1], splits[2], v, nowtime)
            args_group.append(sql_data3)
        return (sql, args, sql_group, args_group, '单个场景 天 访问页面入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景 天 访问页面计算异常------------'


# 单个场景 天 跳出页
def hs_scene_opc(items, groupidlist):
    """单个场景 天 跳出页、跳出率计算方法，items:rediskey列表，groupidlist:场景值组列表"""
    insert_fields = ['app_key', 'day', 'scene_id', 'one_page_count', 'bounce_rate', 'update_at']  # 插入的字段
    update_fields = ['one_page_count', 'bounce_rate']  # 需要更新的字段
    sql = prepare_sql('aldstat_scene_statistics', insert_fields, update_fields)  # 准备 sql 语句

    insert_fields_group = ['app_key', 'day', 'scene_group_id', 'one_page_count', 'bounce_rate', 'update_at']  # 插入的字段
    update_fields_group = ['one_page_count', 'bounce_rate']  # 需要更新的字段
    sql_group = prepare_sql('aldstat_daily_scene_group', insert_fields_group, update_fields_group)

    args = []  # 定义批量入表aldstat_scene_statistics集合
    args_group = []  # 定义批量入表aldstat_daily_scene_group集合 跳出次数
    dic_opc = {}
    dic_pc = {}
    try:
        for i in items:
            splits = i.split("_")
            date = splits[3]
            scene_id = splits[5]
            groupid = groupidlist.get(int(scene_id), str(11))
            ak = splits[4]
            count_opc = 0
            count_pc = 0
            bounce_rate = 0.0
            for value in r.hvals(i):  # value: 获取rediskey的value
                if value == "1":
                    count_opc += 1
                count_pc += int(value)
                if (count_pc == 0):
                    bounce_rate = 0
                else:
                    bounce_rate = count_opc / float(count_pc)  # 计算跳出率
            sql_data = (ak, date, scene_id, count_opc, bounce_rate, int(time.time()))
            args.append(sql_data)

            dickey = '''%s_%s_%s''' % (ak, date, groupid)
            try:
                dic_opc[dickey] += count_opc
            except KeyError:
                dic_opc[dickey] = count_opc
            try:
                dic_pc[dickey] += count_pc
            except KeyError:
                dic_pc[dickey] = count_pc
        # k:字典key,由ak_date_groupid组成，v:跳出次数
        for (k, v) in dic_opc.items():
            spt = k.split("_")
            pc = dic_pc.get(k)
            if (pc is 0):
                group_bounce_rate = 0
            else:
                group_bounce_rate = v / float(pc)
            sql_data_group = (spt[0], spt[1], spt[2], v, group_bounce_rate, nowtime)
            args_group.append(sql_data_group)
        return (sql, args, sql_group, args_group, '单个场景 天 跳出页入库')
    except Exception, e:
        print Exception, ":", e, ' 单个场景 天 跳出页计算异常------------'


def ald_index(argument):
    """表驱动, 指标到索引"""
    switcher = {
        10: "HLL_HourScene_nu",  # 单个场景值分时 新增用户统计
        11: "HLL_HourScene_vc",  # 单个场景值分时  访问人数
        12: "HLL_HourScene_oc",  # 单个场景值分时   打开次数
        13: "STR_HourScene_pc",  # 单个场景值分时    访问次数
        14: "HS_HourScene_opc",  # 单个场景值分时    跳出页
        15: "HLL_Scene_nu",  # 单个场景值天统计   新增用户统计
        16: "HLL_Scene_vc",  # 单个场景值天统计    访问人数
        17: "HLL_Scene_oc",  # 单个场景值天统计    打开次数
        18: "STR_Scene_pc",  # 单个场景值天统计    访问次数
        19: "HS_Scene_opc",  # 单个场景值天统计    跳出页
    }
    return switcher.get(argument, "nothing")


if __name__ == '__main__':
    ald_start_time = int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nowdate = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天日期
    nowtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 获取当天时间
    ts = int(time.time())
    num_cpu = sys.argv
    if len(num_cpu) < 2:
        num_cpu.append(3)
    
    group_id = {}  # scene_id 到 scene_group_id 的映射
    ald_scene = {}  # 每个 scene_group_id 下的场景值列表

    conn = db_connect()
    conn.set_character_set('utf8')
    cur = conn.cursor()

    # 设置字节码
    cur.execute('SET NAMES utf8;')
    cur.execute('SET CHARACTER SET utf8;')
    cur.execute('SET character_set_connection=utf8;')

    sql = "select sid, scene_group_id from ald_cms_scene"
    cur.execute(sql)
    results = cur.fetchall()

    # 每个场景值对应的组
    for row in results:
        group_id[row[0]] = row[1]  # 每个场景值对应的场景值组

    keyf = lambda s: str(s[1])  # 根据场景值类型分组
    for category, items in groupby(results, key=keyf):
        scene_list = []
        for ite in list(items):
            scene_list.append(str(ite[0]))
        ald_scene[category] = scene_list

    # 连接 redis
    r = redis_connect()
    redisKeys = r.keys("*" + "Scene" + "*" + nowdate + "*")  # 获取今天的所有的redis的 key
    
    hll_hourscene_nu_list = []
    hll_hourscene_vc_list = []
    hll_hourscene_oc_list = []
    str_hourscene_pc_list = []
    hs_hourscene_opc_list = []
    hll_scene_nu_list = []
    hll_scene_vc_list = []
    hll_scene_oc_list = []
    str_scene_pc_list = []
    hs_scene_opc_list = []

    # 匿名函数   三元表达式
    keyf = lambda s: "_".join(s.split("_", 3)[0:3]) if ('_' in s) and ('=' not in s) else "_".join(s.split("=", 2)[0:2])

    for category, items in groupby(redisKeys, key=keyf):  # 按照 rediskey、key分组
        t = list(items)
        # 调到所执行的代码
        if category == ald_index(10):
            hll_hourscene_nu_list += t
        if category == ald_index(11):
            hll_hourscene_vc_list += t
        if category == ald_index(12):
            hll_hourscene_oc_list += t
        if category == ald_index(13):
            str_hourscene_pc_list += t
        if category == ald_index(14):
            hs_hourscene_opc_list += t
        if category == ald_index(15):
            hll_scene_nu_list += t
        if category == ald_index(16):
            hll_scene_vc_list += t
        if category == ald_index(17):
            hll_scene_oc_list += t
        if category == ald_index(18):
            str_scene_pc_list += t
        if category == ald_index(19):
            hs_scene_opc_list += t
    
    pool = multiprocessing.Pool(processes=int(num_cpu[1]))

    jobs = []
    if len(hll_hourscene_nu_list) > 0:
        jobs.append(pool.apply_async(hll_hourscene_nu, (hll_hourscene_nu_list, group_id,)))
    if len(hll_hourscene_vc_list) > 0:
        jobs.append(pool.apply_async(hll_hourscene_vc, (hll_hourscene_vc_list, group_id,)))
    if len(hll_hourscene_oc_list) > 0:
        jobs.append(pool.apply_async(hll_hourscene_oc, (hll_hourscene_oc_list, group_id,)))
    if len(str_hourscene_pc_list) > 0:
        jobs.append(pool.apply_async(str_hourscene_pc, (str_hourscene_pc_list, group_id,)))
    if len(hs_hourscene_opc_list) > 0:
        jobs.append(pool.apply_async(hs_hourscene_opc, (hs_hourscene_opc_list, group_id,)))

    if len(hll_scene_nu_list) > 0:
        jobs.append(pool.apply_async(hll_scene_nu, (hll_scene_nu_list, group_id,)))
    if len(hll_scene_vc_list) > 0:
        jobs.append(pool.apply_async(hll_scene_vc, (hll_scene_vc_list, group_id,)))
    if len(hll_scene_oc_list) > 0:
        jobs.append(pool.apply_async(hll_scene_oc, (hll_scene_oc_list, group_id,)))
    if len(str_scene_pc_list) > 0:
        jobs.append(pool.apply_async(str_scene_pc, (str_scene_pc_list, group_id,)))
    if len(hs_scene_opc_list) > 0:
        jobs.append(pool.apply_async(hs_scene_opc, (hs_scene_opc_list, group_id,)))

    pool.close()
    pool.join()
    for job in jobs:
        try:
            return_result = job.get()
            for i in range(0, (len(return_result) - 1) / 2):
                try:
                    print return_result[len(return_result) - 1], len(return_result[i * 2 + 1])
                    cur.executemany(return_result[i * 2], return_result[i * 2 + 1], )  # 批量入库
                except Exception, e:
                    print Exception, ':', e, '_', return_result[len(return_result) - 1], '异常', '============'
        except TypeError:
            print return_result
    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
