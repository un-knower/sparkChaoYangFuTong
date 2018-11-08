# -*- coding: utf-8 -*-
import dbconf
import MySQLdb
import datetime,time
import redis
from itertools import groupby

"""
描述脚本功能：
这个python脚本主要是处理所有关于分享指标从redis获取数据入mysql的
主要就是不同指标的不同功能分别入库
所以根据不同的表分了不同的函数
将redis key放入列表中和指标字段传入函数
虽然指标和表名不一样，但是大致相同
所以第一个入库函数详细讲解
其余大致介绍，
如有疑问可以参考第一个函数注释
"""

# redis的连接
def redis_connect():
    ''' 连接 Redis '''
    pool = redis.ConnectionPool(host='10.0.0.156', port=6379, password='crs-ezh65tiq:aldwxredis123', db=9)
    r = redis.Redis(connection_pool=pool)
    return r

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

# 每个小时的分享公共类
def STR_HourShare_c(tmp, column):
    # 拼接每小时入库sql的语句
    sql2 = '''insert into aldstat_hourly_share_summary (`day`,`hour`,`app_key`,`''' + column + '''`,`update_at`)
    values (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE ''' + column + '''=VALUES(''' + column + '''),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    # 循环所有的redis_key，
    for iii in tmp:
        # 将redis key进行拆分
        row = iii.split("~")
        # 获取日期
        date_row = row[3].split("_")[0]
        # 获取小时
        hour_row = row[3].split("_")[1]
        count = 0
        # 判断是否为分享次数，如果是，使用get，如果不是，使用pfcount
        if column == "share_count":
            count = r.get(iii)
        else:
            count = r.pfcount(iii)
        #     将所有信息拼接成一个
        sql_data = ([date_row, hour_row, row[4], count, update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 每天的分享公共类
def STR_DayShare_c(tmp, column):
    # 拼接每天的分享入库sql
    sql2 = '''insert into aldstat_daily_share_summary (`day`,`app_key`,`''' + column + '''`,`update_at`)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE ''' + column + '''=VALUES(''' + column + '''),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    # 循环所有redis key 进行处理
    for iii in tmp:
        row = iii.split("~")
        count = 0
        if column == "share_count":
            count = r.get(iii)
        else:
            count = r.pfcount(iii)
        sql_data = ([row[3], row[4], count, update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 每天页面的分享公共类
def STR_PageShare_c(tmp, column):
    # 拼接页面分享的入库sql
    sql2 = '''insert into aldstat_dailyshare_page (`day`,`app_key`,`page_uri`,`''' + column + '''`,`update_at`)
    values (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE ''' + column + '''=VALUES(''' + column + '''),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    # 循环所有的redis key进行拆分处理
    for iii in tmp:
        row = iii.split("~")
        count = 0
        if column == "share_count":
            count = r.get(iii)
        else:
            count = r.pfcount(iii)
        sql_data = ([row[3], row[4], row[5], count, update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 回流比，分享打开次数/分享次数
def hour_reflux_ratio(tmp_date):
    sql = "select `day`,`hour`,app_key,share_open_count/share_count FROM aldstat_hourly_share_summary where day =" + "'" + tmp_date + "'" + " GROUP BY app_key,`day`,`hour`"
    cur.execute(sql)
    results = cur.fetchall()
    sql2 = '''insert into aldstat_hourly_share_summary (`day`,`hour`,`app_key`,`share_reflux_ratio`,`update_at`)
    values (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE share_reflux_ratio=VALUES(share_reflux_ratio),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


# 每天的分享回流比
def day_reflux_ratio(tmp_date):
    sql = "select `day`,`app_key`,share_open_count/share_count FROM aldstat_daily_share_summary where day =" + "'" + tmp_date + "'" + " GROUP BY app_key,`day`"
    cur.execute(sql)
    results = cur.fetchall()
    sql2 = '''insert into aldstat_daily_share_summary (`day`,`app_key`,`share_reflux_ratio`,`update_at`)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE share_reflux_ratio=VALUES(share_reflux_ratio),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], row[1], row[2], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 每天的页面分享回流比
def page_reflux_ratio(tmp_date):
    sql = "select `day`,`app_key`,`page_uri`,share_open_count/share_count FROM aldstat_dailyshare_page where day =" + "'" + tmp_date + "'" + " GROUP BY app_key,`day`,`page_uri`"
    cur.execute(sql)
    results = cur.fetchall()
    sql2 = '''insert into aldstat_dailyshare_page (`day`,`app_key`,`page_uri`,`share_reflux_ratio`,`update_at`)
    values (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE share_reflux_ratio=VALUES(share_reflux_ratio),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3], update_at])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
        # 关闭数据库连接
# 分享层级的所有指标
def hierarchy_share_all(tmp, column):
    sql2 = '''insert into ald_hierarchy_share (`day`,`app_key`,`''' + column + '''`,`update_at`)
    values (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE ''' + column + '''=VALUES(''' + column + '''),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for iii in tmp:
        count = 0
        if column == "first_share_count" or column == "secondary_share_count":
            count = r.get(iii)
        else:
            count = r.pfcount(iii)
        sql_data = (
            [iii.split("_")[3], iii.split("_")[4], count, update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'


# 一度分享和二度分享的回流比
def hierarchy_reflux_ratio(tmp_date):
    sql = "select day,app_key,frist_backflow/first_share_count,secondary_backflow/secondary_share_count from ald_hierarchy_share where day =" + "'" + tmp_date + "'" + " GROUP BY app_key"
    cur.execute(sql)
    results = cur.fetchall()
    sql2 = '''insert into ald_hierarchy_share (`day`,`app_key`,`frist_backflow_ratio`,`secondary_backflow_ratio`,`update_at`)
    values (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE frist_backflow_ratio=VALUES(frist_backflow_ratio),secondary_backflow_ratio=VALUES(secondary_backflow_ratio),update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for row in results:
        sql_data = ([row[0], row[1], row[2], row[3], update_at_tmp])
        args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 关键用户分析
def ST_ChainShare_nu(tmp):

    sql2 = '''insert into aldstat_key_user (`day`,`app_key`,`share_uuid`,`secondary_share_uuid`,`third_share_uuid`,`update_at`)
    values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE update_at = VALUES (update_at)
    '''
    # 批量写入
    args = []
    for iii in tmp:
        count = r.smembers(iii)
        for uuidi in count:
            uuidspt = uuidi.split("_")
            if len(uuidspt) == 1:
                sql_data = (
                    [iii.split("_")[3], iii.split("_")[4], uuidspt[0], "", "", update_at])
                args.append(sql_data)
            if len(uuidspt) == 2:
                sql_data = (
                    [iii.split("_")[3], iii.split("_")[4], uuidspt[0], uuidspt[1], "", update_at])
                args.append(sql_data)
            if len(uuidspt) == 3:
                sql_data = (
                    [iii.split("_")[3], iii.split("_")[4], uuidspt[0], uuidspt[1], uuidspt[2], update_at])
                args.append(sql_data)

    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

# 表驱动, 指标到索引
def ald_index(argument):
    switcher = {
        # 每小时的分享次数
        1: "STR~HourShare~c",
        # 每小时的分享带来的新增人数
        2: "HLL~HourShare~nu",
        # 每小时的分享打开次数
        3: "HLL~HourShare~oc",
        # 每小时的分享打开人数
        4: "HLL~HourShare~voc",
        # 分享人数
        5: "HLL~HourShare~uc",
        # 每天分享次数
        6: "STR~DayShare~c",
        # 每天分享带来的新增用户数
        7: "HLL~DayShare~nu",
        # 每天分享打开次数
        8: "HLL~DayShare~oc",
        # 每天分享打开人数
        9: "HLL~DayShare~voc",
        # 每天分享人数
        10: "HLL~DayShare~uc",
        # 每天页面分享次数
        11: "STR~PageShare~c",
        # 每天页面分享带来的新增用户数
        12: "HLL~PageShare~nu",
        # 每天页面分享打开次数
        13: "HLL~PageShare~oc",
        # 每天页面分享打开人数
        14: "HLL~PageShare~voc",
        # 每天分享人数
        15: "HLL~PageShare~uc",
    }
    return switcher.get(argument, "nothing")

def ald_index_tmp(argument):
    switcher = {
        # 一度分享人数
        9: "HLL_ZongShare_vc",
        # 一度分享次数
        10: "STR_ZongShare_c",
        # 一度分享打开次数
        11: "HLL_ZongShare_oc",
        # 一度分享带来的新用户数
        12: "HLL_ZongShare_nu",
        # 二度分享人数
        13: "HLL_ZongShare_sdvc",
        # 二度分享次数
        14: "STR_ZongShare_sdc",
        # 二度分享打开次数
        15: "HLL_ZongShare_sdoc",
        # 二度分享新增用户数
        16: "HLL_ZongShare_sdnu",
        # 关键用户分析
        17: "ST_ChainShare_nu"
        # # 所有用户信息
        # 18: "STR_UuidShare_nu"
    }
    return switcher.get(argument, "nothing")


if __name__ == '__main__':
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_at_tmp = int(time.time())
    ald_start_time =  int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = db_connect()
    cur = conn.cursor()

    nowDate = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天时间
    # 连接 redis
    r = redis_connect()
    redisKeys = r.keys("*Share*" + nowDate + "*")

    # 按照规定的格式拆分和拼接
    keyf = lambda s: "~".join(s.split("~", 3)[0:3])  # 分组函数
    keyf_tmp = lambda s: "_".join(s.split("_", 3)[0:3])  # 分组函数
    # 按照keyf拼接处的字符串分类循环redis_key,
    ald_index1 = []
    ald_index2 = []
    ald_index3 = []
    ald_index4 = []
    ald_index5 = []
    ald_index6 = []
    ald_index7 = []
    ald_index8 = []
    ald_index9 = []
    ald_index10 = []
    ald_index11 = []
    ald_index12 = []
    ald_index13 = []
    ald_index14 = []
    ald_index15 = []
    ald_index16 = []
    ald_index17 = []
    ald_index_tmp9 = []
    ald_index_tmp10 = []
    ald_index_tmp11 = []
    ald_index_tmp12 = []
    ald_index_tmp13 = []
    ald_index_tmp14 = []
    ald_index_tmp15 = []
    ald_index_tmp16 = []
    ald_index_tmp17 = []

    for category_rk, items in groupby(redisKeys, key=keyf):
        if category_rk == ald_index(1):
            ald_index1 += list(items)
        # 分享带来的新增
        if category_rk == ald_index(2):
            ald_index2 += list(items)
        # 分享打开次数
        if category_rk == ald_index(3):
            ald_index3 += list(items)
        # 分享打开人数
        if category_rk == ald_index(4):
            ald_index4 += list(items)
        # 分享打开人数
        if category_rk == ald_index(5):
            ald_index5 += list(items)

        # *****************计算每天分享的指标*******************
        if category_rk == ald_index(6):
            ald_index6 += list(items)
        if category_rk == ald_index(7):
            ald_index7 += list(items)
        if category_rk == ald_index(8):
            ald_index8 += list(items)
        if category_rk == ald_index(9):
            ald_index9 += list(items)
        if category_rk == ald_index(10):
            ald_index10 += list(items)

        # *****************计算每天页面分享的指标*******************

        if category_rk == ald_index(11):
            ald_index11 += list(items)
        if category_rk == ald_index(12):
            ald_index12 += list(items)
        if category_rk == ald_index(13):
            ald_index13 += list(items)
        if category_rk == ald_index(14):
            ald_index14 += list(items)
        if category_rk == ald_index(15):
            ald_index15 += list(items)

    for category_rk_tmp, items_tmp in groupby(redisKeys, key=keyf_tmp):
        # *****************计算每天分享层级的指标*******************
        if category_rk_tmp == ald_index_tmp(9):
            ald_index_tmp9 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(10):
            ald_index_tmp10 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(11):
            ald_index_tmp11 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(12):
            ald_index_tmp12 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(13):
            ald_index_tmp13 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(14):
            ald_index_tmp14 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(15):
            ald_index_tmp15 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(16):
            ald_index_tmp16 += list(items_tmp)
        if category_rk_tmp == ald_index_tmp(17):
            ald_index_tmp17 += list(items_tmp)

    STR_HourShare_c(ald_index1, "share_count")
    STR_HourShare_c(ald_index2, "new_count")
    STR_HourShare_c(ald_index3, "share_open_count")
    STR_HourShare_c(ald_index4, "share_open_user_count")
    STR_HourShare_c(ald_index5, "share_user_count")
    STR_DayShare_c(ald_index6, "share_count")
    STR_DayShare_c(ald_index7, "new_count")
    STR_DayShare_c(ald_index8, "share_open_count")
    STR_DayShare_c(ald_index9, "share_open_user_count")
    STR_DayShare_c(ald_index10, "share_user_count")
    STR_PageShare_c(ald_index11, "share_count")
    STR_PageShare_c(ald_index12, "new_count")
    STR_PageShare_c(ald_index13, "share_open_count")
    STR_PageShare_c(ald_index14, "share_open_user_count")
    STR_PageShare_c(ald_index15, "share_user_count")
    hierarchy_share_all(ald_index_tmp9, "first_share_user_count")
    hierarchy_share_all(ald_index_tmp10, "first_share_count")
    hierarchy_share_all(ald_index_tmp11, "frist_backflow")
    hierarchy_share_all(ald_index_tmp12, "first_share_new_user_count")
    hierarchy_share_all(ald_index_tmp13, "secondary_share_user_count")
    hierarchy_share_all(ald_index_tmp14, "secondary_share_count")
    hierarchy_share_all(ald_index_tmp15, "secondary_backflow")
    hierarchy_share_all(ald_index_tmp16, "secondary_share_new_user_count")
    ST_ChainShare_nu(ald_index_tmp17)
    hour_reflux_ratio(nowDate)
    day_reflux_ratio(nowDate)
    page_reflux_ratio(nowDate)
    hierarchy_reflux_ratio(nowDate)

    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
