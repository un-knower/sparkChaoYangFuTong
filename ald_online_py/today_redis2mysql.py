# -*- coding: utf-8 -*-
import MySQLdb
import redis
import time
import datetime
import dbconf
from itertools import groupby
from collections import defaultdict

"""
    modified by sunxiaowei on 2018-01-09
"""

def redis_connect():
    """ 连接 Redis """
    pool = redis.ConnectionPool(host='10.0.0.156', port=6379, password='crs-ezh65tiq:aldwxredis123', db=9)
    r = redis.Redis(connection_pool=pool)
    return r

def db_connect():
    """ 连接 mysql """
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
    """ 准备 sql 语句模版
        talbe：数据库的表名
        insert_fields：入库时需要插入的字段
        update_fields：ON DUPLICATE KEY 时需要更新的字段
    """

    on_duplicate_key = []
    placeholder = []

    [on_duplicate_key.append(item + '=' + 'VALUES(' + item + ')') for item in update_fields]
    [placeholder.append('%s') for item in insert_fields]

    insert_columns = '(' + ','.join(insert_fields) + ')'
    values = 'values(' + ','.join(placeholder) + ')'
    on_duplicate_key_update = ','.join(on_duplicate_key)

    return "insert into %s %s %s ON DUPLICATE KEY UPDATE %s" % (table, insert_columns, values, on_duplicate_key_update)

def hll_today_oc(items):
    """ 今日打开次数 """
    insert_fields = ['app_key', 'day', 'open_count', 'update_at']             # 插入的字段
    update_fields = ['open_count', 'update_at']                               # 需要更新的字段
    sql = prepare_sql('aldstat_trend_analysis', insert_fields, update_fields) # sql 语句

    today = datetime.date.today()  # 今日时间
    args = []                      # 保存 sql 参数的列表
    update_at = int(time.time())   # 更新时间
    for item in items:
        ak = item.strip().split('_')[4]     # 小程序唯一标识
        today_open_count = r.pfcount(item)  # 今日打开次数
        sql_data = (ak, today, today_open_count, update_at) # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表
    try:
        cur.executemany(sql, args) # 今日打开次数的批量入库
    except Exception, e:
        print Exception, ':', e, '今日打开次数写入异常!'


def str_today_tpc(items):
    """ 今日访问次数 """
    insert_fields = ['app_key', 'day', 'total_page_count', 'update_at']       # 插入的字段
    update_fields = ['total_page_count', 'update_at']                         # 需要更新的字段
    sql = prepare_sql('aldstat_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = int(time.time())  # 更新时间
    today = datetime.date.today() # 今日日期
    for item in items:
        ak = item.strip().split('_')[4]       # 小程序唯一标识
        today_total_page_count = r.get(item)  # 今日访问次数
        sql_data = (ak, today, today_total_page_count, update_at) # sql 数据
        args.append(sql_data)                                     # 将 sql 数据追加到 args 列表
    try:
        cur.executemany(sql, args)
    except Exception, e:
        print Exception, ':', e, '今日访问次数写入异常!'

def hll_today_vc(items):
    """ 今日访问人数  """
    insert_fields = ['app_key', 'day', 'visitor_count', 'update_at']          # 插入的字段
    update_fields = ['visitor_count', 'update_at']                            # 需要更新的字段
    sql = prepare_sql('aldstat_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = int(time.time())
    today = datetime.date.today()
    for item in items:
        ak = item.strip().split('_')[4]        # 小程序唯一标识
        today_visitor_count = r.pfcount(item)  # 今日访问人数
        sql_data = (ak, today, today_visitor_count, update_at) # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表
    try:
        cur.executemany(sql, args) # 今日访问人数的批量入库
    except Exception, e:
        print Exception, ':', e, '今日访问人数写入异常!'


def hll_today_ncnu(items):
    """ 今日新访问用户数 """
    insert_fields = ['app_key', 'day', 'new_comer_count', 'update_at']        # 插入的字段
    update_fields = ['new_comer_count', 'update_at']                          # 需要更新的字段
    sql = prepare_sql('aldstat_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = int(time.time())  # 更新时间
    today = datetime.date.today() # 今天的日期
    for item in items:
        ak = item.strip().split('_')[4]                          # 小程序唯一标识
        today_new_comer_count = r.pfcount(item)                  # 今日新访问用户数
        sql_data = (ak, today, today_new_comer_count, update_at) # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表
    try:
        cur.executemany(sql, args) # 今日新访问用户数的批量入库
	total_new_user()               # 累加新增用户数
    except Exception, e:
        print Exception, ':', e, '今日新访问用户数写入异常!'


def hs_today_br(items):
    """ 跳出率 """
    # HS_Today_br_2017-10-13_30fb2c160a428289bfe3d7b8d775af0e
    insert_fields = ['app_key', 'day', 'bounce_rate', 'update_at']            # 插入的字段
    update_fields = ['bounce_rate', 'update_at']                              # 需要更新的字段
    sql = prepare_sql('aldstat_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = int(time.time())  # 更新时间
    today = datetime.date.today() # 今日日期
    for item in items:
        ak = item.strip().split('_')[4]  # 小程序唯一标识
        count1 = 0          # 初始跳出页个数为 0
        count2 = 0          # 初始访问次数为 0
        bounce_rate = 0.0   # 初始跳出率为 0.0

        # 计算每日跳出率
        for v in r.hvals(item):
            if v == "1": count1 += 1 # 如果 v 的值是1, 则跳出页个数就 + 1
            count2 += int(v)         # 访问次数的值也加上 int(v)

        # 处理分母为 0 的异常情况
        if count2 == 0:
            bounce_rate = 0.0
        else:
            bounce_rate = count1 / float(count2) # 计算跳出率

        sql_data = (ak, today, bounce_rate, update_at) # sql 数据
        args.append(sql_data)

    try:
        cur.executemany(sql, args) # 今日跳出率的批量入库
    except Exception, e:
        print Exception, ':', e, '今日跳出率写入异常!'

def hll_hour_oc(items):
    """ 每小时打开次数 """
    insert_fields = ['app_key', 'day', 'hour', 'open_count', 'update_at']            # 插入的字段
    update_fields = ['open_count', 'update_at']                                      # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间, 格式为 YYMMDD HH:MM:SS 
    for item in items:
        (hour, ak) = item.strip().split('_')[4:6]                     # 获取 hour 和 app_key
        hourly_open_count = r.pfcount(item)                           # 每小时的打开次数
        sql_data = (ak, nowDate, hour, hourly_open_count, update_at)  # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每小时打开次数的批量入库
    except Exception, e:
        print Exception, ':', e, '今日每小时的打开次数写入异常!'


def str_hour_tpc(items):
    """ 每小时访问次数 """
    insert_fields = ['app_key', 'day', 'hour', 'total_page_count', 'update_at']      # 插入的字段
    update_fields = ['total_page_count', 'update_at']                                # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间
    for item in items:
        (hour, ak) = item.strip().split('_')[4:6] # 获取小时和 app_key
        hourly_page_count = r.get(item)           # 每小时的访问次数
        sql_data = (ak, nowDate, hour, hourly_page_count, update_at) # sql 数据
        args.append(sql_data)                     # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每小时访问次数的批量入库
    except Exception, e:
        print Exception, ':', e, '今日每小时的访问次数写入异常!'


def hll_hour_vc(items):
    """ 每小时访问人数 """
    insert_fields = ['app_key', 'day', 'hour', 'visitor_count', 'update_at']         # 插入的字段
    update_fields = ['visitor_count', 'update_at']                                   # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_trend_analysis', insert_fields, update_fields) # sql 数据

    args = []
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")   # 更新时间
    for item in items:
        (hour, ak) = item.strip().split('_')[4:6]                       # 获取小时和 app_key
        hourly_visitor_count = r.pfcount(item)                          # 每小时的访问次数
        sql_data = (ak, nowDate, hour, hourly_visitor_count, update_at) # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每小时访问人数的批量入库
    except Exception, e:
        print Exception, ':', e, '今日每小时的访问人数写入异常!'

def hll_hour_ncnu(items):
    """ 每小时新访问人数
        items: 列表类型,保存这个小时前缀为 HLL_Hour_ncnu 的 redis key
    """
    insert_fields = ['app_key', 'day', 'hour', 'new_comer_count', 'update_at']       # 插入的字段
    update_fields = ['new_comer_count', 'update_at']                                 # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新时间
    for item in items:
        (hour, ak) = item.strip().split('_')[4:6]                      # 获取小时和 app_key
        hourly_new_count = r.pfcount(item)                             # 每小时的新访问用户数
        sql_data = (ak, nowDate, hour, hourly_new_count, update_at)    # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每小时新访问人数批量入库
    except Exception, e:
        print Exception, ':', e, '今日每小时的访问人数写入异常!'


def hs_hourtoday_br(items):
    """
        小程序每小时跳出率
        items: 列表类型,保存这个小时前缀为 HS_HourToday_br 的 redis key
    """
    insert_fields = ['app_key', 'day', 'hour', 'bounce_rate', 'update_at']           # 插入的字段
    update_fields = ['bounce_rate', 'update_at']                                     # 需要更新的字段
    sql = prepare_sql('aldstat_hourly_trend_analysis', insert_fields, update_fields) # sql 语句

    args = []
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间
    for item in items:
        (hour, ak) = item.strip().split('_')[4:6] # 获取小时和app_key

        count1 = 0        # 初始跳出页个数
        count2 = 0        # 初始访问次数
        bounce_rate = 0.0 # 初始跳出率

        # 计算每小时的跳出率
        for v in r.hvals(item):
            if v == "1": count1 += 1 # 如果 v 的值为1则, 跳出页个数就 + 1
            count2 += int(v)         # 访问次数的值也相应增加 int(v)

        # 处理分母为 0 的异常情况:
        if count2 == 0:
            # 分母为 0 则跳出率为 0.0
            bounce_rate = 0.0
        else:
            # 分母不为 0 则直接计算跳出率
            bounce_rate = count1 / float(count2)

        sql_data = (ak, nowDate, hour, bounce_rate, update_at) # sql 数据
        args.append(sql_data) # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每小时跳出率的批量入库
    except Exception, e:
        print Exception, ':', e, '今日每小时的跳出率写入异常!'

def str_dayshare_c(redis_keys, share_count):
    """ 
        每天的分享次数
        redis_keys: 列表类型,保存今天前缀为 STR_DayShare_c 的 redis key
        share_count: 要插入的字段名
    """
    current_time = int(time.time()) # 更新时间

    # 准备 sql 语句
    sql = '''insert into aldstat_trend_analysis (`app_key`,`day`, `''' + share_count + '''`, `update_at`)
        values (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE ''' + share_count + '''=VALUES(''' + share_count + '''),update_at = VALUES (update_at)
        '''

    args = []
    for key in redis_keys:
        # redis key: STR_HourToday_sc_2017-11-13_64a7f2b033fb593a8598bbc48c3b8486
        row = key.split("_")       # 按照下划线 _ 分割 redis key
        share_count = r.get(key)   # 分享次数
        # row[4] -> app_key, row[3] -> day, share_count -> 分享次数, current_time -> 更新时间
        sql_data = ([row[4], row[3], share_count, current_time]) # sql 数据
        args.append(sql_data)      # 将 sql 数据追加到 args 列表

    try:
        cur.executemany(sql, args) # 每天分享次数的批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'

def str_hourshare_c(redis_keys, column):
    """ 每小时的分享次数
        redis_keys: 列表类型,保存这个小时前缀为 STR_HourShare_c 的 redis key
        column：要 insert into 的列
    """
    update_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间
    # 准备 sql 语句
    sql = '''insert into aldstat_hourly_trend_analysis (`app_key`, `day`,`hour`,`''' + column + '''`,`update_at`)
         values (%s,%s,%s,%s,%s)
         ON DUPLICATE KEY UPDATE ''' + column + '''=VALUES(''' + column + '''),update_at = VALUES (update_at)
         '''

    args = []
    # STR_HourToday_sc_2017-11-13_17_5baa602a58f5f152c13035091083bc2a
    for key in redis_keys:
        row = key.split("_")   # 按照下划线 _ 分割 redis key
        if len(row) == 6:
            count = r.get(key) # 每小时的分享次数
            sql_data = ([row[5], row[3], row[4], count, update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql, args) # 每小时分享次数的批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error'  # 表驱动, 指标到索引

def total_new_user():
    """ 累加新增用户数 """
    # 查询出今天的每个app_key,今日新增用户数+昨日累加数
    select_user_count = '''select now.app_key,now.new_comer_count+ytd.total_visitor_count,now.day from
        (select app_key,new_comer_count,total_visitor_count from aldstat_trend_analysis where day=%s) ytd
        inner join (select app_key,new_comer_count,total_visitor_count,day from aldstat_trend_analysis where day=%s) now
        on now.app_key=ytd.app_key
        '''
    cur.execute(select_user_count, (yesterday, nowDate))
    result = cur.fetchall()
    # 更新今日的total_visitor_count
    update_user_count = 'UPDATE aldstat_trend_analysis SET total_visitor_count=%s WHERE app_key=%s AND day=%s'
    # 过滤掉app_key为null和None的
    for line in result:
        if line[0] != None and line[0] != 'null':
            cur.execute(update_user_count, (line[1], str(line[0]), str(line[2])))

def ald_index(argument):
    """ 表驱动, 指标到索引 """
    switcher = {
        1: "HLL_Today_ncnu",    # 今日新访问人数
        2: "HLL_Hour_ncnu",     # 每小时新访问人数
        3: "HLL_Today_oc",      # 今日打开次数
        4: "HLL_Hour_oc",       # 每小时打开次数
        5: "STR_Today_tpc",     # 今日访问次数
        6: "STR_Hour_tpc",      # 每小时访问次数
        7: "HLL_Today_vc",      # 今日访问人数
        8: "HLL_Hour_vc",       # 每小时访问人数
        9: "HS_Today_br",       # 今日跳出率
        10: "HS_HourToday_br",  # 每小时跳出率
        11: "STR_Today_sc",     # 每天的分享次数
        12: "STR_HourToday_sc"  # 每小时的分享次次数
    }
    return switcher.get(argument, "nothing")

if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    nowDate = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天时间
    nowDate2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    ts = int(time.time())

    conn = db_connect()
    cur = conn.cursor()

    # 连接 redis
    r = redis_connect()
    # 获取redis的所有key
    rediskeys = r.keys("*" + nowDate + "*")  

    # 定义存储 redis-key 的列表
    hll_today_ncnu_list = []
    hll_hour_ncnu_list = []
    hll_today_oc_list = []
    hll_hour_oc_list = []
    str_today_tpc_list = []
    str_hour_tpc_list = []
    hll_today_vc_list = []
    hll_hour_vc_list = []
    hs_today_br_list = []
    hs_hourtoday_br_list = []

    # 分组函数, 按照前 3 个下划线分割的字符串对 redis key 进行分类
    keyf = lambda s: "_".join(s.split("_", 3)[0:3]) 
    for category, items in groupby(rediskeys, key=keyf):
        redis_key_list = list(items)

        if category == ald_index(1):  hll_today_ncnu_list += redis_key_list
        if category == ald_index(2):  hll_hour_ncnu_list += redis_key_list
        if category == ald_index(3):  hll_today_oc_list += redis_key_list
        if category == ald_index(4):  hll_hour_oc_list += redis_key_list
        if category == ald_index(5):  str_today_tpc_list += redis_key_list
        if category == ald_index(6):  str_hour_tpc_list += redis_key_list
        if category == ald_index(7):  hll_today_vc_list += redis_key_list
        if category == ald_index(8):  hll_hour_vc_list += redis_key_list
        if category == ald_index(9):  hs_today_br_list += redis_key_list
        if category == ald_index(10): hs_hourtoday_br_list += redis_key_list

    if len(hll_today_ncnu_list) > 0: hll_today_ncnu(hll_today_ncnu_list)  # 今日新访问人数
    if len(hll_hour_ncnu_list) > 0: hll_hour_ncnu(hll_hour_ncnu_list)  # 每小时新访问人数
    if len(hll_today_oc_list) > 0: hll_today_oc(hll_today_oc_list)  # 今日打开次数
    if len(hll_hour_oc_list) > 0: hll_hour_oc(hll_hour_oc_list)  # 每小时打开次数
    if len(str_today_tpc_list) > 0: str_today_tpc(str_today_tpc_list)  # 今日访问次数
    if len(str_hour_tpc_list) > 0: str_hour_tpc(str_hour_tpc_list)  # 每小时访问次数
    if len(hll_today_vc_list) > 0: hll_today_vc(hll_today_vc_list)  # 今日访问人数
    if len(hll_hour_vc_list) > 0: hll_hour_vc(hll_hour_vc_list)  # 每小时访问人数
    if len(hs_today_br_list) > 0: hs_today_br(hs_today_br_list)  # 每天跳出率
    if len(hs_hourtoday_br_list) > 0: hs_hourtoday_br(hs_hourtoday_br_list)  # 每小时跳出率

    # 分享的  redis key
    redis_share_keys = r.keys("*Today_sc_*" + nowDate + "*")

    str_dayshare_c_list = []
    str_hourshare_c_list = []

    # 分组函数
    keyshare = lambda s: "_".join(s.split("_", 3)[0:3]) 
    for category, items in groupby(redis_share_keys, key=keyshare):
        redis_key_list = list(items)
        # 趋势分析分享次数
        if category == ald_index(11): str_dayshare_c_list += redis_key_list
        if category == ald_index(12): str_hourshare_c_list += redis_key_list

    if len(str_dayshare_c_list) > 0: str_dayshare_c(str_dayshare_c_list, "daily_share_count")
    if len(str_hourshare_c_list) > 0: str_hourshare_c(str_hourshare_c_list, "daily_share_count")
    #total_new_user()  # 累加新增用户数

    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
