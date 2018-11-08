# coding=utf-8
import MySQLdb
import time

import datetime

import sys

import dbconf


def db_connect():
    time.time()
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


# 获取指定日期和前n天
def get_udf_date(year, month, day, num_day):
    date = datetime.date(year=year, month=month, day=day)
    num_date_ago = date - datetime.timedelta(days=num_day)
    return num_date_ago


# 1表示昨天日期
def get_date(day):
    return (datetime.date.today() - datetime.timedelta(days=day)).strftime('%Y-%m-%d')


# 1，0，0，0表示昨天的当前日期时间
def get_date_time(day, hour, minutes, microseconds):
    return (datetime.datetime.today() - datetime.timedelta(days=day, hours=hour, minutes=minutes,
                                                           microseconds=microseconds)).strftime('%Y-%m-%d %H:%M:%S')


# 单个外链汇总
def single_link_collect(num_date_ago, day, du):
    sql = '''
        select app_key,link_key,sum(link_authuser_count) link_authuser_count,sum(link_open_count) link_open_count,sum(link_page_count) link_page_count,
         sum(link_click_count) link_click_count,sum(link_newer_for_app) link_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(link_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(link_page_count) bounce_rate
        from aldstat_daily_link where day between %s and %s GROUP BY app_key,link_key
        '''

    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_fields = ['app_key', 'day', 'link_key', 'link_authuser_count','link_open_count', 'link_page_count',
                     'link_click_count','link_newer_for_app','total_stay_time','secondary_stay_time','one_page_count','bounce_rate']  # 插入的字段
    update_fields = ['link_authuser_count','link_open_count','link_page_count','link_click_count','link_newer_for_app',
                     'total_stay_time','secondary_stay_time','one_page_count','bounce_rate']  # 需要更新的字段
    insert_seven_collect = prepare_sql('''aldstat_%sdays_single_link'''%(du), insert_fields, update_fields)  # 准备 sql 语句
    args=[]
    for row in seven_days_result:

        sql_data = (row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],row[9],row[10])
        args.append(sql_data)

    try:
        print len(args)
        cur.executemany(insert_seven_collect, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error','single_link_collect'

# 单个媒体汇总
def single_media_collect(num_date_ago, day, du):
    sql = '''
        select app_key,media_id,sum(media_authuser_count) media_authuser_count,sum(media_open_count) media_open_count,sum(media_page_count) media_page_count,
        sum(media_click_count) media_click_count, sum(media_newer_for_app) media_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(media_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(media_page_count) bounce_rate
        from aldstat_daily_media where day between %s and %s GROUP BY app_key,media_id
        '''

    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_fields = ['app_key', 'day', 'media_id', 'media_authuser_count', 'media_open_count', 'media_page_count',
                     'media_click_count', 'media_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 插入的字段
    update_fields = ['media_authuser_count', 'media_open_count', 'media_page_count',
                     'media_click_count', 'media_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 需要更新的字段
    insert_seven_collect = prepare_sql('''aldstat_%sdays_single_media''' % (du), insert_fields, update_fields)  # 准备 sql 语句
    args = []

    for row in seven_days_result:

        sql_data = (row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
        args.append(sql_data)
    try:
        print len(args)
        cur.executemany(insert_seven_collect, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error','single_media_collect'
# 单个位置汇总
def single_position_collect(num_date_ago, day, du):
    # 查出单个位置7天或30天数据
    sql = '''
        select app_key,position_id,sum(position_authuser_count) position_authuser_count,sum(position_open_count) position_open_count,
        sum(position_page_count) position_page_count,sum(position_click_count) position_click_count, sum(position_newer_for_app) position_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(position_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(position_page_count) bounce_rate
        from aldstat_daily_position where day between %s and %s GROUP BY app_key,position_id
        '''
    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_fields = ['app_key', 'day', 'position_id', 'position_authuser_count', 'position_open_count', 'position_page_count',
                     'position_click_count', 'position_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 插入的字段
    update_fields = ['position_authuser_count', 'position_open_count', 'position_page_count',
                     'position_click_count', 'position_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 需要更新的字段
    insert_seven_collect = prepare_sql('''aldstat_%sdays_single_position''' % (du), insert_fields, update_fields)  # 准备 sql 语句
    args = []


    # 插入到单个位置汇总表
    for row in seven_days_result:

        sql_data = (row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
        args.append(sql_data)

    try:
        print len(args)
        cur.executemany(insert_seven_collect, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error', 'single_position_collect'
# 7和30天汇总
def ad_tracking_collect(num_date_ago, day, du):
    print('汇总%s到%s的数据' % (num_date_ago, day))
    # 查每日表7天或30天的数据
    sql = '''
        select app_key,sum(link_authuser_count) link_authuser_count,sum(link_open_count) link_open_count,sum(link_page_count) link_page_count,
         sum(link_click_count) link_click_count,sum(link_newer_for_app) link_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(link_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(link_page_count) bounce_rate
        from aldstat_link_summary where day between %s and %s GROUP BY app_key
        '''
    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_fields = ['app_key', 'day', 'link_authuser_count', 'link_open_count',
                     'link_page_count',
                     'link_click_count', 'link_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 插入的字段


    update_fields = ['link_authuser_count', 'link_open_count',
                     'link_page_count',
                     'link_click_count', 'link_newer_for_app', 'total_stay_time', 'secondary_stay_time',
                     'one_page_count', 'bounce_rate']  # 需要更新的字段
    insert_seven_collect = prepare_sql('''aldstat_%sdays_link_summary''' % (du), insert_fields,
                                   update_fields)  # 准备 sql 语句
    args = []  # 插入到7天或30天汇总表
    for row in seven_days_result:
        sql_data = (row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        args.append(sql_data)

    try:
        print len(args)
        cur.executemany(insert_seven_collect, args)  # 批量入库
    except Exception, e:
        print Exception, ":", e, ' sql insert error', 'ad_tracking_collect'
if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = db_connect()
    conn.set_character_set('utf8')
    cur = conn.cursor()

    days = [7, 30]
    args = sys.argv
    # 正常运行不用传参数，从昨天开始算(包括昨天),往前推7天和30天
    if '-du' not in args and '-d' not in args:
        print('正常执行')
        for du in days:
            # -----单个指标 to mysql
            single_link_collect(get_date(du), get_date(1), du)
            single_media_collect(get_date(du), get_date(1), du)
            single_position_collect(get_date(du), get_date(1), du)

            # -----汇总 to mysql
            ad_tracking_collect(get_date(du), get_date(1), du)
    # 根据指定-d的日期往前推-du（7 or 30）天
    else:
        du = int(args[args.index('-du') + 1])
        print('补%s天的数据' % du)
        day = args[args.index('-d') + 1]
        date = day.split('-')
        year = int(date[0])
        month = int(date[1])
        day_of_month = int(date[2])
        # -----单个指标 to mysql
        single_link_collect(get_udf_date(year, month, day_of_month, du), day, du)
        single_media_collect(get_udf_date(year, month, day_of_month, du), day, du)
        single_position_collect(get_udf_date(year, month, day_of_month, du), day, du)

        # -----汇总 to mysql
        ad_tracking_collect(get_udf_date(year, month, day_of_month, du), day, du)

    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
