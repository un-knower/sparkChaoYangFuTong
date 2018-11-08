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
        select app_key,link_key,sum(link_open_count) link_open_count,sum(link_page_count) link_page_count, sum(link_newer_for_app) link_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(link_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(link_page_count) bounce_rate
        from aldstat_daily_link where day between %s and %s GROUP BY app_key,link_key
        '''

    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_seven_collect = '''
                insert into aldstat_%sdays_single_link(app_key,day,link_key,link_open_count,link_page_count,link_newer_for_app,total_stay_time,
                secondary_stay_time,one_page_count,bounce_rate)
                VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE link_open_count=VALUES(link_open_count),link_page_count=VALUES(link_page_count),
                link_newer_for_app=VALUES(link_newer_for_app),total_stay_time=VALUES (total_stay_time),secondary_stay_time=VALUES (secondary_stay_time),
                one_page_count=VALUES (one_page_count),bounce_rate=VALUES (bounce_rate)
                '''
    values = []
    i = 0
    for row in seven_days_result:
        if i < 1000:
            # cur.execute(insert_seven_collect,(du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            values.append((du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            i += 1
        else:
            cur.executemany(insert_seven_collect, values)
            conn.commit()
            values = []
            i = 0

    if i > 0:
        cur.executemany(insert_seven_collect, values)
        conn.commit()


# 单个媒体汇总
def single_media_collect(num_date_ago, day, du):
    sql = '''
        select app_key,media_id,sum(media_open_count) media_open_count,sum(media_page_count) media_page_count, sum(media_newer_for_app) media_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(media_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(media_page_count) bounce_rate
        from aldstat_daily_media where day between %s and %s GROUP BY app_key,media_id
        '''

    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_seven_collect = '''
                insert into aldstat_%sdays_single_media(app_key,day,media_id,media_open_count,media_page_count,media_newer_for_app,total_stay_time,
                secondary_stay_time,one_page_count,bounce_rate)
                VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE media_open_count=VALUES(media_open_count),media_page_count=VALUES(media_page_count),
                media_newer_for_app=VALUES(media_newer_for_app),total_stay_time=VALUES (total_stay_time),secondary_stay_time=VALUES (secondary_stay_time),
                one_page_count=VALUES (one_page_count),bounce_rate=VALUES (bounce_rate)
                '''
    values = []
    i = 0
    for row in seven_days_result:
        # cur.execute(insert_seven_collect,(du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        if i < 1000:
            values.append((du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            i += 1
        else:
            cur.executemany(insert_seven_collect, values)
            conn.commit()
            values = []
            i = 0
    if i > 0:
        cur.executemany(insert_seven_collect, values)
        conn.commit()


# 单个位置汇总
def single_position_collect(num_date_ago, day, du):
    # 查出单个位置7天或30天数据
    sql = '''
        select app_key,position_id,sum(position_open_count) position_open_count,sum(position_page_count) position_page_count, sum(position_newer_for_app) position_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(position_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(position_page_count) bounce_rate
        from aldstat_daily_position where day between %s and %s GROUP BY app_key,position_id
        '''
    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_seven_collect = '''
                insert into aldstat_%sdays_single_position(app_key,day,position_id,position_open_count,position_page_count,position_newer_for_app,total_stay_time,
                secondary_stay_time,one_page_count,bounce_rate)
                VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE position_open_count=VALUES(position_open_count),position_page_count=VALUES(position_page_count),
                position_newer_for_app=VALUES(position_newer_for_app),total_stay_time=VALUES (total_stay_time),secondary_stay_time=VALUES (secondary_stay_time),
                one_page_count=VALUES (one_page_count),bounce_rate=VALUES (bounce_rate)
                '''
    values = []
    i = 0
    # 插入到单个位置汇总表
    for row in seven_days_result:
        # cur.execute(insert_seven_collect,(du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        if i < 1000:
            values.append((du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            i += 1
        else:
            cur.executemany(insert_seven_collect, values)
            conn.commit()
            values = []
            i = 0
    if i > 0:
        cur.executemany(insert_seven_collect, values)
        conn.commit()


# 7和30天汇总
def ad_tracking_collect(num_date_ago, day, du):
    print('汇总%s到%s的数据' % (num_date_ago, day))
    # 查每日表7天或30天的数据
    sql = '''
        select app_key,sum(link_open_count) link_open_count,sum(link_page_count) link_page_count, sum(link_newer_for_app) link_newer_for_app,
        sum(total_stay_time) total_stay_time,sum(total_stay_time)/sum(link_page_count) secondary_stay_time,sum(one_page_count) one_page_count,
        sum(one_page_count)/sum(link_page_count) bounce_rate
        from aldstat_link_summary where day between %s and %s GROUP BY app_key
        '''
    cur.execute(sql, (num_date_ago, day,))
    seven_days_result = cur.fetchall()

    insert_seven_collect = '''
                insert into aldstat_%sdays_link_summary(app_key,day,link_open_count,link_page_count,link_newer_for_app,total_stay_time,
                secondary_stay_time,one_page_count,bounce_rate)
                VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE link_open_count=VALUES(link_open_count),link_page_count=VALUES(link_page_count),
                link_newer_for_app=VALUES(link_newer_for_app),total_stay_time=VALUES (total_stay_time),secondary_stay_time=VALUES (secondary_stay_time),
                one_page_count=VALUES (one_page_count),bounce_rate=VALUES (bounce_rate)
                '''
    values = []
    i = 0
    # 插入到7天或30天汇总表
    for row in seven_days_result:
        # cur.execute(insert_seven_collect,(du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
        if i < 1000:
            values.append((du, row[0], day, row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            i += 1
        else:
            cur.executemany(insert_seven_collect, values)
            conn.commit()
            values = []
            i = 0
    if i > 0:
        cur.executemany(insert_seven_collect, values)
        conn.commit()


if __name__ == '__main__':
    ald_start_time = int(time.time())
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
