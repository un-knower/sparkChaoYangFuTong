# coding=utf-8
import MySQLdb
import datetime
import dbconf
import time


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


def update_user_apps_statistics_test():
    trend_result_today = 'select app_key,total_visitor_count,new_comer_count,visitor_count,total_page_count from aldstat_trend_analysis where day=%s and app_key=%s '
    update_today = 'update ald_user_apps_statistics set total_user_count=%s,today_new_user_count=%s,today_user_count=%s,today_visits_count=%s where app_key=%s and day=%s '
    trend_result_yesterday = 'select app_key,new_comer_count,visitor_count,total_page_count from aldstat_trend_analysis where day=%s and app_key=%s'
    update_yesterday = 'update ald_user_apps_statistics set yesterday_new_user_count=%s,yesterday_user_count=%s,yesterday_visits_count=%s where app_key=%s and day=%s '
    cur.execute(trend_result_today, (today, '64a7f2b033fb593a8598bbc48c3b8486'))
    for row in cur.fetchall():
        cur.execute(update_today, (row[1], row[2], row[3], row[4], row[0], today))

    cur.execute(trend_result_yesterday, (yesterday, '64a7f2b033fb593a8598bbc48c3b8486'))
    for row in cur.fetchall():
        cur.execute(update_yesterday, (row[1], row[2], row[3], row[0], yesterday))


# 从aldstat_trend_analysic ==> ald_user_apps_statistics
def update_user_apps_statistics():
    # 查询今日和昨日数据
    trend_result = '''select tod.app_key,tod.day,tod.total_visitor_count,tod.new_comer_count,ysd.new_comer_count,tod.visitor_count,ysd.visitor_count,tod.total_page_count,ysd.total_page_count
                        FROM (
                        (select app_key,day,total_visitor_count,new_comer_count,visitor_count,total_page_count from aldstat_trend_analysis where day=%s) tod
                        LEFT JOIN
                        (select app_key,new_comer_count,visitor_count,total_page_count from aldstat_trend_analysis where day=%s) ysd
                        ON tod.app_key=ysd.app_key)
                   '''

    # 插入或更新，不能用ignore方式
    add_result = '''insert into
    ald_user_apps_statistics(app_key,day,total_user_count,today_new_user_count,yesterday_new_user_count,
        today_user_count,yesterday_user_count,today_visits_count,yesterday_visits_count,update_at)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE total_user_count=VALUES (total_user_count),today_new_user_count=VALUES (today_new_user_count),yesterday_new_user_count=VALUES (yesterday_new_user_count),
      today_user_count=VALUES (today_user_count),yesterday_user_count=VALUES (yesterday_user_count),today_visits_count=VALUES (today_visits_count),yesterday_visits_count=VALUES (yesterday_visits_count),
      update_at=VALUES (update_at)
    '''

    cur.execute(trend_result, (today, yesterday,))
    for row in cur.fetchall():
        print row
        if row[4] is None and row[6] is None and row[8] is None:
            cur.execute(add_result,
                        (row[0], row[1], row[2], row[3], 0, row[5], 0, row[7], 0, int(time.time()),))
        else:
            cur.execute(add_result,
                        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], int(time.time()),))


def update_hourly_statistics_test():
    trend_hour_result = '''select new_comer_count,visitor_count,open_count,total_page_count,secondary_avg_stay_time,bounce_rate,app_key,DATE_FORMAT(day,'%Y-%m-%d'),hour
                           from aldstat_hourly_trend_analysis where app_key='64a7f2b033fb593a8598bbc48c3b8486' and day='2017-11-23' and hour='17'
                        '''
    update_statistics = '''update aldstat_hourly_statistics
                           set new_comer_count=%s,visitor_count=%s,open_count=%s,total_page_count=%s,secondary_avg_stay_time=%s,bounce_rate=%s
                           where app_key=%s and day=%s and hour=%s
                        '''
    cur.execute(trend_hour_result)
    for row in cur.fetchall():
        cur.execute(update_statistics, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))


# 趋势分析小时表 -> aldstat_hourly_statistics 一个小时一次
def update_hourly_statistics():
    trend_hour_result = '''select app_key,day,hour,new_comer_count,visitor_count,open_count,total_page_count,secondary_avg_stay_time,bounce_rate
                           from aldstat_hourly_trend_analysis where day=%s and hour=%s
                        '''
    add_statistics = ''' insert IGNORE into
                      aldstat_hourly_statistics(app_key,day,hour,new_comer_count,visitor_count,open_count,total_page_count,secondary_avg_stay_time,update_at,bounce_rate)
                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   '''
    cur.execute(trend_hour_result, (today, hour))
    for row in cur.fetchall():
        cur.execute(add_statistics,
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], int(time.time()), row[8]))

    # 单独更新次均停留时长
    if hour >= 4:
        stay_time_result = '''select app_key,day,hour,secondary_avg_stay_time
                               from aldstat_hourly_trend_analysis where day=%s and hour=%s
                         '''
        cur.execute(stay_time_result, (today, hour - 3,))
        for row in cur.fetchall():
            stay_time_update = 'update aldstat_hourly_statistics set secondary_avg_stay_time=%s where app_key=%s and day=%s and hour=%s'
            cur.execute(stay_time_update, (row[3], row[0], row[1], row[2]))


if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    today = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当天时间
    hour = int(datetime.datetime.now().strftime("%H")) - 1  # 获取当前小时的前一小时
    if hour == -1:
        hour = 23
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 昨天
    conn = db_connect()
    cur = conn.cursor()

    update_user_apps_statistics()
    # update_hourly_statistics_test()
    update_hourly_statistics()

    cur.close()
    conn.commit()
    conn.close()

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
