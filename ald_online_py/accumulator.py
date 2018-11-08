# -*- coding: utf-8 -*-
import MySQLdb
import time
import datetime
import dbconf


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


def total_new_user():
    """ 累加新增用户数 """
    #=============================================================1
    #gcs:查询出今天的每个app_key,今日新增用户数+昨日或者之前的累加数。
    select_user_count = '''
        select now.ak,now.new_comer_count+cur.total_visitor_count,now.day from
        (
        select app_key ak,total_visitor_count,day from aldstat_trend_analysis self 
        inner join
        (select cur.ak ak,max(day) max_day from
        (select app_key ak,day from aldstat_trend_analysis where day<CURRENT_DATE) cur
        group by cur.ak) cur
        on self.app_key=cur.ak and self.day=cur.max_day
        ) cur
        inner join
        (select app_key ak,new_comer_count,total_visitor_count,day from aldstat_trend_analysis where day=CURRENT_DATE) now
        on cur.ak=now.ak
        '''

    #=============================================================2
    #gcs:执行这条sql的语句。
    cur.execute(select_user_count)
    #=============================================================3
    #gcs:获得计算结果
    result = cur.fetchall()

    #=============================================================4
    #gcs:将最终的结果更新到 aldstat_trend_analysis 这个库当中SQl语句
    # 更新今日的total_visitor_count
    update_user_count = 'UPDATE aldstat_trend_analysis SET total_visitor_count=%s WHERE app_key=%s AND day=%s'

    #=============================================================5
    #gcs:执行Sql语句，将数据插入到 aldstat_trend_analysis 表当中

    # 过滤掉app_key为null和None的
    for line in result:
        if line[0] != None and line[0] != 'null':
            total_user = line[1]
            ak = line[0]
            print(total_user)
            cur.execute(update_user_count, (total_user, str(ak), str(line[2])))


if __name__ == '__main__':
    #=============================================================1
    #gcs:获得当前的秒级别的时间，将时间转换为int类型
    ald_start_time = int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #gcs:将今天的数据的格式设定为Y-m-d H:M:S的时间格式

    nowDate = datetime.datetime.now().strftime("%Y-%m-%d")  #gcs: 获取当天时间，将当天的时间的格式设定为Y-m-d 的格式

    #gcs:这里获得了nowDate的第二份的时间
    nowDate2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') #gcs:获得昨天的时间
    ts = int(time.time())

    conn = db_connect()
    cur = conn.cursor()
    total_new_user()

    cur.close()
    conn.commit()
    conn.close()
