# -*- coding: utf-8 -*-
import MySQLdb
import datetime
import time
import dbconf

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

#次均停留时长
conn = MySQLdb.connect(
    host=dbconf.host,
    port=int(dbconf.port),
    user=dbconf.username,
    passwd=dbconf.password,
    db=dbconf.db,
    charset='utf8'
)

ald_start_time =  int(time.time())
enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

nowDate=datetime.datetime.now().strftime("%Y-%m-%d")
nowDate2=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

conn.set_character_set('utf8') #设置字符集
#创建 游标
cur = conn.cursor()

cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')

#趋势分析小时 表
sqli="select app_key,day,hour,total_stay_time,open_count from  aldstat_hourly_trend_analysis where day=%s"
cur.execute(sqli,(nowDate,))
trend1 = cur.fetchall()
for i in trend1 :
    ak = i[0]
    day = i[1]
    hour = i[2]
    total_stay_time = i[3]
    open_count = i[4]

    if open_count==0 :
        avg_time_hour=0
    else:
        avg_time_hour=total_stay_time/float(open_count)

    #print avg_time_hour,"单个小时停留时长"
    sql1="update aldstat_hourly_trend_analysis set `secondary_avg_stay_time`=%s ,`update_at`=%s  where `day`=%s and `app_key`=%s and `hour`=%s"
    cur.execute(sql1,(avg_time_hour,nowDate2,nowDate,ak,hour))


#趋势分析 天表
sqli="select app_key,day,total_stay_time,open_count from  aldstat_trend_analysis where day=%s"
cur.execute(sqli,(nowDate,))
trend2 = cur.fetchall()
for i in trend2 :
    ak = i[0]
    day = i[1]
    total_stay_time = i[2]
    open_count = i[3]

    if open_count==0 :
        avg_time_day=0
    else:
        avg_time_day=total_stay_time/float(open_count)

    sql1="update aldstat_trend_analysis set `secondary_avg_stay_time`=%s ,`update_at`=%s where `day`=%s and `app_key`=%s  "
    cur.execute(sql1,(avg_time_day,int(time.time()),nowDate,ak))


cur.close()
conn.commit()
conn.close()

ald_end_time = int(time.time())
print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
