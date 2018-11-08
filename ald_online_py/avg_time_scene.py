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

#单个场景值 小时 次均停留时长

sqli="select app_key,day,hour,scene_id,total_stay_time,open_count from  aldstat_hourly_scene where day=%s"
# 单个场景值 小时 次均停留时长
cur.execute(sqli,(nowDate,))
result = cur.fetchall()
for i in result:
    ak = i[0]
    day = i[1]
    hour = i[2]
    scene_id = i[3]
    total_stay_time = i[4]
    open_count = i[5]
    if open_count==0 :
        avg_time_hour=0
    else:
        avg_time_hour=total_stay_time/float(open_count)

    sql1="update aldstat_hourly_scene set `secondary_stay_time`=%s,`update_at`=%s where `day`=%s and `app_key`=%s and `hour`=%s and `scene_id`=%s"
    cur.execute(sql1,(avg_time_hour,nowDate,day,ak,hour,scene_id))

#单个场景值 天 次均停留时长
sqli="select app_key,day,scene_id,total_stay_time,scene_open_count from  aldstat_scene_statistics  where day=%s"
cur.execute(sqli,(nowDate,))
result1 = cur.fetchall()
for i in result1:
    ak = i[0]
    day = i[1]
    scene_id = i[2]
    total_stay_time = i[3]
    open_count = i[4]
    if open_count==0 :
        avg_time_day=0
    else:
        avg_time_day=total_stay_time/float(open_count)

    #print avg_time_day,"单个天停留时长"
    sql1="update aldstat_scene_statistics set `secondary_stay_time`=%s,`update_at`=%s where `day`=%s and `app_key`=%s and `scene_id`=%s"
    cur.execute(sql1,(avg_time_day,int(time.time()),day,ak,scene_id))

#场景值组 小时 次均停留时长
sqli="select app_key,day,hour,scene_group_id,total_stay_time,open_count from  aldstat_hourly_scene_group where day=%s"
cur.execute(sqli,(nowDate,))
result2 = cur.fetchall()
for i in result2:
    ak = i[0]
    day = i[1]
    hour = i[2]
    group_id = i[3]
    total_stay_time = i[4]
    open_count = i[5]
    if open_count==0 :
        avg_time_hour_group=0
    else:
        avg_time_hour_group=total_stay_time/float(open_count)

    #print avg_time_hour_group,"组个小时停留时长"
    sql1="update aldstat_hourly_scene_group set `secondary_stay_time`=%s,`update_at`=%s where `day`=%s and `app_key`=%s and `hour`=%s and `scene_group_id`=%s"
    cur.execute(sql1,(avg_time_hour_group,nowDate,day,ak,hour,group_id))

#场景值组 天 次均停留时长
sqli="select app_key,day,scene_group_id,total_stay_time,open_count from  aldstat_daily_scene_group where day=%s"
cur.execute(sqli,(nowDate,))
result3 = cur.fetchall()
for i in result3:
    ak = i[0]
    day = i[1]
    group_id = i[2]
    total_stay_time = i[3]
    open_count = i[4]
    if open_count==0 :
        avg_time_day_group=0
    else:
        avg_time_day_group=total_stay_time/float(open_count)
    sql1="update aldstat_daily_scene_group set `secondary_stay_time`=%s,`update_at`=%s where `day`=%s and `app_key`=%s and `scene_group_id`=%s"
    cur.execute(sql1,(avg_time_day_group,nowDate,day,ak,group_id))



cur.close()
conn.commit()
conn.close()

ald_end_time = int(time.time())
print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
