# -*- coding:utf-8 -*-
import re
import sys
import time
from datetime import datetime, timedelta
import MySQLdb
import dbconf


#=============================================================1
#gcs:写一个连接数据库的函数
# 连接数据库的函数
def db_connect(): #gcs:python即使有返回值，在写返回值的时候也是不要写内容的
    conn = MySQLdb.connect(
        host=dbconf.host,
        port=int(dbconf.port), #gcs:将dbconf中的端口号port提取出来
        user=dbconf.username,
        passwd=dbconf.password,
        db=dbconf.db,
        charset='utf8'
    )
    return conn  #gcs:注意啊，python当中即使有返回值，也是不要写返回值的




def seven_time(timedate):
    """gcs:\n
    返回七天的开始和结束时间 \n
    @:param timedate 用来计算时间的timedate的时间
    """

    #=============================================================1
    #gcs:创建了几个参数。这些参数是应该制定为全局的变量这样会不会好一些呢
    today = ""
    seven_days_ago = ""
    d = ""
    d0 = ""
    #=============================================================2
    #gcs:如果传进来的时间是一个空字符串。就取出来今天的时间。并且在今天的基础之上获得7天之前的时间
    if (timedate == ""):
        d = datetime.now()  # 获取今天日期
        d0 = d + timedelta(days=-1) #gcs:获得昨天的日期
        d1 = d + timedelta(days=-7)  # 获取七天前的日期
        today = d0.strftime("%Y-%m-%d")  # 将昨天的日期转换成str类型
        seven_days_ago = d1.strftime("%Y-%m-%d")  # 将7天前的日期转换成str类型
        #=============================================================3
        #gcs:判断日期的格式是否符合正则表达式。注意啊，在字符串中使用"与"运算符号的时候，要使用 and 这个符号
    elif (timedate != "" and re.search("\d{4}-\d{2}-\d{2}", timedate)): #gcs:我去，这里还有一个正则表达式的判断啊！
        d = datetime.now()
        a = time.mktime(time.strptime(timedate, '%Y-%m-%d'))
        x = time.localtime(a - 604800)  #gcs:这是计算的7天之前的时间
        y = time.localtime(a - 86400)   #gcs:这是计算的昨天的时间
        seven_days_ago = time.strftime('%Y-%m-%d', x)
        today = time.strftime('%Y-%m-%d', y)
    else:
        d = datetime.now()  # 获取今天日期
        d0 = d + timedelta(days=-1)
        d1 = d + timedelta(days=-7)  # 获取七天前的日期
        today = d0.strftime("%Y-%m-%d")  # 将日期转换成str类型
        seven_days_ago = d1.strftime("%Y-%m-%d")  # 将日期转换成str类型
    return (today, seven_days_ago, d)

def sum_terminal_all(timedate):
    (today, seven_days_ago, update_at) = seven_time(timedate)
    #=============================================================5
    #gcs:将数据换成了按照day字段进行了累加的方式。你把7天之前的天数计算出来之后，对每一天的数据进行了累加。
    sql = "SELECT app_key,type,type_value,SUM(new_comer_count),SUM(visitor_count),SUM(open_count),SUM(total_page_count),SUM(total_stay_time),SUM(one_page_count),SUM(total_stay_time)/SUM(open_count),SUM(one_page_count)/SUM(total_page_count) from aldstat_terminal_analysis where day >=" + "'" + seven_days_ago + "'" + " and day <=" + "'" + today + "'" + " GROUP BY app_key,type,type_value"
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    # 将7日汇总结果写入 7 日结果表中
    sql2 = """
    insert into aldstat_7days_terminal_analysis
(app_key,day,type,type_value,new_comer_count,visitor_count,open_count,total_page_count,total_stay_time,one_page_count,avg_stay_time,bounce_rate,update_at)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE new_comer_count=VALUES(new_comer_count),visitor_count=VALUES(visitor_count),open_count=VALUES(open_count),total_page_count=VALUES(total_page_count),total_stay_time=VALUES(total_stay_time),one_page_count=VALUES(one_page_count),avg_stay_time=VALUES(avg_stay_time),bounce_rate=VALUES(bounce_rate),update_at=VALUES(update_at) """
    # 批量写入
    args = []
    for row in results:
        r9 = 0
        r8 = 0
        if (row[9] == None):
            r8
        else:
            r8 = row[9]
        if (row[10] == None):
            r9
        else:
            r9 = row[10]

        if row[0] != "":
            sql_data = ([row[0], today, row[1], row[2], row[3],0 , row[5], row[6], row[7], row[8], float(r8), float(r9),update_at])
            args.append(sql_data)
    try:
        cur.executemany(sql2, args)
    except Exception, e:
        print Exception, ":", e, ' sql insert error'
    # 关闭数据库连接
    cur.close()
    conn.commit()
    conn.close()


def args_main():
    """gcs:\n
    判断参数是否有值
    """

    timeargs = ""
    dimension = ""

    if (len(sys.argv) > 1 and len(sys.argv) < 3):
        timeargs = sys.argv[1]
    elif (len(sys.argv) > 2 and len(sys.argv) < 4):
        timeargs = sys.argv[1]
        dimension = sys.argv[2]
    else:
        dimension = ""
        timeargs = ""
    return (dimension, timeargs)


if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#=============================================================4
    #gcs:从args_main函数当中获得多个返回值。在获得返回值的时候要使用花括号来进行接受()
    (dimension, timeargs) = args_main()
    if (timeargs == "terminal" or dimension == "terminal"):
        sum_terminal_all(timeargs)
    else:
        sum_terminal_all(timeargs)

    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
