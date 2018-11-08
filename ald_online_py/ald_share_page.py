# -*- coding:utf-8 -*-
import MySQLdb
import dbconf
import time,datetime

"""
    created by ymlf on 2018-01-08
"""


def db_connect():
    """ 连接数据库 """
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
    """
        准备 sql 语句模版, 返回一个 sql 语句:
        insert into table_name (...) values ('xxx', 'xxx', 'xxx') ON DUPLICATE KEY UPDATE ...
    """
    on_duplicate_key = []
    placeholder      = []

    [on_duplicate_key.append(item + '=' + 'VALUES(' + item + ')') for item in update_fields]
    [placeholder.append('%s') for item in insert_fields]

    insert_columns = '(' + ','.join(insert_fields) + ')'  # 准备要插入的字段
    values = 'values(' + ','.join(placeholder) + ')'      # 准备 values 值
    on_duplicate_key_update = ','.join(on_duplicate_key)  # 准备需要更新的字段

    # 将各部分拼接为字符串 insert into table (...) values(...) ON DUPLICATE KEY UPDATE ... 并返回
    return  "insert into %s %s %s ON DUPLICATE KEY UPDATE %s" % (table, insert_columns, values, on_duplicate_key_update)

def prepare_page_uri_for_ald_share_page():
    """
        把每日分享段页面路径写入 ald_share_page 表
    """
    day           = str(datetime.datetime.now().strftime("%Y-%m-%d"))           # 今日时间
    insert_fields = ['app_key','page_uri']                                      # 需要入库的字段
    update_fields = ['page_uri']                                                # 需要更新的字段
    sql           = prepare_sql('ald_share_page', insert_fields, update_fields) # 准备 sql 语句

    # 准备 sql 语句, 选出昨天的 app_key 和 page_uri
    ak_page_url = ''' select distinct app_key,page_uri from aldstat_dailyshare_page where day = '%s'  ''' % day
    # 执行该 sql
    cur.execute(ak_page_url)
    # 将查询出来的结果保存到名为 results 的列表中
    results = cur.fetchall()

    args = []
    for item in results:
        sql_data  = (item[0], item[1]) # item[0] -> app_key, item[1] -> page_uri
        args.append(sql_data)          # 将 sql 模版需要的数据追加到 args 数组中
    try:
        cur.executemany(sql,args)      # 执行批量入库
    except Exception,e:
        print Exception,':', e, 'ald_share_page写入异常!' # 打印异常

def prepare_uuid_for_ald_wechat_user_bind():
    """
        把分享者的 uuid 存入 ald_wechat_user_bind 表
    """
    update_at     = int(time.time())                                    # 数据更新时间
    day           = str(datetime.datetime.now().strftime("%Y-%m-%d"))   # 今日时间
    insert_fields = ['uuid','create_at']                                # 需要插入的字段
    update_fields = ['create_at']                                       # 需要更新的字段
    sql           = prepare_sql('ald_wechat_user_bind', insert_fields, update_fields)

    # 取昨天分享者的 uuid（去重）
    uuid_sql = ''' select distinct sharer_uuid from aldstat_dailyshare_user where day = '%s'  ''' % day
    cur.execute(uuid_sql)
    results = cur.fetchall()

    args = []
    for item in results:
        sql_data  = (item[0],update_at) # item[0] -> uuid
        args.append(sql_data)           # 将 sql 模版需要的数据追加到 args 数组
    try:
        cur.executemany(sql,args)       # 执行批量入库
    except Exception,e:
        print Exception,':', e, 'ald_wechat_user_bind 写入异常!'

def prepare_page_view_for_ald_share_page():
    """
        把受访页中的 page_view 写入 ald_share_page 表
    """
    day           = str(datetime.datetime.now().strftime("%Y-%m-%d"))           # 今日时间
    insert_fields = ['app_key','page_uri']                                      # 需要入库的字段
    update_fields = ['page_uri']                                                # 需要更新的字段
    sql           = prepare_sql('ald_share_page', insert_fields, update_fields) # 准备 sql 语句

    # 准备 sql 语句, 选出昨天的 app_key 和 page_path
    ak_page_url = ''' select distinct app_key,page_path from aldstat_page_view where day = '%s'  ''' % day
    # 执行该 sql
    cur.execute(ak_page_url)
    # 将查询出来的结果保存到名为 results 的列表中
    results = cur.fetchall()

    args = []
    for item in results:
        sql_data  = (item[0], item[1]) # item[0] -> app_key, item[1] -> page_path
        args.append(sql_data)          # 将 sql 模版需要的数据追加到 args 数组中
    try:
        cur.executemany(sql,args)      # 执行批量入库
    except Exception,e:
        print Exception,':', e, 'ald_share_page 写入异常!' # 打印异常


if __name__ == '__main__':
    """
        在 main 函数中调用各功能函数,
    """


    ald_start_time =  int(time.time()) # 脚本开始执行时间
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 脚本开始执行日期

    conn = db_connect() # 获取数据库连接
    cur = conn.cursor() # 获取数据库游标

    prepare_page_uri_for_ald_share_page()    # 都用函数
    prepare_uuid_for_ald_wechat_user_bind()  # 调用函数
    prepare_page_view_for_ald_share_page()   # 调用函数

    cur.close()   # 关闭数据库游标
    conn.commit() # 提交 sql
    conn.close()  # 关闭数据库连接

    ald_end_time = int(time.time()) # 脚本执行结束时间
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s" # 打印脚本执行时长
