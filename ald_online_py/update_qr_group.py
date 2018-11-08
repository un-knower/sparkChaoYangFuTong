# -*- coding: utf-8 -*-
import MySQLdb
import time
import datetime
import hashlib
import dbconf
from collections import defaultdict

def db_connect():
    """ 数据库连接 """
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
    """ 准备 sql 语句模版 """
    on_duplicate_key = []
    placeholder      = []

    [on_duplicate_key.append(item + '=' + 'VALUES(' + item + ')') for item in update_fields]
    [placeholder.append('%s') for item in insert_fields]

    insert_columns = '(' + ','.join(insert_fields) + ')'
    values = 'values(' + ','.join(placeholder) + ')'
    on_duplicate_key_update = ','.join(on_duplicate_key)

    return  "insert into %s %s %s ON DUPLICATE KEY UPDATE %s" % (table, insert_columns, values, on_duplicate_key_update)

def md5_secret(qr_group_id):
    """ md5 加密 """
    unix_time = time.time()
    orgin_string = str(qr_group_id) + str(unix_time)
    m = hashlib.md5()
    m.update(orgin_string)
    return m.hexdigest()

def update_qr_before_online():
    insert_fields = ['app_key','qr_group_name'] # 插入的字段
    update_fields = ['qr_group_name']           # 需要更新的字段
    qr_group_sql  = prepare_sql('ald_qr_group', insert_fields, update_fields)

    insert_fields_ald_code = ['app_key','qr_group_key'] # 插入的字段
    update_fields_ald_code = ['qr_group_key']           # 需要更新的字段
    qr_group_sql_ald_code  = prepare_sql('ald_code', insert_fields, update_fields)

    # 数据库连接
    conn = db_connect()
    cur  = conn.cursor()

    # 获取没有默认组的 app_key
    sql = "SELECT app_key from user_apps where app_key NOT in (SELECT b.app_key from user_apps as a join ald_qr_group as b on a.app_key = b.app_key where b.qr_group_name='默认组' group by b.app_key) and app_key is not null and length(app_key)=32"
    cur.execute(sql)
    results = cur.fetchall()

    args = []
    for app_key in results:
        sql_data = (app_key[0], '默认组')
        args.append(sql_data)
        print(app_key)

    # 向 ald_qr_group 表中写入 qr_group_name 为默认组
    try:
        cur.executemany(qr_group_sql,args)
    except Exception,e:
        print Exception,':', e, 'ald_qr_group 写入异常!'

    for app_key in results:
        # 从 ald_qr_group 表中取得 id, 进行 md5 加密, 得到 qr_group_key
        cur.execute("select id from ald_qr_group where app_key ='%s'" % app_key[0])
        qr_group_id = cur.fetchone()
        qr_group_key = md5_secret(qr_group_id[0])

        # 更新 ald_qr_group 表中的 qr_group_key
        cur.execute("update IGNORE ald_qr_group set qr_group_key = '%s' where app_key = '%s'" % (qr_group_key, app_key[0]))

        # 更新 ald_code 表中的 qr_group_key
        sql_test= "update IGNORE ald_code set qr_group_key = '%s' where app_key = '%s' and qr_group_key = ''" % (qr_group_key, app_key[0])
        print sql_test
        cur.execute("update IGNORE ald_code set qr_group_key = '%s' where app_key = '%s' and qr_group_key = ''" % (qr_group_key, app_key[0]))

    cur.close()
    conn.commit()
    conn.close()

def update_qr_after_online():
    ''' 上线后执行 '''
    insert_fields = ['app_key','qr_group_name'] # 插入的字段
    update_fields = ['qr_group_name']           # 需要更新的字段
    qr_group_sql  = prepare_sql('ald_qr_group', insert_fields, update_fields)

    insert_fields_ald_code = ['app_key','qr_group_key'] # 插入的字段
    update_fields_ald_code = ['qr_group_key']           # 需要更新的字段
    qr_group_sql_ald_code  = prepare_sql('ald_code', insert_fields, update_fields)

    # 数据库连接
    conn = db_connect()
    cur  = conn.cursor()

    # 如果该 app_key 已经存在, 则使用之前的 qr_group_key

    old_qr_dict = {}
    sqlx = "select distinct app_key, qr_group_key from ald_qr_group where qr_group_name = '默认组'"
    cur.execute(sqlx)
    xresults = cur.fetchall()

    # 已经存在到默认组
    default_qr_group = defaultdict(dict)
    for row in xresults:
            default_qr_group[row[0]] = row[1]

    # 获取没有默认组的 app_key
    sql = "SELECT distinct app_key from ald_code where app_key is not null and app_key !=''  and qr_group_key =''"
    cur.execute(sql)
    results = cur.fetchall()

    args = []
    for app_key in results:
        sql_data = (app_key, '默认组')
        if default_qr_group.get(app_key[0]):
            print str(app_key) + " 已经在 ald_qr_group 表中有默认组： " + str(default_qr_group.get(app_key[0]))
        else:
            args.append(sql_data)

    # 向 ald_qr_group 表中写入 qr_group_name 为默认组
    try:
        cur.executemany(qr_group_sql,args)
    except Exception,e:
        print Exception,':', e, 'ald_qr_group 写入异常!'

    for app_key in results:
        # 从 ald_qr_group 表中取得 id, 进行 md5 加密, 得到 qr_group_key
        cur.execute("select id from ald_qr_group where app_key ='%s'" % app_key[0])
        qr_group_id = cur.fetchone()
        qr_group_key =  default_qr_group.get(app_key[0], md5_secret(qr_group_id[0]))
        print app_key,qr_group_key,"----"

        # 更新 ald_qr_group 表中的 qr_group_key
        if default_qr_group.get(app_key[0]):
            print str(app_key[0]) + " 在 ald_qr_group 表中已存在默认组: " + str(default_qr_group.get(app_key[0]))
        else:
            cur.execute("update IGNORE ald_qr_group set qr_group_key = '%s' where app_key = '%s'" % (qr_group_key, app_key[0]))

        # 更新 ald_code 表中的 qr_group_key
        cur.execute("update IGNORE ald_code set qr_group_key = '%s' where app_key = '%s' and qr_group_key = ''" % (qr_group_key, app_key[0]))

    cur.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    ald_start_time =  int(time.time())
    enter_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_qr_after_online() # 等用户不再使用旧版官网时该脚本就废弃了
    ald_end_time = int(time.time())
    print "start at: " + str(enter_time) + ", takes: " + str(ald_end_time - ald_start_time) + " s"
