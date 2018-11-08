#-*- coding:utf-8 -*-

import time,datetime,sys,re,os
import MySQLdb
import dbconf
import urllib
import json
import urlparse

"""
    created by sunxiaowei on 2018-01-11 
    功能：过滤 u.html 类型的日志并处理, 然后入库
"""

def db_connect():
    """ 数据库连接 """
    conn    =  MySQLdb.connect(
        host    =  dbconf.host,
        port    =  int(dbconf.port),
        user    =  dbconf.username,
        passwd  =  dbconf.password,
        db      =  dbconf.db,
        charset = 'utf8mb4'
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

def aldwx_log_split(logname):
    ''' 数据清洗 '''

    istart = int(time.time())

    file_path   = '/data/app/ald_log/log_log/' + logname
    log_file    = open(file_path,'r')

    ald_bind_user_list = [] # 保存一个日志文件中所有的 bind_user

    try:
        for line in log_file:
            if ('GET /u.html?' in line) or ('u.php?' in line):
                try:
                    b_list = ald_user_bind(line)
                    if (b_list != None and len(b_list)>0): ald_bind_user_list.append(b_list)
                except Exception,e:
                    print Exception, ":", e, " split faild at user bind logs in ", logname
            else:
                pass
    finally:
         log_file.close()

    iend =  int(time.time())
    print '日志清洗时间: ', str(iend - istart), " s"
                 
    # 保存 user bind 数据到数据库
    if len(ald_bind_user_list)>0:
        try:
            start = int(time.time())
            save_user_bind_logs(ald_bind_user_list)
            end = int(time.time())
            print '入库时间:', str(end - start), " s"
        except Exception,e:
            print Exception, ":", e, " on save_bind_logs in ", logname+'_'+str(len(ald_bind_user_list))+'_binduser'
    else:
        print 'There is no user bind logs in ' + logname

def save_user_bind_logs(ald_bind_list):
    ''' 批量入库 '''
    insert_fields       = ['uuid','openid','unionid', 'nickname', 'country','province', 'city', 'gender', 'avatar_url', 'user_remark', 'create_at']                 # 插入的字段
    update_fields       = ['openid', 'unionid','nickname', 'country','province', 'city', 'gender', 'avatar_url', 'user_remark', 'create_at']                                           # 需要更新的字段
    sql                 = prepare_sql('ald_wechat_user_bind', insert_fields, update_fields)     # 准备 sql 语句

    conn = db_connect()
    cur = conn.cursor()
    
    args = []
    for row in ald_bind_list:
        data = (row['uuid'],row['openid'],row['unionid'],row['nickname'],row['country'],row['province'],row['city'],row['gender'],row['avatar_url'],row['user_remark'], int(time.time()))
        args.append(data)        
    try:
        cur.executemany(sql,args)
    except MySQLdb.IntegrityError:
        print 'ald_wechat_user_bind 写入异常'

    cur.close()
    conn.commit()
    conn.close()
     
def ald_user_bind(row):
    ''' 解析 nginx 日志, 返回一个包含各字段的字典 '''
    sdk_dict         = {}
    user_bind_dict   = {}
    pattern_url  = re.search('GET /u.html|GET /u.php', row)

    try:
        if (('servicewechat.com' in row) and pattern_url):
        
            s = ['uu','nickName','country','province', 'city', 'avatarUrl', 'user_remark']
            i = ['openid', 'unionid', 'gender']
    
            result = urlparse.urlparse(row)
            u_dict = urlparse.parse_qs(result.query)
            
            user_bind_dict['uuid']        = ''
            user_bind_dict['openid']      = 0
            user_bind_dict['unionid']     = 0
            user_bind_dict['nickname']    = ''
            user_bind_dict['country']     = ''
            user_bind_dict['province']    = ''
            user_bind_dict['city']        = ''
            user_bind_dict['gender']      = 0
            user_bind_dict['avatar_url']  = ''
            user_bind_dict['user_remark'] = ''
            user_bind_dict['create_at']   = int(time.time())
    
            if u_dict.get('uu'):          user_bind_dict['uuid']        = u_dict.get('uu')[0]
            if u_dict.get('openid'):      user_bind_dict['openid']      = u_dict.get('openid')[0]
            if u_dict.get('unionid'):     user_bind_dict['unionid']     = u_dict.get('unionid')[0]
            if u_dict.get('nickName'):    user_bind_dict['nickname']    = u_dict.get('nickName')[0]
            if u_dict.get('country'):     user_bind_dict['country']     = u_dict.get('country')[0]
            if u_dict.get('province'):    user_bind_dict['province']    = u_dict.get('province')[0]
            if u_dict.get('city'):        user_bind_dict['city']        = u_dict.get('city')[0]
            if u_dict.get('gender'):      user_bind_dict['gender']      = u_dict.get('gender')[0]
            if u_dict.get('avatarUrl'):   user_bind_dict['avatar_url']  = u_dict.get('avatarUrl')[0]
            if u_dict.get('user_remark'): user_bind_dict['user_remark'] = u_dict.get('user_remark')[0]
    except Exception,e:
        print e, " in ", row

    return user_bind_dict
    
def main(args):
    try:
        aldwx_log_split(args[1])
    except Exception,e:
        print Exception, ":", e, " in ", args[1]

if __name__ == '__main__':
    """ 传的参数为文件名 """
    main(sys.argv)
