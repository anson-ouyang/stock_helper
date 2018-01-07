import pymysql as db
import pandas as pd

#############创建数据库##########
def create_db(host, user, pw, name):
    try:
        con = db.connect(host, user, pw, charset='utf8')
        cursor = con.cursor()
        cursor.execute('show databases')
        rows = cursor.fetchall()
        for row in rows:
            print(row[0])
            if name == row[0]:
                cursor.execute('drop database if exists '+name)
            cursor.execute('create database if not exists ' + name)
            con.commit()

    except db.Error as e:
        print(e.args[0], e.args[1])
        pass
    finally:
        cursor.close()
        con.close()

###################销毁表###################
def drop_table(host, user, pw, db_name, table_name):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('drop table IF EXISTS '+table_name)

###################创建表###################
def create_dailytable(host, user, pw, db_name, table_name):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('CREATE TABLE IF NOT EXISTS ' + table_name +
                           '(Id INT PRIMARY KEY AUTO_INCREMENT, date VARCHAR(14), '+
                           'code varchar(6), name varchar(25), close double,' +
                           'high double, low double, open double, head double,' +
                           'amount double, latitude double, turnover double,' +
                           'volume double, transaction double, hkd double, famc double)')
            con.commit()

###########插入日线数据###########
def insert_dailytable(host, user, pw, db_name, table_name, df):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    length = len(df)
    index = 0
    with con:
        cursor = con.cursor()
        with cursor:
            while index < length:
                try:
                    cursor.execute('insert into ' + table_name +
                               '(date,code,name,close,high,low,open,head,amount,'+
                               'latitude,turnover,volume,transaction,hkd,famc)'+
                               "values('%s','%s','%s','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf')"
                               %(df.iloc[index,0],df.iloc[index,1][1:],df.iloc[index,2],
                                 float(df.iloc[index, 3]),float(df.iloc[index,4]),float(df.iloc[index,5]),
                                 float(df.iloc[index, 6]),float(df.iloc[index,7]),float(df.iloc[index,8]),
                                 float(df.iloc[index, 9]),float(df.iloc[index,10]),float(df.iloc[index,11]),
                                float(df.iloc[index,12]),float(df.iloc[index,13]),float(df.iloc[index,14])))

                except:
                    pass
                index = index + 1
            con.commit()

################插入一条日线记录#############
def insert_onedata(host, user, pw, db_name, table_name, dic):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('insert into ' + table_name +
                               '(date,code,name,close,high,low,open,head,amount,'+
                               'latitude,turnover,volume,transaction,hkd,famc)'+
                               "values('%s','%s','%s','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf','%lf')"
                               %(dic['date'],dic['code'],dic['name'],dic['close'],dic['high'],dic['low']
                                 , dic['open'],dic['head'],dic['amount'],dic['latitude'],dic['turnover']
                                 , dic['volume'],dic['transaction'],dic['hkd'],dic['famc']))

#############添加列##############
def add_column(host, user, pw, db_name, table_name, column_name, datatype):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('alter table '+table_name+' add '+
                           column_name + ' '+datatype)

#############创建月线表###########
def create_monthtb(host, user, pw, db_name, table_name):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('create table if not exists '+table_name +
                           '(Id INT PRIMARY KEY AUTO_INCREMENT, date VARCHAR(14), ' +
                           'code varchar(6), name varchar(25), close double,' +
                           'high double, low double, open double,amount double'+
                           'weight_close double')

#############创建周线表###########
def create_weektb(host, user, pw, db_name, table_name):
    create_monthtb(host, user, pw, db_name, table_name)

############插入一条月线记录#############
def insert_month_data(host, user, pw, db_name, table_name, dic):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute('insert into ' + table_name +
                           '(date,code,name,close,high,low,open,amount,latitude,weight_close)'+
                           'values(%s,%s,%s,%lf,%lf,%lf,%lf,%lf,%lf,%lf)'%(
                               dic['date'], dic['code'], dic['name'], dic['close']
                               , dic['high'], dic['low'], dic['open'], dic['amount']
                               , dic['latitude'], dic['weight_close'] ))

############插入一条周线记录#############
def insert_week_data(host, user, pw, db_name, table_name, dic):
    insert_month_data(host, user, pw, db_name, table_name, dic)

##########查询表#############
def query_tb_common(host, user, pw, db_name, paras):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute(paras)
            datas = cursor.fetchall()
            return datas

#################查询表################
def query_tb(host, user, pw, db_name, table_name):
    paras = 'select * from '+table_name
    query_tb_common(host, user, pw, db_name, paras)



#################更新表################
def update_dailytb(host, user, pw, db_name, table_name, paras):
    con = db.connect(host, user, pw, db_name, charset='utf8')
    with con:
        cursor = con.cursor()
        with cursor:
            cursor.execute(paras)

