################测试代码##################
import calendar

import datetime

import pandas as pd
import pymysql as db
import numpy as np
import time

from lib.db import db_op
from lib.db.db_op import insert_onedata, add_column, query_tb, update_dailytb, insert_month_data, insert_week_data, \
    query_tb_common, insert_dailytable
from lib.ma.ma import cal_ma_all, is_good_stock
from lib.stock_data.TickData import getAllTicksCode, getTickDataDaily

db_host = ''
db_user = 'root'
db_pw = 'ouyang123'
db_name = 'StockInfoSql'


############创建数据库############
def create_db():
    db_op.create_db(db_host,db_user,db_pw,db_name)

#################创建日线表####################
def create_dailytb():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        db_op.create_dailytable(db_host,db_user,db_pw,db_name, tb_name)

###################插入所有日线数据到表###################
def insert_dailytb():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        df = pd.read_table('C:\\all_stock_data\\' + code + '.csv', sep=',', encoding='gbk')
        insert_dailytable(db_host, db_user, db_pw, db_name, tb_name, df)

###################插入单条日线数据到表###################
def insert_onedailytb():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    print(codes)
    # for code in codes:
    code = '000002'
    tb_name = tb_head + code
    datas = getTickDataDaily(code)
    print(datas)
    insert_onedata(db_host, db_user, db_pw, db_name, tb_name,datas)

#####添加列
def add_alltick_column():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        add_column(db_host, db_user, db_pw, db_name, tb_name, 'weight_close', 'double')

#################创建月线表####################
def create_monthtb():
    tb_head = 'month_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        db_op.create_monthtb(db_host, db_user, db_pw, db_name, tb_name)

##############日线复权计算##########
def daily_weight_cal():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        datas = query_tb(db_host, db_user, db_pw, db_name, tb_name)
        if datas is None:
            print(code + 'no data')
            return
        number = len(datas)
        if(number<1):
            paras = 'update '+tb_name+' set weight_close = %lf where date=%s'%(datas[0][16],datas[0][1])
            update_dailytb(db_host, db_user, db_pw, db_name, tb_name,paras)
        index = 1
        while index < number:
            ####复权后的值=昨权值*（1-今涨幅）
            weight_close = datas[index-1][16]*(1 - datas[index][10]/100.0)
            paras = 'update '+tb_name+' set weight_close = %lf where date=%s'%(weight_close,datas[index][1])
            update_dailytb(db_host, db_user, db_pw, db_name, tb_name,paras)
            index = index+1

###从数据库中取出数据 放入dataframe中
def get_dataframe(tb_name):
    con = db.connect(db_host, db_user, db_pw, db_name, charset='utf8')
    paras = 'select * from ' + tb_name
    df = pd.read_sql(paras, con)
    con.close()
    return df

###日期+1###########
def add_one_mon(date):
    tmp=time.strptime(date,'%Y-%m')
    year = tmp.tm_year
    mon = tmp.tm_mon
    if mon == 12:
        mon = 1
        year = year + 1
    else:
        mon = mon + 1
    date = '%(year)4d-%(mon)02d'%{'year':year, 'mon':mon}
    return date

###############月线计算###########
def set_month_data():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    date_now = time.strftime('%Y-%m-%d',time.localtime())
    dic = dict()
    for code in codes:
        tb_name = tb_head + code
        df = get_dataframe(tb_name)
        length = len(df)
        if length < 1:
            return
        start_date = df.iloc[0, 1][:7]
        name = df.iloc[0, 3]
        while start_date < date_now:
            ###得到月线dataframe
            end_date = start_date + '-31'
            df_tmp = df.loc[df.loc[:,'date'] > start_date]
            df_tmp = df_tmp.loc[df_tmp.loc[:, 'date'] <= end_date]
            ###########所有价格与上月收盘价一致，权值与上月一致
            if df_tmp is None:
                return
            else:
                high = df_tmp.sort_values('high').loc[:'high'].iloc[-1]
                low = df_tmp.sort_values('low').loc[:'low'].iloc[0]
                open_price = df_tmp.loc[:'open'].iloc[0]
                close_price = df_tmp.loc[:'close'].iloc[-1]
                head = df_tmp.loc[:'head'].iloc[0]
                amount = head - close_price
                latitude = np.sum(df_tmp.loc[:'latitude'])
                weight_close = df_tmp.loc[:'weight_close'][-1]
                date = df_tmp.loc[:'date'][-1]
                dic.clear()
                dic['date'] = date
                dic['code'] = code
                dic['name'] = name
                dic['close'] = close_price
                dic['high'] = high
                dic['low'] = low
                dic['open'] = open_price
                dic['amount'] = amount
                dic['latitude'] = latitude
                dic['weight_close'] = weight_close
            ###保存数据入库
            insert_month_data(db_host, db_user, db_pw, db_name, tb_name, dic)
            ###继续下个月的计算
            start_date = add_one_mon(start_date)

#############获取同一周的日期列表###############
def getAllWeeks(df):
    length = len(df)
    if length < 1:
        print(' no data , return')
        return
    ####获取所有日期
    df_all_dates = df.loc[:,'date']
    dic_all_dates = dict()
    dt_all_w = list()
    dt_dates_list = list()
    ####把字符串日期转换成datetime.date,再转成周; 把周和日期存成字典
    for dt in df_all_dates:
        year = int(dt[:4])
        month = int(dt[5:7])
        day = int(dt[8:10])
        dt_t = datetime.date(year,month,day)
        dt_w = dt_t.isocalendar()
        dic_all_dates[dt] = dt_w[0:2]
        if dt_w[0:2] not in dt_all_w:
            dt_all_w.append(dt_w[0:2])

    ####根据字典，把同周的日期放入同一个元组
    for dtl in dt_all_w:
        w_list = list()
        for key, value in dic_all_dates.items():
            if value == dtl:
                w_list.append(key)
        dt_dates_list.append(w_list)
    return dt_dates_list

#############周线计算#############
def set_week_data():
    tb_head = 'daily_'
    codes = getAllTicksCode()
    dic = dict()
    for code in codes:
        tb_name = tb_head+code
        ###从数据库中取出数据 放入dataframe中
        df = get_dataframe(tb_name)

        ###遍历每一周，得到开盘价、收盘价、最高价、最低价、涨跌幅、涨跌额、权价
        week_date_list = getAllWeeks(df)
        if week_date_list is None:
            return
        name = df.iloc[0,3]
        for wdl in week_date_list:
            length = len(wdl)
            if length < 1:
                continue
            ###得到起止日期
            start_date = wdl[0]
            end_date = wdl[-1]
            ###得到这一周的dataframe
            df_week = df.loc[df.loc[:,'date']>=start_date]
            df_week = df_week.loc[df_week.loc[:,'date']<=end_date]
            ###获得所有周线数据
            high = df_week.sort_values('high').loc[:'high'].iloc[-1]
            low = df_week.sort_values('low').loc[:'low'].iloc[0]
            open_price = df_week.loc[:'open'].iloc[0]
            close_price = df_week.loc[:'close'].iloc[-1]
            head = df_week.loc[:'head'].iloc[0]
            amount = head - close_price
            latitude = np.sum(df_week.loc[:'latitude'])
            weight_close = df_week.loc[:'weight_close'][-1]
            date = df_week.loc[:'date'][-1]
            dic.clear()
            dic['date'] = date
            dic['code'] = code
            dic['name'] = name
            dic['close'] = close_price
            dic['high'] = high
            dic['low'] = low
            dic['open'] = open_price
            dic['amount'] = amount
            dic['latitude'] = latitude
            dic['weight_close'] = weight_close
            ###插入一条周线数据
            insert_week_data(db_host, db_user, db_pw, db_name, tb_name,dic)

##############均线存库#################
def save_all_ma():
    codes = getAllTicksCode()
    cal_ma_all(codes, 'daily_', 5)#五日均线
    cal_ma_all(codes, 'daily_', 10)  # 十日均线
    cal_ma_all(codes, 'daily_', 20)  # 二十日均线

    cal_ma_all(codes, 'week_', 5)  # 五周均线
    cal_ma_all(codes, 'week_', 10)  # 十周均线
    cal_ma_all(codes, 'week_', 20)  # 二十周均线

    cal_ma_all(codes, 'month_', 5)  # 五月均线
    cal_ma_all(codes, 'month_', 10)  # 十月均线
    cal_ma_all(codes, 'month_', 20)  # 二十月均线

###########用20日均线找出所有好股代码########
def find_all_good_stocks_with_ma():
    codes = getAllTicksCode()
    li = list()
    good_stock = list()
    for code in codes:
        tb_name = 'month_'+code
        paras = 'select ma20 from '+ tb_name + 'where ma20 is not null'
        datas = query_tb_common(db_host, db_user, db_pw, db_name, paras)
        li.clear()
        for data in datas:
            li.append(data[0])
        if is_good_stock(li):
            good_stock.append(code)
    return good_stock


# create_db()
# create_dailytb()
# insert_dailytb()
# insert_onedailytb()
# add_alltick_column()
#create_monthtb()
#set_month_data()
#set_week_data()
#save_all_ma()

# query_tb(db_host, db_user, db_pw, db_name, 'daily_000002')
