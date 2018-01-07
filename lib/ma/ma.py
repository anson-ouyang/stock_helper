import numpy as np
import pandas as pd

from lib.db.db_op import update_dailytb
from lib.db.testdb import get_dataframe

db_host = ''
db_user = 'root'
db_pw = 'ouyang123'
db_name = 'StockInfoSql'


##############均线计算##################
################输入datafram,n日均线################
def cal_ma(df, nMa):
    length = len(df)
    if length < nMa:
        print('no enough data , return')
        return
    df_tmp = df.loc[:, 'weight_close']
    index = 0
    dic = dict()
    while index < length - nMa + 1:
        sum_close = np.sum(df_tmp[index:index+nMa])
        ma = sum_close / nMa
        dic[df.loc[:, 'date'].iloc[index + nMa - 1]]=ma
        index = index + 1
    return dic

###计算所有均线,并存库###
def cal_ma_all(codes, tb_head, nMa):
    for code in codes:
        tb_name = tb_head + code
        df = get_dataframe(tb_name)
        dic = cal_ma(df, nMa)
        for key in dic.keys():
            paras = 'update into ' + tb_name + ' set ma%d = %lf where date = %s'%(nMa, dic[key], key)
            update_dailytb(db_host, db_user, db_pw, db_name, tb_name,paras)

############获得总均值数、递增MA数#############
def get_rise_mas(ma):
    count = 0
    size = len(ma)
    if size < 2:
        return 0,0
    for index in range(size - 1):
        if ma[index + 1] >= ma[index]:
            count += 1
    return size,count

######用均值判断股票好坏##########
def is_good_stock(ma):
    size,count = get_rise_mas(ma)
    if float(count)/float(size)>0.8:
        return True
    else:
        return False

# stock_CodeUrl = 'http://quote.eastmoney.com/stocklist.html'

