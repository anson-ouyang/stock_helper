################测试代码##################
import calendar

import datetime

import pandas as pd
import pymysql as db
import numpy as np
import time
import multiprocessing as mp
from lib.db import db_op
from lib.db.db_op import insert_onedata, add_column, query_tb, update_dailytb, insert_month_data, insert_week_data, \
    query_tb_common, insert_dailytable
# from lib.ma.ma import cal_ma_all
# from lib.ma.ma import is_good_stock
from lib.stock_data.TickData import getAllTicksCode, getTickDataDaily, deleteInvalidRows

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
        ##删掉无用行
        df = deleteInvalidRows(df)
        ##倒序
        df = df.sort_index(ascending=False)
        insert_dailytable(db_host, db_user, db_pw, db_name, tb_name, df)

def task_insert_onedailytb(codes, lock):
    tb_head = 'daily_'
    while True:
        lock.acquire()
        lenght = len(codes)
        if lenght < 1:
            lock.release()
            return
        code = codes.pop()
        lock.release()
        tb_name = tb_head + code
        datas = getTickDataDaily(code)
        insert_onedata(db_host, db_user, db_pw, db_name, tb_name, datas)

###################插入单条日线数据到表###################
def insert_onedailytb():
    codes = mp.Manager().list(['300628'])#getAllTicksCode())
    lock = mp.Lock()
    p1 = mp.Process(target=task_insert_onedailytb, args=(codes, lock))
    p2 = mp.Process(target=task_insert_onedailytb, args=(codes, lock))
    p3 = mp.Process(target=task_insert_onedailytb, args=(codes, lock))
    p4 = mp.Process(target=task_insert_onedailytb, args=(codes, lock))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()



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

#################创建周线表####################
def create_weektable():
    tb_head = 'week_'
    codes = getAllTicksCode()
    for code in codes:
        tb_name = tb_head + code
        db_op.create_weektb(db_host, db_user, db_pw, db_name, tb_name)

def task_daily_weight_cal(codes, lock):
    tb_head = 'daily_'
    while True:
        lock.acquire()
        num = len(codes)
        if num <= 0:
            lock.release()
            return
        code = codes.pop()
        lock.release()
        tb_name = tb_head + code
        datas = query_tb(db_host, db_user, db_pw, db_name, tb_name)
        if datas is None:
            print(code + ' no data')
            return
        number = len(datas)
        if(number<1):
            continue
        index = 1
        weight_close = datas[0][4]
        date = datas[0][1]
        paras = 'update ' + tb_name + ' set weight_close = %lf where date="%s"' % (weight_close, date)
        update_dailytb(db_host, db_user, db_pw, db_name, tb_name, paras)

        con = db.connect(db_host, db_user, db_pw, db_name, charset='utf8')
        cursor = con.cursor()

        while index < number:
            ####复权后的值=昨权值*（1+今涨幅）
            weight_close = weight_close * (1 + datas[index][10]/100.0)
            date = datas[index][1]
            paras = 'update '+tb_name+' set weight_close = %lf where date="%s"'%(weight_close,date)
            try:
                cursor.execute(paras)
                con.commit()
            except:
                print(code + " update error")
                break
            index = index+1
        cursor.close()
        con.close()
        print(code + ' ok')

##############日线复权计算##########
def daily_weight_cal():
    mycodes=['600000', '600001', '600002', '600003', '600004', '600005', '600006', '600007', '600008', '600009', '600010', '600011', '600012', '600015', '600016', '600017', '600018', '600019', '600020', '600021', '600022', '600023', '600025', '600026', '600027', '600028', '600029', '600030', '600031', '600033', '600035', '600036', '600037', '600038', '600039', '600048', '600050', '600051', '600052', '600053', '600054', '600055', '600056', '600057', '600058', '600059', '600060', '600061', '600062', '600063', '600064', '600065', '600066', '600067', '600068', '600069', '600070', '600071', '600072', '600073', '600074', '600075', '600076', '600077', '600078', '600079', '600080', '600081', '600082', '600083', '600084', '600085', '600086', '600087', '600088', '600089', '600090', '600091', '600092', '600093', '600094', '600095', '600096', '600097', '600098', '600099', '600100', '600101', '600102', '600103', '600104', '600105', '600106', '600107', '600108', '600109', '600110', '600111', '600112', '600113', '600114', '600115', '600116', '600117', '600118', '600119', '600120', '600121', '600122', '600123', '600125', '600126', '600127', '600128', '600129', '600130', '600131', '600132', '600133', '600135', '600136', '600137', '600138', '600139', '600141', '600143', '600145', '600146', '600148', '600149', '600150', '600151', '600152', '600153', '600155', '600156', '600157', '600158', '600159', '600160', '600161', '600162', '600163', '600165', '600166', '600167', '600168', '600169', '600170', '600171', '600172', '600173', '600175', '600176', '600177', '600178', '600179', '600180', '600181', '600182', '600183', '600184', '600185', '600186', '600187', '600188', '600189', '600190', '600191', '600192', '600193', '600195', '600196', '600197', '600198', '600199', '600200', '600201', '600202', '600203', '600205', '600206', '600207', '600208', '600209', '600210', '600211', '600212', '600213', '600215', '600216', '600217', '600218', '600219', '600220', '600221', '600222', '600223', '600225', '600226', '600227', '600228', '600229', '600230', '600231', '600232', '600233', '600234', '600235', '600236', '600237', '600238', '600239', '600240', '600241', '600242', '600243', '600246', '600247', '600248', '600249', '600250', '600251', '600252', '600253', '600255', '600256', '600257', '600258', '600259', '600260', '600261', '600262', '600263', '600265', '600266', '600267', '600268', '600269', '600270', '600271', '600272', '600273', '600275', '600276', '600277', '600278', '600279', '600280', '600281', '600282', '600283', '600284', '600285', '600286', '600287', '600288', '600289', '600290', '600291', '600292', '600293', '600295', '600296', '600297', '600298', '600299', '600300', '600301', '600302', '600303', '600305', '600306', '600307', '600308', '600309', '600310', '600311', '600312', '600313', '600315', '600316', '600317', '600318', '600319', '600320', '600321', '600322', '600323', '600325', '600326', '600327', '600328', '600329', '600330', '600331', '600332', '600333', '600335', '600336', '600337', '600338', '600339', '600340', '600343', '600345', '600346', '600348', '600349', '600350', '600351', '600352', '600353', '600354', '600355', '600356', '600357', '600358', '600359', '600360', '600361', '600362', '600363', '600365', '600366', '600367', '600368', '600369', '600370', '600371', '600372', '600373', '600375', '600376', '600377', '600378', '600379', '600380', '600381', '600382', '600383', '600385', '600386', '600387', '600388', '600389', '600390', '600391', '600392', '600393', '600395', '600396', '600397', '600398', '600399', '600400', '600401', '600403', '600405', '600406', '600408', '600409', '600410', '600415', '600416', '600418', '600419', '600420', '600421', '600422', '600423', '600425', '600426', '600428', '600429', '600432', '600433', '600435', '600436', '600438', '600439', '600444', '600446', '600448', '600449', '600452', '600455', '600456', '600458', '600459', '600460', '600461', '600462', '600463', '600466', '600467', '600468', '600469', '600470', '600472', '600475', '600476', '600477', '600478', '600479', '600480', '600481', '600482', '600483', '600485', '600486', '600487', '600488', '600489', '600490', '600491', '600493', '600495', '600496', '600497', '600498', '600499', '600500', '600501', '600502', '600503', '600505', '600506', '600507', '600508', '600509', '600510', '600511', '600512', '600513', '600515', '600516', '600517', '600518', '600519', '600520', '600521', '600522', '600523', '600525', '600526', '600527', '600528', '600529', '600530', '600531', '600532', '600533', '600535', '600536', '600537', '600538', '600539', '600540', '600543', '600545', '600546', '600547', '600548', '600549', '600550', '600551', '600552', '600553', '600555', '600556', '600557', '600558', '600559', '600560', '600561', '600562', '600563', '600565', '600566', '600567', '600568', '600569', '600570', '600571', '600572', '600573', '600575', '600576', '600577', '600578', '600579', '600580', '600581', '600582', '600583', '600584', '600585', '600586', '600587', '600588', '600589', '600590', '600591', '600592', '600593', '600594', '600595', '600596', '600597', '600598', '600599', '600600', '600601', '600602', '600603', '600604', '600605', '600606', '600607', '600608', '600609', '600610', '600611', '600612', '600613', '600614', '600615', '600616', '600617', '600618', '600619', '600620', '600621', '600622', '600623', '600624', '600625', '600626', '600627', '600628', '600629', '600630', '600631', '600632', '600633', '600634', '600635', '600636', '600637', '600638', '600639', '600640', '600641', '600642', '600643', '600644', '600645']
    codes = mp.Manager().list(mycodes)
    lock = mp.Lock()
    p1 = mp.Process(target= task_daily_weight_cal, args=(codes,lock))
    p2 = mp.Process(target= task_daily_weight_cal, args=(codes,lock))
    p3 = mp.Process(target= task_daily_weight_cal, args=(codes,lock))
    p4 = mp.Process(target= task_daily_weight_cal, args=(codes,lock))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()

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
    mtb_head = 'month_'
    codes = getAllTicksCode()
    date_now = time.strftime('%Y-%m-%d',time.localtime())
    dic = dict()
    for code in codes:
        tb_name = tb_head + code
        mtb_name = mtb_head +code
        df_mon = get_dataframe(mtb_name)
        size = len(df_mon)
        if size>=1:
            continue
        df = get_dataframe(tb_name)
        length = len(df)
        if length < 1:
            continue
        start_date = df.iloc[0, 1][:7]
        name = df.iloc[0, 3]
        while start_date < date_now:
            ###得到月线dataframe
            end_date = start_date + '-31'
            df_tmp = df.loc[df.loc[:,'date'] > start_date]
            df_tmp = df_tmp.loc[df_tmp.loc[:, 'date'] <= end_date]
            ###预置下个月的计算
            start_date = add_one_mon(start_date)
            ###########所有价格与上月收盘价一致，权值与上月一致
            if len(df_tmp) < 1:
                continue
            else:
                high = df_tmp.sort_values('high').loc[:,'high'].iloc[-1]
                low = df_tmp.sort_values('low').loc[:,'low'].iloc[0]
                open_price = df_tmp.loc[:,'open'].iloc[0]
                close_price = df_tmp.loc[:,'close'].iloc[-1]
                head = df_tmp.loc[:,'head'].iloc[0]
                amount = close_price-head
                latitude = np.sum(df_tmp.loc[:,'latitude'])
                weight_close = df_tmp.loc[:,'weight_close'].iloc[-1]
                date = df_tmp.loc[:,'date'].iloc[-1]
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
            insert_month_data(db_host, db_user, db_pw, db_name, mtb_name, dic)
        print(code + 'inserted')


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
    from lib.ma.ma import cal_ma_all
    # cal_ma_all(codes, 'daily_', 5)#五日均线
    # cal_ma_all(codes, 'daily_', 10)  # 十日均线
    # cal_ma_all(codes, 'daily_', 20)  # 二十日均线

    # cal_ma_all(codes, 'week_', 5)  # 五周均线
    # cal_ma_all(codes, 'week_', 10)  # 十周均线
    # cal_ma_all(codes, 'week_', 20)  # 二十周均线

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
        paras = 'select ma20 from '+ tb_name + ' where ma20 is not null'
        datas = query_tb_common(db_host, db_user, db_pw, db_name, paras)
        li.clear()
        if len(datas) < 1:
            continue
        for data in datas:
            li.append(data[0])
        from lib.ma.ma import is_good_stock
        if is_good_stock(li):
            good_stock.append(code)
    return good_stock


if __name__ == '__main__':
    # create_db()
    # create_dailytb()
    # insert_dailytb()
    # daily_weight_cal()
    # print(getAllTicksCode())
    # insert_onedailytb()
    # add_alltick_column()
    # create_monthtb()
    # set_month_data()
    #set_week_data()
    # save_all_ma()
    # create_weektable()
    # query_tb(db_host, db_user, db_pw, db_name, 'daily_000002')
    good_stocks = find_all_good_stocks_with_ma()
    print(good_stocks)