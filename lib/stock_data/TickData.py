##################抓取东方财富数据,获取股票数据######################################
import time
import socket
import urllib.request
import re
import urllib.request as urllib2
import numpy as np
import datetime

def getDateInStr(string):
    li = string.split(sep='-')
    year = int(li[0])
    month = int(li[1])
    day = int(li[2])
    return year,month,day

#获取所有股票代码列表
def getAllTicksCode():
    url = 'http://quote.eastmoney.com/stocklist.html'
    allCodeList = []
    html = urllib.request.urlopen(url).read()
    html = html.decode('gbk')
    s = r'<li><a target="_blank" href="http://quote.eastmoney.com/\S\S(.*?).html">'
    pat = re.compile(s)
    code = pat.findall(html)
    for item in code:
        if item[0]=='6' or item[0]=='3' or item[0]=='0':
            allCodeList.append(item)
    return allCodeList

#############获取所有股票数据，保存到EXCEL################
def getAllTickData():
    allCodelist = getAllTicksCode()
    date = datetime.datetime.now().strftime('%Y%m%d')

    for code in allCodelist:
        print('正在获取%s股票数据...'%code)
        try:
            if code[0]=='6':
                url = 'http://quotes.money.163.com/service/chddata.html?code=0'+code+\
                '&end='+date+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
            else:
                url = 'http://quotes.money.163.com/service/chddata.html?code=1'+code+\
                '&end='+date+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
            urllib.request.urlretrieve(url,'c:\\all_stock_data\\'+code+'.csv')#可以加一个参数dowmback显示下载进度
        except:
            pass

##################爬虫获取网页##################
def getHtmlData(url):
    request = urllib2.Request(url)
    response = urllib2.urlopen(request)
    html = response.read()
    return html

#####################获取股票单条日线数据######################
def getTickDataDaily(tickCode):
    if tickCode[0] == '6' :
        url = r"https://gupiao.baidu.com/stock/sh" + tickCode + r".html"
    else:
        url = r"https://gupiao.baidu.com/stock/sz" + tickCode + r".html"
    try:
        html = getHtmlData(url)
        html = html.decode('utf8')
        ##收盘价
        s2 = r'<strong  class="_close">(-?\d*\.\d*)?</strong>'
        pat = re.compile(s2)
        close = pat.findall(html)
        if close[0] == '':
            return
        print(close)
        ##名称
        s3 = r'<meta charset="UTF-8"><meta name="keywords" content="(.*)\(\d\d\d\d\d\d\),.*股票,'
        pat = re.compile(s3)
        name = pat.findall(html)
        print(name)
        ##涨跌幅
        s4 = r'<span>([+-]?\d*\.\d*)?%</span>'
        pat = re.compile(s4)
        latitude = pat.findall(html)
        print(latitude)
        ##涨跌额
        amount = [0]
        if latitude[0] != '0.00':
            s5 = r'<span>([+-]?\d*\.\d*)?</span>'
            pat = re.compile(s5)
            amount = pat.findall(html)
        print(amount)
        # 获得股票数据
        s1 = r'<dl><dt>(.*)</dt><dd.*>(-?\d*\.\d*)?(万|亿)?.*</dd>'
        pat = re.compile(s1)
        tickData = pat.findall(html)
        print(tickData)
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        if tickData[0][1]=='' or tickData[1][1] == '':
            return
        dic = dict()
        dic['date'] = date
        dic['code'] = tickCode
        dic['name'] = name[0]
        dic['close'] = float(close[0])
        dic['high'] = float(tickData[2][1])
        dic['low'] = float(tickData[12][1])
        dic['open'] = float(tickData[0][1])
        dic['head'] = float(tickData[10][1])
        dic['amount'] = float(amount[0])
        dic['latitude'] = float(latitude[0])
        dic['turnover'] = float(tickData[11][1])
        dic['volume'] = float(tickData[1][1])*10000
        if tickData[1][2] == '亿':
            dic['volume'] = float(tickData[1][1]) * 10000

        dic['transaction'] = float(tickData[17][1])
        dic['hkd'] = float(tickData[16][1])*100000000
        dic['famc'] = float(tickData[7][1])*100000000

        return dic
    except:
        pass

###############获取市盈率,获取不到返空（退市或停牌股票）#################
def getTickMrq(tickCode):
    if tickCode[0] == '6' :
        url = r"https://gupiao.baidu.com/stock/sh" + tickCode + r".html"
    else:
        url = r"https://gupiao.baidu.com/stock/sz" + tickCode + r".html"
    try:
        print(url)
        html = getHtmlData(url)
        html = html.decode('utf8')
        s = r'<dl><dt class="mt-1">市盈率<sup>MRQ</sup></dt><dd>(-?\d*\.\d*)?</dd></dl>'
        # print(html)
        #获得市盈率
        pat = re.compile(s)
        startPrice = pat.findall(html)
        return startPrice[0]
    except:
        pass
# print(getTickMrq('000001'))
# print(getTickMrq('601313'))

#########删除无效行##########
def deleteInvalidRows(dailyDf):
    return dailyDf.drop(dailyDf[dailyDf.收盘价<=0].index.tolist())

# codeList = getAllTicksCode()
# print(codeList)
# for code in codeList:
#     print(getTickDataDaily(code))
# path = 'C:\\all_stock_data\\' + '002415' + '.csv'
# df = pd.read_table(path, sep=',', encoding='gbk')
# df = deleteInvalidRows(df)
# df = rehabilitation(df)
# # print(df)
# m_aver_price = getAllMonthClosePrice(df)
# print(m_aver_price)
# flag = isGoodTicket(m_aver_price, 0.8)
# if flag is True:
#     print('002415' + ' is a good ticket')
