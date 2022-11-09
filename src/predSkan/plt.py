# 画一些图
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import sys
sys.path.append('/src')

from src.tools import getFilename

def totalCvR7():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7_%d.png'%(cv))
        print('save to /src/data/totalCvR7_%d.png'%(cv))
        plt.clf()

def totalCvR7Less1000():
    print('画图，cv count 小于1000，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 1000)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L1k_%d.png'%(cv))
        print('save to /src/data/totalCvR7L1k_%d.png'%(cv))

def totalCvR7Less50():
    print('画图，cv count 小于50，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 50)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L50_%d.png'%(cv))
        print('save to /src/data/totalCvR7L50_%d.png'%(cv))

# 过滤掉平均7日付费过高的数据，最高每人400usd
def totalCvR7F1():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    df.loc[df.sumr7usd > df['count'] * 400,'sumr7usd']=df['count']*400
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7F1_%d.png'%(cv))
        print('save to /src/data/totalCvR7F1_%d.png'%(cv))
        plt.clf()

# 和geo相关的一些
def geoAbout():

    geoList = [
        {'name':'US','codeList':['US'],'userCountLine':[]},
        {'name':'CA','codeList':['CA'],'userCountLine':[]},
        {'name':'AU','codeList':['AU'],'userCountLine':[]},
        {'name':'GB','codeList':['GB'],'userCountLine':[]},
        {'name':'NZ','codeList':['NZ'],'userCountLine':[]},
        {'name':'DE','codeList':['DE'],'userCountLine':[]},
        {'name':'FR','codeList':['FR'],'userCountLine':[]},
        {'name':'KR','codeList':['KR'],'userCountLine':[]},
        {'name':'GCC','codeList':['AE','BH','KW','OM','QA','ZA','SA'],'userCountLine':[]}
    ]

    geoDf = pd.read_csv(getFilename('totalGeoData4_20220501_20220930'))
    totalDf = pd.read_csv(getFilename('totalData20220501_20220930'))

    # 按日期，7日付费金额划线，累计线
    dateList = []
    userCountLine = []

    sinceTime = datetime.datetime.strptime('20220501','%Y%m%d')
    unitlTime = datetime.datetime.strptime('20220930','%Y%m%d')
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dateList.append(dayStr)
        totalUsd = totalDf.loc[totalDf.install_date == dayStr,'count'].sum()
        userCountLine.append(totalUsd)

        for geo in geoList:
            usd = geoDf.loc[(geoDf.install_date == dayStr) & (geoDf.geo == geo['name']),'count'].sum()
            geo['userCountLine'].append(usd)

        
    plt.title("") 
    plt.xlabel("date 20220501->20220930") 
    plt.ylabel("user count") 
    plt.plot(dateList,userCountLine,'-',label='total')
    print(sum(userCountLine))
    for geo in geoList:
        plt.plot(dateList,geo['userCountLine'],'-',label=geo['name'])
        userCountLine = sum(geo['userCountLine'])
        print(geo['name'],userCountLine)
    plt.legend()
    plt.savefig('/src/data/geo.png')
    print('save to /src/data/geo.png')
    plt.clf()

import numpy as np
# 美国数据分析 
def us():
    # 针对美国和大盘的对比
    geoDf = pd.read_csv(getFilename('totalGeoData4_20220501_20220930'))
    totalDf = pd.read_csv(getFilename('totalData20220501_20220930'))
    usDf = geoDf.loc[geoDf.geo == 'US'].groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    globalDf = totalDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})

    df = globalDf.join(usDf,how='left',on='install_date',lsuffix='_global',rsuffix='_us')
    
    # 美国/大盘 1日
    plt.title("US/Global revenue in 1d") 
    plt.xlabel("date") 
    plt.ylabel("r1usd sum") 
    plt.plot(np.arange(len(df)),df['sumr1usd_global'].to_numpy(),'r-')
    plt.plot(np.arange(len(df)),df['sumr1usd_us'].to_numpy(),'b-')
    plt.savefig('/src/data/r1usd.png')
    print('save to /src/data/r1usd.png')
    plt.clf()
    # 美国/大盘 7日
    plt.title("US/Global revenue in 7d") 
    plt.xlabel("date") 
    plt.ylabel("r7usd sum") 
    plt.plot(np.arange(len(df)),df['sumr7usd_global'].to_numpy(),'r-')
    plt.plot(np.arange(len(df)),df['sumr7usd_us'].to_numpy(),'b-')
    plt.savefig('/src/data/r7usd.png')
    print('save to /src/data/r7usd.png')
    plt.clf()
    # 美国 1日/7日
    plt.title("US revenue in 1d/7d") 
    plt.xlabel("date") 
    plt.ylabel("r1usd/r7usd sum") 
    plt.plot(np.arange(len(df)),(df['sumr1usd_us']/df['sumr7usd_us']).to_numpy(),'r-')
    plt.savefig('/src/data/r1p7us.png')
    print('save to /src/data/r1p7us.png')
    plt.clf()
    # 大盘 1日/7日
    plt.title("Global revenue in 1d/7d") 
    plt.xlabel("date") 
    plt.ylabel("r1usd/r7usd sum") 
    plt.plot(np.arange(len(df)),(df['sumr1usd_global']/df['sumr7usd_global']).to_numpy(),'r-')
    plt.savefig('/src/data/r1p7glo.png')
    print('save to /src/data/r1p7glo.png')
    plt.clf()
    
    global1p7Np = (df['sumr1usd_global']/df['sumr7usd_global']).to_numpy()
    global1p7NpVar = np.var(global1p7Np)
    print('global 方差：',global1p7NpVar)
    global1p7NpStd = np.std(global1p7Np)
    print('global 标准差：',global1p7NpStd)
    # 
    us1p7Np = (df['sumr1usd_us']/df['sumr7usd_us']).to_numpy()
    us1p7NpVar = np.var(us1p7Np)
    print('us 方差：',us1p7NpVar)
    us1p7NpStd = np.std(us1p7Np)
    print('us 标准差：',us1p7NpStd)
    
from src.predSkan.totalAI2 import dataStep1
# 均线
def MA():
    df = dataStep1('20220501','20220930')
    sumByDayDf = df.groupby('install_date').agg(sum=('sumr7usd','sum')).sort_values(by=['install_date']).reset_index()


    days = [3,7,14]

    for day in days:
        plt.title("US/Global revenue in 1d") 
        plt.xlabel("date") 
        plt.ylabel("r7usd") 
        
        sumByDayDf['sum'].plot(label='true')
        sumByDayDf['sum'].rolling(window=day).mean().plot(label='MA%d'%day)

        plt.legend(loc='best')
        plt.savefig('/src/data/ma%d.png'%day)
        print('save to /src/data/ma%d.png'%day)
        plt.clf()

    # ema
    for day in days:
        plt.title("US/Global revenue in 1d") 
        plt.xlabel("date") 
        plt.ylabel("r7usd") 
        
        sumByDayDf['sum'].plot(label='true')
        sumByDayDf['sum'].ewm(span=day).mean().plot(label='EMA%d'%day)
        plt.legend(loc='best')
        plt.savefig('/src/data/ema%d.png'%day)
        print('save to /src/data/ema%d.png'%day)
        plt.clf()
    

if __name__ == '__main__':
    # totalCvR7()
    # totalCvR7Less1000()
    # totalCvR7Less50()
    # totalCvR7F1()
    # geoAbout()
    # us()
    MA()