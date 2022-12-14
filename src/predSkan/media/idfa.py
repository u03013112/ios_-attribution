# 希望验证一下idfa是否与大盘有着类似的分布与趋势
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import sys
sys.path.append('/src')

from src.tools import getFilename

idfaDf = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
totalDf = pd.read_csv(getFilename('totalData_20220501_20220930'))
# 付费增长率，这里不分媒体
def y():
    global idfaDf,totalDf

    idfaDfSum = idfaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    idfaDfSum.insert(idfaDfSum.shape[1],'y',0)
    idfaDfSum['y'] = idfaDfSum['sumr7usd']/idfaDfSum['sumr1usd'] - 1
    print(idfaDfSum)

    idfaDfSum['y'].rolling(7).mean().plot(label='idfa')

    totalDfSum = totalDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    totalDfSum.insert(totalDfSum.shape[1],'y',0)
    totalDfSum['y'] = totalDfSum['sumr7usd']/totalDfSum['sumr1usd'] - 1
    print(totalDfSum)

    totalDfSum['y'].rolling(7).mean().plot(label='total')

    plt.title("r7/r1 - 1") 
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/idfaY.png')
    print('save to /src/data/idfaY.png')
    plt.clf()


# 首日付费率对比
def cv0():
    global idfaDf,totalDf

    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    for i in range(153):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)

    data = {
        'install_date':dayList
    }
    # 先要sort，原来的数据是否sort过没有印象了
    idfaDfSort = idfaDf.sort_values(by=['install_date','cv'])
    idfaDfSortSum = idfaDfSort.groupby(['install_date','cv']).agg('sum')

    idfaX = idfaDfSortSum['count'].to_numpy().reshape((-1,64))
    idfaXSum = idfaX.sum(axis=1).reshape(-1,1)
    idfaX = idfaX/idfaXSum

    idfaCv0 = idfaX[:,0].reshape(-1)
    ifdaPR = (1-idfaCv0)*100
    # print(ifdaPR)
    data['ifdaPR'] = list(ifdaPR)
    # plt.plot(ifdaPR,label='idfa pay rate')

    totalDfSort = totalDf.sort_values(by=['install_date','cv'])
    totalDfSortSum = totalDfSort.groupby(['install_date','cv']).agg('sum')

    totalX = totalDfSortSum['count'].to_numpy().reshape((-1,64))
    totalXSum = totalX.sum(axis=1).reshape(-1,1)
    totalX = totalX/totalXSum

    totalCv0 = totalX[:,0].reshape(-1)
    totalPR = (1-totalCv0)*100
    # print(totalPR)
    data['totalPR'] = list(totalPR)
    # plt.plot(totalPR,label='total pay rate')

    df = pd.DataFrame(data=data)

    plt.title('pay rate in first day')

    df['ifdaPR'].rolling(7).mean().plot(label='idfa')
    df['totalPR'].rolling(7).mean().plot(label='total')
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/idfaPR.png')
    print('save to /src/data/idfaPR.png')
    plt.clf()

def cv32():
    global idfaDf,totalDf

    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    for i in range(153):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)

    data = {
        'install_date':dayList
    }
    # 先要sort，原来的数据是否sort过没有印象了
    idfaDfSort = idfaDf.sort_values(by=['install_date','cv'])
    idfaDfSortSum = idfaDfSort.groupby(['install_date','cv']).agg('sum')

    idfaX = idfaDfSortSum['count'].to_numpy().reshape((-1,64))
    idfaXSum = idfaX.sum(axis=1).reshape(-1,1)
    idfaX = idfaX/idfaXSum

    idfaCv32 = idfaX[:,1:32].sum(axis=1).reshape(-1)

    ifdaPR = (idfaCv32)*100
    # print(ifdaPR)
    data['ifdaPR'] = list(ifdaPR)
    # plt.plot(ifdaPR,label='idfa pay rate')

    totalDfSort = totalDf.sort_values(by=['install_date','cv'])
    totalDfSortSum = totalDfSort.groupby(['install_date','cv']).agg('sum')

    totalX = totalDfSortSum['count'].to_numpy().reshape((-1,64))
    totalXSum = totalX.sum(axis=1).reshape(-1,1)
    totalX = totalX/totalXSum

    totalCv32 = totalX[:,1:32].sum(axis=1).reshape(-1)
    totalPR = (totalCv32)*100
    # print(totalPR)
    data['totalPR'] = list(totalPR)
    # plt.plot(totalPR,label='total pay rate')

    df = pd.DataFrame(data=data)

    plt.title('cv 1~31 rate in first day')

    df['ifdaPR'].rolling(7).mean().plot(label='idfa')
    df['totalPR'].rolling(7).mean().plot(label='total')
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/idfaCv32.png')
    print('save to /src/data/idfaCv32.png')
    plt.clf()

def cv64():
    global idfaDf,totalDf

    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    for i in range(153):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)

    data = {
        'install_date':dayList
    }
    # 先要sort，原来的数据是否sort过没有印象了
    idfaDfSort = idfaDf.sort_values(by=['install_date','cv'])
    idfaDfSortSum = idfaDfSort.groupby(['install_date','cv']).agg('sum')

    idfaX = idfaDfSortSum['count'].to_numpy().reshape((-1,64))
    idfaXSum = idfaX.sum(axis=1).reshape(-1,1)
    idfaX = idfaX/idfaXSum

    idfaCv32 = idfaX[:,32:].sum(axis=1).reshape(-1)

    ifdaPR = (idfaCv32)*100
    # print(ifdaPR)
    data['ifdaPR'] = list(ifdaPR)
    # plt.plot(ifdaPR,label='idfa pay rate')

    totalDfSort = totalDf.sort_values(by=['install_date','cv'])
    totalDfSortSum = totalDfSort.groupby(['install_date','cv']).agg('sum')

    totalX = totalDfSortSum['count'].to_numpy().reshape((-1,64))
    totalXSum = totalX.sum(axis=1).reshape(-1,1)
    totalX = totalX/totalXSum

    totalCv32 = totalX[:,32:].sum(axis=1).reshape(-1)
    totalPR = (totalCv32)*100
    # print(totalPR)
    data['totalPR'] = list(totalPR)
    # plt.plot(totalPR,label='total pay rate')
    
    df = pd.DataFrame(data=data)
    df.to_csv('/src/data/idfaCv.csv')
    plt.title('cv 32~63 rate in first day')

    df['ifdaPR'].rolling(7).mean().plot(label='idfa')
    df['totalPR'].rolling(7).mean().plot(label='total')
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/idfaCv64.png')
    print('save to /src/data/idfaCv64.png')
    plt.clf()


def cv0Total():
    totalDf = pd.read_csv(getFilename('totalData20220501_20221215'))
    totalDf2 = pd.read_csv(getFilename('totalData220220501_20221215'))

    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    day1 = datetime.datetime.strptime('20221215','%Y%m%d')
    for i in range((day1-day0).days+1):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)

    data = {
        'install_date':dayList
    }
    
    totalDfSort = totalDf.sort_values(by=['install_date','cv'])
    # totalDfSortSum = totalDfSort.groupby(['install_date']).agg('sum')
    payCountDf = totalDfSort.loc[totalDfSort.cv > 0].groupby(['install_date']).agg('sum')
    print(totalDfSort.loc[totalDfSort.cv > 0])
    print(payCountDf)
    payCountNp = payCountDf['count'].to_numpy().reshape(-1)
    data['payCountNp'] = payCountNp

    totalDfSort2 = totalDf2.sort_values(by=['install_date','cv'])
    # totalDfSortSum2 = totalDfSort2.groupby(['install_date']).agg('sum')
    payCountDf2 = totalDfSort2.loc[totalDfSort2.cv > 0].groupby(['install_date']).agg('sum')
    print(payCountDf2)
    payCountNp2 = payCountDf2['count'].to_numpy().reshape(-1)
    data['payCountNp2'] = payCountNp2
    
    df = pd.DataFrame(data=data)

    plt.title('pay users in first day')

    # df['totalPR'].rolling(7).mean().plot(label='total rolling7')
    df['payCountNp'].rolling(7).mean().plot(label='AF')
    df['payCountNp2'].rolling(7).mean().plot(label='BI')
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/totalPR.png')
    print('save to /src/data/totalPR.png')
    plt.clf()

def r7Total():
    totalDf = pd.read_csv(getFilename('totalData20220501_20221215'))
    totalDf2 = pd.read_csv(getFilename('totalData220220501_20221215'))

    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    day1 = datetime.datetime.strptime('20221215','%Y%m%d')
    for i in range((day1-day0).days+1):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)

    data = {
        'install_date':dayList
    }
    
    totalDfSort = totalDf.sort_values(by=['install_date','cv'])

    totalDfSortSum = totalDfSort.groupby(['install_date']).agg('sum')
    totalR7 = totalDfSortSum['sumr7usd'].to_numpy().reshape((-1))
    data['totalR7'] = list(totalR7)
    # 
    totalDfSort2 = totalDf2.sort_values(by=['install_date','cv'])
    totalDfSortSum2 = totalDfSort2.groupby(['install_date']).agg('sum')
    totalR72 = totalDfSortSum2['sumr7usd'].to_numpy().reshape((-1))
    data['totalR72'] = list(totalR72)
    
    df = pd.DataFrame(data=data)

    plt.title('revenue 7 days')

    df['totalR7'].rolling(7).mean().plot(label='AF')
    df['totalR72'].rolling(7).mean().plot(label='BI')
    plt.xlabel("install date")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/totalR7.png')
    print('save to /src/data/totalR7.png')
    plt.clf()



if __name__ == '__main__':
    # y()
    # cv0()
    # cv32()
    # cv64()
    cv0Total()
    # r7Total()