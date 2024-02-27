# 计算各种指数
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os as importOs
import sys
sys.path.append('/src')

from src.sensortower.intel import getTopApp,getDownloadAndRevenue
from src.sensortower.iosIdToName import iOSIdToName
from src.sensortower.androidIdToName import androidIdToName


# 计算所有相关APP的下载量的和，和的走势就是需要的市场走势
def getDownloadAndRevenueSum(appIdList,os,country,dateGranularity,startDate,endDate):
    sumDf = pd.DataFrame(columns=['date','downloads','revenues'])
    for appid in appIdList:
        downloadDf = getDownloadAndRevenue(appid,os=os,countries=country,date_granularity=dateGranularity,startDate=startDate,endDate=endDate)
        sumDf = pd.concat([sumDf,downloadDf[['date','downloads','revenues']]])
    sumDf = sumDf.groupby('date').sum().reset_index()
    return sumDf

def getAPPsDownloadAndRevenue(startDate,endDate,os,country,dateGranularity='weekly',filterId = '6009d417241bc16eb8e07e9b',limit = 10):
    topAppDf = getTopApp(os=os,custom_fields_filter_id=filterId,time_range='year',limit=limit,category='all',countries=country,startDate=startDate,endDate=endDate)

    downloadsRetDf = pd.DataFrame(columns=['appId','date','downloads'])
    revenuesRetDf = pd.DataFrame(columns=['appId','date','revenues'])

    for appid in tqdm(topAppDf['appId']):
        downloadDf = getDownloadAndRevenue(appid,os=os,countries=country,date_granularity=dateGranularity,startDate=startDate,endDate=endDate)
        downloadDf = downloadDf[['date','downloads','revenues']]

        downloadsRetDf = pd.concat([downloadsRetDf,pd.DataFrame({'appId':[appid]*len(downloadDf),'date':downloadDf['date'],'downloads':downloadDf['downloads']})])
        revenuesRetDf = pd.concat([revenuesRetDf,pd.DataFrame({'appId':[appid]*len(downloadDf),'date':downloadDf['date'],'revenues':downloadDf['revenues']})])

    downloadsRetDf = downloadsRetDf.sort_values(by=['appId','date'],ascending=False)
    revenuesRetDf = revenuesRetDf.sort_values(by=['appId','date'],ascending=False)

    return downloadsRetDf,revenuesRetDf
    
# 从getAPPsDownloadAndRevenue的结果中，获取相关性
# appDf 是 目标app的下载量数据
def getAPPsCorrelationFromDF(df,appDf,colName = 'downloads',corr = ''):
    appIdList = df['appId'].unique().tolist()
    selfAppId = appDf['appId'].unique().tolist()[0]
    if selfAppId in appIdList:
        appIdList.remove(selfAppId)

    df0 = appDf[['date',colName]]
    sum0 = appDf[colName].sum()

    retDf = pd.DataFrame(columns=['appId',colName,'correlation'])
    # retDf = pd.concat([retDf,pd.DataFrame({'appId':[selfAppId],colName:[sum0],'correlation':[1]})])
    for appid in appIdList:
        df1 = df[df['appId'] == appid][['date',colName]]
        dfMerge = pd.merge(df0,df1,on='date',how='left',suffixes=('_0','_1'))
        try:
            if corr == 'spearmanr':
                from scipy.stats import spearmanr
                correlation,_ = spearmanr(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
            elif corr == 'kendalltau':
                from scipy.stats import kendalltau
                correlation,_ = kendalltau(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
            elif corr == 'r2':
                from sklearn.preprocessing import MinMaxScaler
                from sklearn.metrics import r2_score
                scaler = MinMaxScaler()
                dfMerge[f'{colName}_0'] = scaler.fit_transform(dfMerge[f'{colName}_0'].values.reshape(-1,1))
                dfMerge[f'{colName}_1'] = scaler.fit_transform(dfMerge[f'{colName}_1'].values.reshape(-1,1))
                # 检查数据中是否包含NaN值或无穷大值，并替换为0
                dfMerge = dfMerge.replace([np.inf, -np.inf], np.nan).fillna(0)
                correlation = r2_score(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
            else:
                # 默认是pearson相关系数
                correlation = dfMerge[f'{colName}_0'].corr(dfMerge[f'{colName}_1'])
        except AttributeError:
            correlation = 0

        sum1 = df[df['appId'] == appid][colName].sum()
        retDf = pd.concat([retDf,pd.DataFrame({'appId':[appid],colName:[sum1],'correlation':[correlation]})])

    retDf = retDf.sort_values(by=colName,ascending=False)
    return retDf
            

# SLG N 指数
# SLG top N 的下载量转化为指数，并与topwar的下载量进行对比
# 以周为单位
def slgTopNRevenuesIndex(appId = 'com.topwar.gp',os = 'android',country = 'US',startDate='',midDate='',endDate='',N=10):
    dateGranularity = 'weekly'
    # 获取topwar的下载量，周为单位，从startDate到endDate
    topwarDf = getDownloadAndRevenue(appId,os=os,countries=country,date_granularity=dateGranularity,startDate=startDate,endDate=endDate)
    topwarDf = topwarDf[['date','revenues']]
    topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    
    topwarDf['index'] = topwarDf['revenues']

    # 获取SLG top N的下载量的和，周为单位，从startDate到endDate
    topAppDf = getTopApp(os=os,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=N,category='all',countries=country,startDate=startDate,endDate=endDate)
    appIdList = topAppDf['appId'].tolist()

    # 获得app名字
    appNameList = []
    for appId in appIdList:
        if os == 'android':
            appName = androidIdToName(appId)
        else:
            appName = iOSIdToName(appId)
        appNameList.append(appName)
    print(f'{os} {country} revenues appNameList:',appNameList)

    slgTopNDf = getDownloadAndRevenueSum(appIdList,os,country,dateGranularity,startDate,endDate)
    slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:10])

    p = topwarDf[topwarDf['date'] <= midDate]['revenues'].sum()/slgTopNDf[slgTopNDf['date'] <= midDate]['revenues'].sum()
    slgTopNDf['index'] = slgTopNDf['revenues'] * p

    # 使用startDate到midDate的下载量的平均值为基准，定为1000
    stdIndex = topwarDf[topwarDf['date'] <= midDate]['index'].mean()
    topwarDf['index'] = topwarDf['index'] / stdIndex * 1000
    slgTopNDf['index'] = slgTopNDf['index'] / stdIndex * 1000

    df = pd.merge(topwarDf,slgTopNDf,on='date',how='left',suffixes=('_topwar','_slg'))
    df = df[['date','index_topwar','index_slg']]
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv(f'/src/data/SLG_revenues_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.csv',index=False)

    # 画
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(df['date'], df['index_topwar'],label='topwar')
    ax1.plot(df['date'], df['index_slg'],label='slg')
    ax1.set_xlabel('date')
    ax1.set_ylabel('index')

    # 添加中间竖线
    midDate_datetime = datetime.strptime(midDate, '%Y-%m-%d')
    plt.axvline(x=midDate_datetime, color='r', linestyle='-', label='Mid Date')

    date_fmt = mdates.DateFormatter('%Y-%m-%d')
    ax1.xaxis.set_major_formatter(date_fmt)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_revenues_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.png'
    plt.savefig(filename)
    print(f'save to {filename}')

    return appIdList
    
def slgTopNDownloadsIndex(appId = 'com.topwar.gp',os = 'android',country = 'US',startDate='',midDate='',endDate='',dateGranularity = 'weekly',N=10):
    # 获取topwar的下载量，周为单位，从startDate到endDate
    topwarDf = getDownloadAndRevenue(appId,os=os,countries=country,date_granularity=dateGranularity,startDate=startDate,endDate=endDate)
    topwarDf = topwarDf[['date','downloads']]
    topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    
    topwarDf['index'] = topwarDf['downloads']

    # 获取SLG top N的下载量的和，周为单位，从startDate到endDate
    topAppDf = getTopApp(os=os,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=N,category='all',countries=country,startDate=startDate,endDate=endDate)
    appIdList = topAppDf['appId'].tolist()

    # 获得app名字
    appNameList = []
    for appId in appIdList:
        if os == 'android':
            appName = androidIdToName(appId)
        else:
            appName = iOSIdToName(appId)
        appNameList.append(appName)
    print(f'{os} {country} downloads appNameList:',appNameList)

    slgTopNDf = getDownloadAndRevenueSum(appIdList,os,country,dateGranularity,startDate,endDate)
    slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:10])

    p = topwarDf[topwarDf['date'] <= midDate]['downloads'].sum()/slgTopNDf[slgTopNDf['date'] <= midDate]['downloads'].sum()
    slgTopNDf['index'] = slgTopNDf['downloads'] * p

    # 使用startDate到midDate的下载量的平均值为基准，定为1000
    stdIndex = topwarDf[topwarDf['date'] <= midDate]['index'].mean()
    topwarDf['index'] = topwarDf['index'] / stdIndex * 1000
    slgTopNDf['index'] = slgTopNDf['index'] / stdIndex * 1000

    df = pd.merge(topwarDf,slgTopNDf,on='date',how='left',suffixes=('_topwar','_slg'))
    df = df[['date','index_topwar','index_slg']]
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv(f'/src/data/SLG_downloads_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.csv',index=False)

    # 画
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(df['date'], df['index_topwar'],label='topwar')
    ax1.plot(df['date'], df['index_slg'],label='slg')
    ax1.set_xlabel('date')
    ax1.set_ylabel('index')

    # 添加中间竖线
    midDate_datetime = datetime.strptime(midDate, '%Y-%m-%d')
    plt.axvline(x=midDate_datetime, color='r', linestyle='-', label='Mid Date')

    date_fmt = mdates.DateFormatter('%Y-%m-%d')
    ax1.xaxis.set_major_formatter(date_fmt)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_downloads_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.png'
    plt.savefig(filename)
    print(f'save to {filename}')

    return appIdList
    


def corrTopNDownloadsIndex(appId = 'com.topwar.gp',os = 'android',country = 'US',startDate='',midDate='',endDate='',N=10):
    dateGranularity = 'weekly'
    filename = f'/src/data/appsDownloadsAndRevenue_{appId}_{os}_{country}_{startDate}_{endDate}.csv'
    if importOs.path.exists(filename):
        downloadsRetDf = pd.read_csv(filename)
    else:
        downloadsRetDf,_ = getAPPsDownloadAndRevenue(os=os,country=country,startDate=startDate,endDate=endDate,limit=500)
        downloadsRetDf.to_csv(filename,index=False)

    topwarDf = getDownloadAndRevenue(appId,os=os,countries=country,date_granularity=dateGranularity,startDate=startDate,endDate=endDate)
    topwarDf['appId'] = appId
    topwarDf['index'] = topwarDf['downloads']
    topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    topwarDf0 = topwarDf[topwarDf['date']<=midDate]

    downloadsRetDf['date'] = downloadsRetDf['date'].apply(lambda x:x[:10])
    downloadsRetDf0 = downloadsRetDf[downloadsRetDf['date']<=midDate]
    

    appsDownloadsCorrDf = getAPPsCorrelationFromDF(downloadsRetDf0,topwarDf0,colName = 'downloads',corr = 'spearmanr')
    # appsDownloadsCorrDf 过滤掉下载数量过少的app
    appsDownloadsCorrDf = appsDownloadsCorrDf[
        (appsDownloadsCorrDf['correlation'] > 0.5) &
        (appsDownloadsCorrDf['downloads'] > 10000)
    ]
    # print(appsDownloadsCorrDf.sort_values(by='correlation',ascending=False))
    appIdList = appsDownloadsCorrDf.sort_values(by='correlation',ascending=False)['appId'].tolist()[:N]

    print(f'corrTopNDownloadsIndex: {appIdList}')
    slgTopNDf = getDownloadAndRevenueSum(appIdList,os,country,dateGranularity,startDate,endDate)
    slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:10])
    
    p = topwarDf[topwarDf['date'] <= midDate]['downloads'].sum()/slgTopNDf[slgTopNDf['date'] <= midDate]['downloads'].sum()
    slgTopNDf['index'] = slgTopNDf['downloads'] * p

    # 使用startDate到midDate的下载量的平均值为基准，定为1000
    stdIndex = topwarDf[topwarDf['date'] <= midDate]['index'].mean()
    topwarDf['index'] = topwarDf['index'] / stdIndex * 1000
    slgTopNDf['index'] = slgTopNDf['index'] / stdIndex * 1000
    # print(slgTopNDf)

    df = pd.merge(topwarDf,slgTopNDf,on='date',how='left',suffixes=('_topwar','_slg'))
    df = df[['date','index_topwar','index_slg']]
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv(f'/src/data/CORR_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.csv',index=False)

    # 画
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(df['date'], df['index_topwar'],label='topwar')
    ax1.plot(df['date'], df['index_slg'],label='other apps')
    ax1.set_xlabel('date')
    ax1.set_ylabel('index')

    # 添加中间竖线
    midDate_datetime = datetime.strptime(midDate, '%Y-%m-%d')
    plt.axvline(x=midDate_datetime, color='r', linestyle='-', label='Mid Date')

    date_fmt = mdates.DateFormatter('%Y-%m-%d')
    ax1.xaxis.set_major_formatter(date_fmt)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/CORR_{appId}_{os}_{country}_{startDate}_{midDate}_{endDate}_{N}.png'
    plt.savefig(filename)
    print(f'save to {filename}')

    return

def slg():
    slgTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    slgTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    slgTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

def corr():
    corrTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    corrTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    corrTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=10)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=20)
    corrTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate='2023-06-01',midDate='2023-12-31',endDate='2024-02-20',N=50)

def slg2():
    startDate = '2023-06-01'
    midDate = '2023-12-31'
    
    # 计算在今天之前的最近的周日
    today = datetime.now()
    days_since_last_sunday = today.weekday() + 1
    last_sunday = today - timedelta(days=days_since_last_sunday)
    # 由于sensor tower的数据滞后，所以需要往前推一周，TODO：等等看什么时候可以更新出新一周的数据
    last_sunday = last_sunday - timedelta(weeks=1)
    endDate = last_sunday.strftime('%Y-%m-%d')

    print(f'startDate: {startDate}, midDate: {midDate}, endDate: {endDate}')

    slgTopNDownloadsIndex(country = 'US',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNDownloadsIndex(country = 'KR',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNDownloadsIndex(country = 'JP',startDate=startDate,midDate=midDate,endDate=endDate,N=20)

    slgTopNRevenuesIndex(country = 'US',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNRevenuesIndex(country = 'KR',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNRevenuesIndex(country = 'JP',startDate=startDate,midDate=midDate,endDate=endDate,N=20)

    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'US',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'KR',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNDownloadsIndex(os = 'ios',appId='1479198816',country = 'JP',startDate=startDate,midDate=midDate,endDate=endDate,N=20)

    slgTopNRevenuesIndex(os = 'ios',appId='1479198816',country = 'US',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNRevenuesIndex(os = 'ios',appId='1479198816',country = 'KR',startDate=startDate,midDate=midDate,endDate=endDate,N=20)
    slgTopNRevenuesIndex(os = 'ios',appId='1479198816',country = 'JP',startDate=startDate,midDate=midDate,endDate=endDate,N=20)

# topwar长期指数，用2021年全年作为基准，单位是月
# 指数统计 topwar所有时间的指数，目前2019年到2023年
def slg3():
    stdStartDate = '2021-01-01'
    stdEndDate = '2021-12-31'

    indexStartDate = '2019-01-01'
    indexEndDate = '2023-12-31'

    # 获取stdStartDate到stdEndDate的Top 20 SLG id List
    

if __name__ == '__main__':
    # slg()
    # corr()
    slg2()
    