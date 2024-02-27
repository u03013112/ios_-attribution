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
    

# 添加可变化dateGranularity
# 不再使用minDate，而是改为 stdStartDate, stdEndDate, indexStartDate, indexEndDate
def slgTopNDownloadsIndex2(appId = 'com.topwar.gp',os = 'android',country = 'US',dateGranularity = 'weekly',N=10,downloadsOrRevenue='downloads',stdStartDate='',stdEndDate='',indexStartDate='',indexEndDate=''):
    # 获取topwar的下载量，周为单位，从startDate到endDate
    topwarDf = getDownloadAndRevenue(appId,os=os,countries=country,date_granularity=dateGranularity,startDate=indexStartDate,endDate=indexEndDate)
    topwarDf = topwarDf[['date',downloadsOrRevenue]]
    topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    
    topwarDf['index'] = topwarDf[downloadsOrRevenue]

    # 获取SLG top N的下载量的和，周为单位，从startDate到endDate
    measure = 'units' if downloadsOrRevenue == 'downloads' else 'revenue'
    topAppDf = getTopApp(os=os,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=N,category='all',measure = measure,countries=country,startDate=stdStartDate,endDate=stdEndDate)
    appIdList = topAppDf['appId'].tolist()

    # print(appIdList)
    # 获得app名字
    appNameList = []
    for appId0 in appIdList:
        if os == 'android':
            appName = androidIdToName(appId0)
        else:
            appName = iOSIdToName(appId0)
        appNameList.append(appName)
    print(f'{os} {country} {downloadsOrRevenue} appNameList:',appNameList)

    slgTopNDf = getDownloadAndRevenueSum(appIdList,os,country,dateGranularity,indexStartDate,indexEndDate)
    slgTopNDf = slgTopNDf[['date',downloadsOrRevenue]]
    slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:10])

    stdTopwarDf = topwarDf[(topwarDf['date'] >= stdStartDate) & (topwarDf['date'] <= stdEndDate)]
    stdTopwarDownloadsMean = stdTopwarDf[downloadsOrRevenue].mean()
    topwarDf['index'] = topwarDf[downloadsOrRevenue] / stdTopwarDownloadsMean * 1000

    stdSlgTopNDf = slgTopNDf[(slgTopNDf['date'] >= stdStartDate) & (slgTopNDf['date'] <= stdEndDate)]
    stdSlgTopNDfDownloadsMean = stdSlgTopNDf[downloadsOrRevenue].mean()
    slgTopNDf['index'] = slgTopNDf[downloadsOrRevenue] / stdSlgTopNDfDownloadsMean * 1000

    df = pd.merge(topwarDf,slgTopNDf,on='date',how='left',suffixes=('_topwar','_slg'))
    # df = df[['date',f'{downloadsOrRevenue}_topwar',f'{downloadsOrRevenue}_slg','index_topwar','index_slg']]
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv(f'/src/data/SLG_{downloadsOrRevenue}_{appId}_{os}_{country}_{indexStartDate}_{indexEndDate}_{N}.csv',index=False)

    # 画
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(df['date'], df['index_topwar'],label='topwar')
    ax1.plot(df['date'], df['index_slg'],label='slg')
    ax1.set_xlabel('date')
    ax1.set_ylabel('index')

    # 添加中间竖线
    stdStartDate_datetime = datetime.strptime(stdStartDate, '%Y-%m-%d')
    stdEndDate_datetime = datetime.strptime(stdEndDate, '%Y-%m-%d')
    plt.axvline(x=stdStartDate_datetime, color='r', linestyle='-', label='std start date')
    plt.axvline(x=stdEndDate_datetime, color='r', linestyle='-', label='std end Date')

    date_fmt = mdates.DateFormatter('%Y-%m-%d')
    ax1.xaxis.set_major_formatter(date_fmt)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_{downloadsOrRevenue}_{appId}_{os}_{country}_{indexStartDate}_{indexEndDate}_{N}.png'
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

def slg3(os = 'android',appId='com.topwar.gp'):

    topwarDf = getDownloadAndRevenue(appId,os=os,countries='WW',date_granularity='monthly',startDate='2021-01-01',endDate='2023-12-31')
    topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    # 取topwar的前12个月，即前12行的数据作为基准
    # topwarDownloadsMean12 = topwarDf['downloads'].head(12).mean()
    # 获取前12个数据
    topwar_downloads_12 = topwarDf['downloads'].head(12)
    # 获取最小值和最大值
    min_value = topwar_downloads_12.nsmallest(1).iloc[0]
    max_value = topwar_downloads_12.nlargest(1).iloc[0]
    # 从数据中移除最小值和最大值
    filtered_downloads = topwar_downloads_12[(topwar_downloads_12 != min_value) & (topwar_downloads_12 != max_value)]
    # 计算剩余数据的均值
    topwarDownloadsMean12 = filtered_downloads.mean()

    topwarDf['downloads index'] = topwarDf['downloads'] / topwarDownloadsMean12 * 1000 
    # topwarRevenueMean12 = topwarDf['revenues'].head(12).mean()
    topwar_revenues_12 = topwarDf['revenues'].head(12)
    min_value = topwar_revenues_12.nsmallest(1).iloc[0]
    max_value = topwar_revenues_12.nlargest(1).iloc[0]
    filtered_revenues = topwar_revenues_12[(topwar_revenues_12 != min_value) & (topwar_revenues_12 != max_value)]
    topwarRevenueMean12 = filtered_revenues.mean()
    topwarDf['revenues index'] = topwarDf['revenues'] / topwarRevenueMean12 * 1000

    topwarDf.drop(columns=['date'],inplace=True)
    topwarDf.rename(columns={
        'downloads':f'topwar downloads',
        'revenues':f'topwar revenues',
        'downloads index':f'topwar downloads index',
        'revenues index':f'topwar revenues index'
        },inplace=True)


    dateList = [
        {'stdStartDate':'2019-01-01','stdEndDate':'2019-12-31','indexStartDate':'2019-01-01','indexEndDate':'2022-12-31'},
        {'stdStartDate':'2020-01-01','stdEndDate':'2020-12-31','indexStartDate':'2020-01-01','indexEndDate':'2023-12-31'},
        {'stdStartDate':'2021-01-01','stdEndDate':'2021-12-31','indexStartDate':'2021-01-01','indexEndDate':'2023-12-31'},
    ]

    for date in dateList:
        downloadsTop20Df = getTopApp(os=os,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=20,category='all',measure = 'units',countries='WW',startDate=date['stdStartDate'],endDate=date['stdEndDate'])
        downloadsTop20AppIdList = downloadsTop20Df['appId'].tolist()

        # 获得app名字
        appNameList = []
        for appId in downloadsTop20AppIdList:
            if os == 'android':
                appName = androidIdToName(appId)
            else:
                appName = iOSIdToName(appId)
            appNameList.append(appName)
        print(f'{date["stdStartDate"]} {os} WW downloads appNameList:',appNameList)

        slgDownloadsTop20Df = getDownloadAndRevenueSum(downloadsTop20AppIdList,os,'WW','monthly',date['indexStartDate'],date['indexEndDate'])
        slgDownloadsTop20Df['date'] = slgDownloadsTop20Df['date'].apply(lambda x:x[:10])
        downloadsSLGTop20Mean12 = slgDownloadsTop20Df['downloads'].head(12).mean()
        slgDownloadsTop20Df['downloads index'] = slgDownloadsTop20Df['downloads'] / downloadsSLGTop20Mean12 * 1000
        slgDownloadsTop20Df = slgDownloadsTop20Df[['date','downloads','downloads index']]
        yearStr = date['stdStartDate'][:4]
        slgDownloadsTop20Df.rename(columns={
            'downloads':f'{yearStr} downloads',
            'downloads index':f'{yearStr} downloads index',
            },inplace=True)
        topwarDf = pd.concat([topwarDf.reset_index(drop=True), slgDownloadsTop20Df.reset_index(drop=True)], axis=1)

        # revenue
        revenuesTop20Df = getTopApp(os=os,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=20,category='all',measure = 'revenue',countries='WW',startDate=date['stdStartDate'],endDate=date['stdEndDate'])
        revenuesTop20AppIdList = revenuesTop20Df['appId'].tolist()

        # 获得app名字
        appNameList = []
        for appId in revenuesTop20AppIdList:
            if os == 'android':
                appName = androidIdToName(appId)
            else:
                appName = iOSIdToName(appId)
            appNameList.append(appName)
        print(f'{date["stdStartDate"]} {os} WW revenues appNameList:',appNameList)

        slgRevenuesTop20Df = getDownloadAndRevenueSum(revenuesTop20AppIdList,os,'WW','monthly',date['indexStartDate'],date['indexEndDate'])
        slgRevenuesTop20Df['date'] = slgRevenuesTop20Df['date'].apply(lambda x:x[:10])
        revenuesSLGTop20Mean12 = slgRevenuesTop20Df['revenues'].head(12).mean()
        slgRevenuesTop20Df['revenues index'] = slgRevenuesTop20Df['revenues'] / revenuesSLGTop20Mean12 * 1000
        slgRevenuesTop20Df = slgRevenuesTop20Df[['date','revenues','revenues index']]
        slgRevenuesTop20Df.rename(columns={
            'revenues':f'{yearStr} revenues',
            'revenues index':f'{yearStr} revenues index',
            },inplace=True)
        topwarDf = pd.concat([topwarDf.reset_index(drop=True), slgRevenuesTop20Df.reset_index(drop=True)], axis=1)

    # 为topwarDf添加索引，就用行号做索引
    topwarDf.index = range(1,len(topwarDf)+1)

    topwarDf.to_csv(f'/src/data/SLG_topwar_{os}_WW_2019_2023.csv',index=False)
    print(f'save to /src/data/SLG_topwar_{os}_WW_2019_2023.csv')

    # 画图
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(topwarDf.index, topwarDf['topwar downloads index'],label='topwar downloads index')
    ax1.plot(topwarDf.index, topwarDf['2019 downloads index'],label='2019 downloads index')
    ax1.plot(topwarDf.index, topwarDf['2020 downloads index'],label='2020 downloads index')
    ax1.plot(topwarDf.index, topwarDf['2021 downloads index'],label='2021 downloads index')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(topwarDf.index)
    plt.tight_layout()
    plt.legend()
    
    filename = f'/src/data/SLG_topwar_{os}_WW_2019_2023_downloads.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()

    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(topwarDf.index, topwarDf['topwar revenues index'],label='topwar revenues index')
    ax1.plot(topwarDf.index, topwarDf['2019 revenues index'],label='2019 revenues index')
    ax1.plot(topwarDf.index, topwarDf['2020 revenues index'],label='2020 revenues index')
    ax1.plot(topwarDf.index, topwarDf['2021 revenues index'],label='2021 revenues index')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(topwarDf.index)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_topwar_{os}_WW_2019_2023_revenues.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()

def slg3p():
    iosDf = pd.read_csv('/src/data/SLG_topwar_ios_WW_2019_2023.csv')
    androidDf = pd.read_csv('/src/data/SLG_topwar_android_WW_2019_2023.csv')

    N = 3

    androidDf['topwar downloads index rolling3'] = androidDf['topwar downloads index'].rolling(window=N).mean()
    androidDf['topwar revenues index rolling3'] = androidDf['topwar revenues index'].rolling(window=N).mean()
    androidDf['2019 downloads index rolling3'] = androidDf['2019 downloads index'].rolling(window=N).mean()
    androidDf['2019 revenues index rolling3'] = androidDf['2019 revenues index'].rolling(window=N).mean()
    androidDf['2020 downloads index rolling3'] = androidDf['2020 downloads index'].rolling(window=N).mean()
    androidDf['2020 revenues index rolling3'] = androidDf['2020 revenues index'].rolling(window=N).mean()
    androidDf['2021 downloads index rolling3'] = androidDf['2021 downloads index'].rolling(window=N).mean()
    androidDf['2021 revenues index rolling3'] = androidDf['2021 revenues index'].rolling(window=N).mean()
    
    # 画图
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(androidDf.index, androidDf['topwar downloads index rolling3'],label='topwar downloads index rolling3')
    ax1.plot(androidDf.index, androidDf['2019 downloads index rolling3'],label='2019 downloads index rolling3')
    ax1.plot(androidDf.index, androidDf['2020 downloads index rolling3'],label='2020 downloads index rolling3')
    ax1.plot(androidDf.index, androidDf['2021 downloads index rolling3'],label='2021 downloads index rolling3')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(androidDf.index)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_topwar_android_WW_2019_2023_downloads_rolling3.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()

    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(androidDf.index, androidDf['topwar revenues index rolling3'],label='topwar revenues index rolling3')
    ax1.plot(androidDf.index, androidDf['2019 revenues index rolling3'],label='2019 revenues index rolling3')
    ax1.plot(androidDf.index, androidDf['2020 revenues index rolling3'],label='2020 revenues index rolling3')
    ax1.plot(androidDf.index, androidDf['2021 revenues index rolling3'],label='2021 revenues index rolling3')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(androidDf.index)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_topwar_android_WW_2019_2023_revenues_rolling3.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()

    iosDf['topwar downloads index rolling3'] = iosDf['topwar downloads index'].rolling(window=N).mean()
    iosDf['topwar revenues index rolling3'] = iosDf['topwar revenues index'].rolling(window=N).mean()
    iosDf['2019 downloads index rolling3'] = iosDf['2019 downloads index'].rolling(window=N).mean()
    iosDf['2019 revenues index rolling3'] = iosDf['2019 revenues index'].rolling(window=N).mean()
    iosDf['2020 downloads index rolling3'] = iosDf['2020 downloads index'].rolling(window=N).mean()
    iosDf['2020 revenues index rolling3'] = iosDf['2020 revenues index'].rolling(window=N).mean()
    iosDf['2021 downloads index rolling3'] = iosDf['2021 downloads index'].rolling(window=N).mean()
    iosDf['2021 revenues index rolling3'] = iosDf['2021 revenues index'].rolling(window=N).mean()

    # 画图
    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(iosDf.index, iosDf['topwar downloads index rolling3'],label='topwar downloads index rolling3')
    ax1.plot(iosDf.index, iosDf['2019 downloads index rolling3'],label='2019 downloads index rolling3')
    ax1.plot(iosDf.index, iosDf['2020 downloads index rolling3'],label='2020 downloads index rolling3')
    ax1.plot(iosDf.index, iosDf['2021 downloads index rolling3'],label='2021 downloads index rolling3')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(iosDf.index)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_topwar_ios_WW_2019_2023_downloads_rolling3.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()

    fig, ax1 = plt.subplots(figsize=(16, 5))
    ax1.plot(iosDf.index, iosDf['topwar revenues index rolling3'],label='topwar revenues index rolling3')
    ax1.plot(iosDf.index, iosDf['2019 revenues index rolling3'],label='2019 revenues index rolling3')
    ax1.plot(iosDf.index, iosDf['2020 revenues index rolling3'],label='2020 revenues index rolling3')
    ax1.plot(iosDf.index, iosDf['2021 revenues index rolling3'],label='2021 revenues index rolling3')

    ax1.set_xlabel('months count')
    ax1.set_ylabel('index')
    ax1.set_xticks(iosDf.index)
    plt.tight_layout()
    plt.legend()

    filename = f'/src/data/SLG_topwar_ios_WW_2019_2023_revenues_rolling3.png'
    plt.savefig(filename)
    print(f'save to {filename}')
    plt.close()





if __name__ == '__main__':
    # slg()
    # corr()
    # slg2()
    # slg3()
    # slg3('ios',appId='1479198816')

    slg3p()
    