# 海外iOS 数据对比
# sensortower其他数值比对，主要包括：安装、流水、dau、留存等。用于认识sensortower数据准确性，便于之后竞品数值参考。
# topwar & lastwar

import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_percentage_error, r2_score
import matplotlib.dates as mdates

import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.sensortower.intel import getDownloadAndRevenue,getRetention,getActiveUsers


def getLwDownloadAndRevenueAndDAUData():
    lastwarIosId = '6448786147'

    stDAUDf = getActiveUsers(app_ids=[lastwarIosId],platform='ios',countries='WW',time_period='day',start_date='2024-01-01',end_date='2024-05-20')
    stDAUDf.rename(columns=
        {
            'date':'Date',
            'users':'st DAU'
        }, inplace=True)
    stDAUDf['Date'] = stDAUDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDAUDf = stDAUDf[['Date','st DAU']]

    stDownloadsAndRevenueDf = getDownloadAndRevenue(lastwarIosId,os = 'ios',countries='WW',startDate='2024-01-01',endDate='2024-05-20')
    stDownloadsAndRevenueDf.rename(columns=
        {
            'date':'Date',
            'downloads':'st downloads',
            'revenues':'st revenues'
        }, inplace=True)
    # Date 是类似“2024-01-04T00:00:00Z”的字符串，转为类似“20240104”的字符串
    stDownloadsAndRevenueDf['Date'] = stDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDownloadsAndRevenueDf = stDownloadsAndRevenueDf[['Date','st downloads','st revenues']]

    ssDownloadsAndRevenueDf = pd.read_csv('lwDownloadsAndRevenue.csv')
    ssDownloadsAndRevenueDf.rename(columns=
        {
            '日期':'Date',
            'DAU':'thinkData DAU',
            '流水':'thinkData revenue'
        }, inplace=True)
    # Date 是类似“2024-01-01(一)”的字符串，转为类似“20240101”的字符串
    ssDownloadsAndRevenueDf['Date'] = ssDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    ssDownloadsAndRevenueDf['thinkData DAU'] = ssDownloadsAndRevenueDf['thinkData DAU'].apply(lambda x: int(x.replace(',','')))
    ssDownloadsAndRevenueDf['thinkData revenue'] = ssDownloadsAndRevenueDf['thinkData revenue'].apply(lambda x: float(x.replace(',','')))

    biInstallDf = pd.read_csv('lwBiInstall.csv',dtype={'Date':str})

    df = pd.merge(ssDownloadsAndRevenueDf, biInstallDf, on='Date', how='left')
    df = pd.merge(df, stDownloadsAndRevenueDf, on='Date', how='left')
    df = pd.merge(df, stDAUDf, on='Date', how='left')


    print(df)
    df.to_csv('/src/data/getLwDownloadAndRevenueAndDAUData.csv',index=False)

def getLwDownloadAndRevenueAndDAUDataAndroid():
    lastwarAndroidId = 'com.fun.lastwar.gp'

    stDAUDf = getActiveUsers(app_ids=[lastwarAndroidId],platform='android',countries='WW',time_period='day',start_date='2024-01-01',end_date='2024-05-20')
    stDAUDf.rename(columns=
        {
            'date':'Date',
            'users':'st DAU'
        }, inplace=True)
    stDAUDf['Date'] = stDAUDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDAUDf = stDAUDf[['Date','st DAU']]

    stDownloadsAndRevenueDf = getDownloadAndRevenue(lastwarAndroidId,os = 'android',countries='WW',startDate='2024-01-01',endDate='2024-05-20')
    stDownloadsAndRevenueDf.rename(columns=
        {
            'date':'Date',
            'downloads':'st downloads',
            'revenues':'st revenues'
        }, inplace=True)
    # Date 是类似“2024-01-04T00:00:00Z”的字符串，转为类似“20240104”的字符串
    stDownloadsAndRevenueDf['Date'] = stDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDownloadsAndRevenueDf = stDownloadsAndRevenueDf[['Date','st downloads','st revenues']]
    print(stDownloadsAndRevenueDf)

    ssDownloadsAndRevenueDf = pd.read_csv('lwDownloadsAndRevenueAndroid.csv')
    ssDownloadsAndRevenueDf.rename(columns=
        {
            '日期':'Date',
            'DAU':'thinkData DAU',
            '流水':'thinkData revenue'
        }, inplace=True)
    
    # Date 是类似“2024-01-01(一)”的字符串，转为类似“20240101”的字符串
    ssDownloadsAndRevenueDf['Date'] = ssDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    ssDownloadsAndRevenueDf['thinkData DAU'] = ssDownloadsAndRevenueDf['thinkData DAU'].apply(lambda x: int(x.replace(',','')))
    ssDownloadsAndRevenueDf['thinkData revenue'] = ssDownloadsAndRevenueDf['thinkData revenue'].apply(lambda x: float(x.replace(',','')))
    print(ssDownloadsAndRevenueDf)

    biInstallDf = pd.read_csv('lwBiInstallAndroid.csv',dtype={'Date':str})

    df = pd.merge(ssDownloadsAndRevenueDf, biInstallDf, on='Date', how='left')
    df = pd.merge(df, stDownloadsAndRevenueDf, on='Date', how='left')
    df = pd.merge(df, stDAUDf, on='Date', how='left')

    print(df)
    df.to_csv('/src/data/getLwDownloadAndRevenueAndDAUDataAndroid.csv',index=False)

def debug():
    filename = '/src/data/getTwDownloadAndRevenueAndDAUDataAndroid.csv'
    df = pd.read_csv(filename, dtype={'Date':str})
    print(df.corr())



def lwDownloadAndRevenueAndDAU():
    # filename = '/src/data/getLwDownloadAndRevenueAndDAUData.csv'
    filename = '/src/data/getLwDownloadAndRevenueAndDAUDataAndroid.csv'
    if os.path.exists(filename) == False:
        getLwDownloadAndRevenueAndDAUData()

    df = pd.read_csv(filename, dtype={'Date':str})
    df = df.loc[df['Date'] <= '20240520']

    df['st revenues'] = df['st revenues']/100
    # # 打印所有列
    # print(df.columns)
    # 'Date', 'thinkData DAU', 'thinkData revenue', 'installs','st downloads', 'st revenues', 'st DAU'

    # 用Date作为索引（x轴），分别针对 'thinkData DAU' vs 'st DAU'，'thinkData revenue' vs 'st revenues'，'installs' vs 'st downloads' 
    # 进行对比，计算MAPE，线性相关系数，
    # 并绘制折线图，3张图纵向画在一张图上，保存到 /src/data/20240523_lwDownloadAndRevenueAndDAU.png
    # Set Date as index
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df.set_index('Date', inplace=True)

    # Define the columns to compare
    compare_columns = [
        ('thinkData DAU', 'st DAU'),
        ('thinkData revenue', 'st revenues'),
        ('installs', 'st downloads')
    ]

    # Create a figure with 3 subplots
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))

    # Loop through the columns and calculate MAPE and R2 score, then plot the data
    for i, (col1, col2) in enumerate(compare_columns):
        mape = mean_absolute_percentage_error(df[col1], df[col2])
        r2 = r2_score(df[col1], df[col2])

        axs[i].plot(df[col1], label=col1)
        axs[i].plot(df[col2], label=col2)
        axs[i].set_title(f"{col1} vs {col2} - MAPE: {mape:.2f}, R2: {r2:.2f}")
        axs[i].legend()

        axs[i].xaxis.set_major_locator(mdates.DayLocator(interval=7))
        axs[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(axs[i].get_xticklabels(), rotation=30, ha="right")

    # Save the figure to the specified path
    plt.tight_layout(h_pad=4.0)
    plt.savefig('/src/data/20240523_lwDownloadAndRevenueAndDAU.png')
    print('saved to /src/data/20240523_lwDownloadAndRevenueAndDAU.png')

def getTwDownloadAndRevenueAndDAUData():
    topwarIosId = '1479198816'

    stDAUDf = getActiveUsers(app_ids=[topwarIosId],platform='ios',countries='WW',time_period='day',start_date='2024-01-01',end_date='2024-05-20')
    stDAUDf.rename(columns=
        {
            'date':'Date',
            'users':'st DAU'
        }, inplace=True)
    stDAUDf['Date'] = stDAUDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDAUDf = stDAUDf[['Date','st DAU']]

    stDownloadsAndRevenueDf = getDownloadAndRevenue(topwarIosId,os = 'ios',countries='WW',startDate='2024-01-01',endDate='2024-05-20')
    stDownloadsAndRevenueDf.rename(columns=
        {
            'date':'Date',
            'downloads':'st downloads',
            'revenues':'st revenues'
        }, inplace=True)
    # Date 是类似“2024-01-04T00:00:00Z”的字符串，转为类似“20240104”的字符串
    stDownloadsAndRevenueDf['Date'] = stDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDownloadsAndRevenueDf = stDownloadsAndRevenueDf[['Date','st downloads','st revenues']]

    ssDownloadsAndRevenueDf = pd.read_csv('twDownloadsAndRevenue.csv')
    ssDownloadsAndRevenueDf.rename(columns=
        {
            '日期':'Date',
            'DAU':'thinkData DAU',
            '流水':'thinkData revenue'
        }, inplace=True)
    # Date 是类似“2024-01-01(一)”的字符串，转为类似“20240101”的字符串
    ssDownloadsAndRevenueDf['Date'] = ssDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    ssDownloadsAndRevenueDf['thinkData DAU'] = ssDownloadsAndRevenueDf['thinkData DAU'].apply(lambda x: int(x.replace(',','')))
    ssDownloadsAndRevenueDf['thinkData revenue'] = ssDownloadsAndRevenueDf['thinkData revenue'].apply(lambda x: float(x.replace(',','')))

    biInstallDf = pd.read_csv('twBiInstall.csv',dtype={'Date':str})

    df = pd.merge(ssDownloadsAndRevenueDf, biInstallDf, on='Date', how='left')
    df = pd.merge(df, stDownloadsAndRevenueDf, on='Date', how='left')
    df = pd.merge(df, stDAUDf, on='Date', how='left')


    print(df)
    df.to_csv('/src/data/getTwDownloadAndRevenueAndDAUData.csv',index=False)

def twDownloadAndRevenueAndDAU():
    filename = '/src/data/getTwDownloadAndRevenueAndDAUData.csv'
    
    if os.path.exists(filename) == False:
        getTwDownloadAndRevenueAndDAUData()

    df = pd.read_csv(filename, dtype={'Date':str})
    df = df.loc[df['Date'] <= '20240520']

    df['st revenues'] = df['st revenues']/100
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df.set_index('Date', inplace=True)

    # Define the columns to compare
    compare_columns = [
        ('thinkData DAU', 'st DAU'),
        ('thinkData revenue', 'st revenues'),
        ('installs', 'st downloads')
    ]

    # Create a figure with 3 subplots
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))

    # Loop through the columns and calculate MAPE and R2 score, then plot the data
    for i, (col1, col2) in enumerate(compare_columns):
        mape = mean_absolute_percentage_error(df[col1], df[col2])
        r2 = r2_score(df[col1], df[col2])

        axs[i].plot(df[col1], label=col1)
        axs[i].plot(df[col2], label=col2)
        axs[i].set_title(f"{col1} vs {col2} - MAPE: {mape:.2f}, R2: {r2:.2f}")
        axs[i].legend()

        axs[i].xaxis.set_major_locator(mdates.DayLocator(interval=7))
        axs[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(axs[i].get_xticklabels(), rotation=30, ha="right")

    # Save the figure to the specified path
    plt.tight_layout(h_pad=4.0)
    plt.savefig('/src/data/20240523_twDownloadAndRevenueAndDAU.png')
    print('saved to /src/data/20240523_twDownloadAndRevenueAndDAU.png')

def getTwDownloadAndRevenueAndDAUDataAndroid():
    topwarIosId = 'com.topwar.gp'

    stDAUDf = getActiveUsers(app_ids=[topwarIosId],platform='android',countries='WW',time_period='day',start_date='2024-01-01',end_date='2024-05-20')
    stDAUDf.rename(columns=
        {
            'date':'Date',
            'users':'st DAU'
        }, inplace=True)
    stDAUDf['Date'] = stDAUDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDAUDf = stDAUDf[['Date','st DAU']]

    stDownloadsAndRevenueDf = getDownloadAndRevenue(topwarIosId,os = 'android',countries='WW',startDate='2024-01-01',endDate='2024-05-20')
    stDownloadsAndRevenueDf.rename(columns=
        {
            'date':'Date',
            'downloads':'st downloads',
            'revenues':'st revenues'
        }, inplace=True)
    # Date 是类似“2024-01-04T00:00:00Z”的字符串，转为类似“20240104”的字符串
    stDownloadsAndRevenueDf['Date'] = stDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    stDownloadsAndRevenueDf = stDownloadsAndRevenueDf[['Date','st downloads','st revenues']]

    ssDownloadsAndRevenueDf = pd.read_csv('twDownloadsAndRevenueAndroid.csv')
    ssDownloadsAndRevenueDf.rename(columns=
        {
            '日期':'Date',
            'DAU':'thinkData DAU',
            '流水':'thinkData revenue'
        }, inplace=True)
    # Date 是类似“2024-01-01(一)”的字符串，转为类似“20240101”的字符串
    ssDownloadsAndRevenueDf['Date'] = ssDownloadsAndRevenueDf['Date'].apply(lambda x: x[:10].replace('-',''))
    ssDownloadsAndRevenueDf['thinkData DAU'] = ssDownloadsAndRevenueDf['thinkData DAU'].apply(lambda x: int(x.replace(',','')))
    ssDownloadsAndRevenueDf['thinkData revenue'] = ssDownloadsAndRevenueDf['thinkData revenue'].apply(lambda x: float(x.replace(',','')))

    biInstallDf = pd.read_csv('twBiInstallAndroid.csv',dtype={'Date':str})

    df = pd.merge(ssDownloadsAndRevenueDf, biInstallDf, on='Date', how='left')
    df = pd.merge(df, stDownloadsAndRevenueDf, on='Date', how='left')
    df = pd.merge(df, stDAUDf, on='Date', how='left')


    print(df)
    df.to_csv('/src/data/getTwDownloadAndRevenueAndDAUDataAndroid.csv',index=False)

def twDownloadAndRevenueAndDAUAndroid():
    filename = '/src/data/getTwDownloadAndRevenueAndDAUDataAndroid.csv'
    
    if os.path.exists(filename) == False:
        getTwDownloadAndRevenueAndDAUDataAndroid()

    df = pd.read_csv(filename, dtype={'Date':str})
    df = df.loc[df['Date'] <= '20240520']

    df['st revenues'] = df['st revenues']/100
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df.set_index('Date', inplace=True)

    # Define the columns to compare
    compare_columns = [
        ('thinkData DAU', 'st DAU'),
        ('thinkData revenue', 'st revenues'),
        ('installs', 'st downloads')
    ]

    # Create a figure with 3 subplots
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))

    # Loop through the columns and calculate MAPE and R2 score, then plot the data
    for i, (col1, col2) in enumerate(compare_columns):
        mape = mean_absolute_percentage_error(df[col1], df[col2])
        r2 = r2_score(df[col1], df[col2])

        axs[i].plot(df[col1], label=col1)
        axs[i].plot(df[col2], label=col2)
        axs[i].set_title(f"{col1} vs {col2} - MAPE: {mape:.2f}, R2: {r2:.2f}")
        axs[i].legend()

        axs[i].xaxis.set_major_locator(mdates.DayLocator(interval=7))
        axs[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(axs[i].get_xticklabels(), rotation=30, ha="right")

    # Save the figure to the specified path
    plt.tight_layout(h_pad=4.0)
    plt.savefig('/src/data/20240523_twDownloadAndRevenueAndDAUAndroid.png')
    print('saved to /src/data/20240523_twDownloadAndRevenueAndDAUAndroid.png')

def lwRetention():
    retentionDf = getRetention(app_ids=['6448786147'],platform='ios',start_date='2024-01-01',end_date='2024-03-31')
    retention = retentionDf[0]['retention']
    # print(retention)
    retentionDf2 = pd.read_csv('lwRetention.csv')
    # 只需要第2行
    retention2 = retentionDf2.iloc[1].values[4:]
    
    l1 = len(retention)
    l2 = len(retention2)

    l = min(l1,l2)

    retention = retention[:l]
    retention2 = retention2[:l]

    df = pd.DataFrame({
        'retention thinkData':retention,
        'retention sensortower':retention2
    })

    # 将retention sensortower列的数据从类似32.59%，转为0.3259
    df['retention sensortower'] = df['retention sensortower'].apply(lambda x: float(x[:-1])/100)
    df['MAPE'] = abs(df['retention thinkData'] - df['retention sensortower']) / df['retention thinkData']

    print('MAPE:',df['MAPE'].mean())

    print(df)
    df.to_csv('/src/data/lwRetention.csv',index=False)

    print(df.corr())

def lwRetentionAndroid():
    retentionDf = getRetention(app_ids=['com.fun.lastwar.gp'],platform='android',start_date='2024-01-01',end_date='2024-03-31')
    retention = retentionDf[0]['retention']
    # print(retention)
    retentionDf2 = pd.read_csv('lwRetentionAndroid.csv')
    # 只需要第2行
    retention2 = retentionDf2.iloc[1].values[4:]
    
    l1 = len(retention)
    l2 = len(retention2)

    l = min(l1,l2)

    retention = retention[:l]
    retention2 = retention2[:l]

    df = pd.DataFrame({
        'retention thinkData':retention,
        'retention sensortower':retention2
    })

    # 将retention sensortower列的数据从类似32.59%，转为0.3259
    df['retention sensortower'] = df['retention sensortower'].apply(lambda x: float(x[:-1])/100)
    df['MAPE'] = abs(df['retention thinkData'] - df['retention sensortower']) / df['retention thinkData']

    print('MAPE:',df['MAPE'].mean())

    # print(df)
    df.to_csv('/src/data/lwRetentionAndroid.csv',index=False)

    print(df.corr())


if __name__ == '__main__':
    
    # lwDownloadAndRevenueAndDAU()
    # twDownloadAndRevenueAndDAU()
    # twDownloadAndRevenueAndDAUAndroid()

    # lwRetention()
    # lwRetentionAndroid()

    debug()
    
