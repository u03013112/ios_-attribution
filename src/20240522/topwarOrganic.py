# iTunes 下载量与BI二次归因下载数总量对比
# iTunes 自然量下载数 与 SKAN下载数对比

import os
import json
import pandas as pd


def getItunesInstallData():
    iTunesDownloadsTotalDf = pd.read_csv('iTunesDownloadsTotal.csv')
    iTunesDownloadsFirstDf = pd.read_csv('iTunesDownloadsFirst.csv')

    iTunesDownloadsTotalDf['Date'] = pd.to_datetime(iTunesDownloadsTotalDf['Date'], format='%m/%d/%y')
    iTunesDownloadsFirstDf['Date'] = pd.to_datetime(iTunesDownloadsFirstDf['Date'], format='%m/%d/%y')
    iTunesDownloadsTotalDf['Date'] = iTunesDownloadsTotalDf['Date'].dt.strftime('%Y%m%d')
    iTunesDownloadsFirstDf['Date'] = iTunesDownloadsFirstDf['Date'].dt.strftime('%Y%m%d')

    iTunesDownloadsTotalDf['paid downloads'] = iTunesDownloadsTotalDf['App Referrer Total Downloads'] + iTunesDownloadsTotalDf['Web Referrer Total Downloads']
    iTunesDownloadsTotalDf['organic downloads'] = iTunesDownloadsTotalDf['App Store Browse Total Downloads'] + iTunesDownloadsTotalDf['App Store Search Total Downloads'] + iTunesDownloadsTotalDf['Unavailable Total Downloads']
    iTunesDownloadsTotalDf['downloads'] = iTunesDownloadsTotalDf['paid downloads'] + iTunesDownloadsTotalDf['organic downloads']
    iTunesDownloadsTotalDf = iTunesDownloadsTotalDf[['Date', 'paid downloads', 'organic downloads','downloads']]

    iTunesDownloadsFirstDf['paid downloads'] = iTunesDownloadsFirstDf['App Referrer First-Time Downloads'] + iTunesDownloadsFirstDf['Web Referrer First-Time Downloads']
    iTunesDownloadsFirstDf['organic downloads'] = iTunesDownloadsFirstDf['App Store Browse First-Time Downloads'] + iTunesDownloadsFirstDf['App Store Search First-Time Downloads'] + iTunesDownloadsFirstDf['Unavailable First-Time Downloads']
    iTunesDownloadsFirstDf['downloads'] = iTunesDownloadsFirstDf['paid downloads'] + iTunesDownloadsFirstDf['organic downloads']
    iTunesDownloadsFirstDf = iTunesDownloadsFirstDf[['Date', 'paid downloads', 'organic downloads','downloads']]

    iTunesDownloadsDf = pd.merge(iTunesDownloadsTotalDf, iTunesDownloadsFirstDf, on='Date', suffixes=('_total', '_first'))
    # iTunesDownloadsDf.to_csv('/src/data/iTunesDownloads.csv', index=False)
    return iTunesDownloadsDf

def getBiData():
    biData = pd.read_csv('topwarBIInstalls.csv', dtype={'Date':str})
    return biData

def getSsotData():
    ssotData = pd.read_csv('twSsot.csv', dtype={'Date':str})
    return ssotData

def getSkanData():
    skanData = pd.read_csv('topwarSkanAF.csv')
    skanData['Date'] = pd.to_datetime(skanData['Date'], format='%Y/%m/%d')
    skanData['Date'] = skanData['Date'].dt.strftime('%Y%m%d')

    skanData['installs'] = skanData['installs'].apply(lambda x: int(x.replace(',','')))
    skanData['installs_new'] = skanData['installs_new'].apply(lambda x: int(x.replace(',','')))

    return skanData

def iTunesVsBi():
    iTunesDownloadsDf = getItunesInstallData()
    biData = getBiData()
    biData.rename(columns={'install':'bi install'}, inplace=True)

    df = pd.merge(iTunesDownloadsDf, biData, on='Date', how='left')
    
    df.to_csv('/src/data/topwar_iTunesVsBi.csv', index=False)
    
    # 按月汇总
    df['Month'] = df['Date'].apply(lambda x: x[:6])
    df = df.groupby('Month').sum().reset_index()
    df = df[['downloads_total','downloads_first','bi install']]
    df['downloads_total/bi install'] = df['downloads_total'] / df['bi install']
    df['downloads_first/bi install'] = df['downloads_first'] / df['bi install']
    df['downloads_total/bi install'] = df['downloads_total/bi install'].apply(lambda x: round(x, 2))
    df['downloads_first/bi install'] = df['downloads_first/bi install'].apply(lambda x: round(x, 2))

    df.to_csv('/src/data/topwar_iTunesVsBi_monthly.csv', index=False)


def iTunesVsSsot():
    iTunesDownloadsDf = getItunesInstallData()
    iTunesDownloadsDf = iTunesDownloadsDf[['Date','organic downloads_first']]
    ssotData = getSsotData()
    ssotData.rename(columns={'install':'ssot install'}, inplace=True)

    df = pd.merge(iTunesDownloadsDf, ssotData, on='Date', how='left')
    df.to_csv('/src/data/topwar_iTunesVsSsot.csv', index=False)

    # 按月汇总
    df['Month'] = df['Date'].apply(lambda x: x[:6])
    df = df.groupby('Month').sum().reset_index()
    df['organic downloads_first/ssot install'] = df['organic downloads_first'] / df['ssot install']

    df.to_csv('/src/data/topwar_iTunesVsSsot_monthly.csv', index=False)

def iTunesVsSkan():
    iTunesDownloadsDf = getItunesInstallData()
    skanData = getSkanData()
    skanData.rename(columns={
        'installs':'skan install',
        'installs_new':'skan install new'
    }, inplace=True)
    biData = getBiData()
    biData.rename(columns={'install':'bi install'}, inplace=True)

    df = pd.merge(iTunesDownloadsDf, skanData, on='Date', how='left')
    df = pd.merge(df, biData, on='Date', how='left')

    df.to_csv('/src/data/topwar_iTunesVsSkan.csv', index=False)

    # 按月汇总
    df['Month'] = df['Date'].apply(lambda x: x[:6])
    df = df.groupby('Month').sum().reset_index()

    df['bi install - skan install'] = df['bi install'] - df['skan install']

    df = df[['Month','organic downloads_first','bi install - skan install']]
    df['organic downloads_first/bi install - skan install'] = df['organic downloads_first'] / df['bi install - skan install']
    df['organic downloads_first/bi install - skan install'] = df['organic downloads_first/bi install - skan install'].apply(lambda x: round(x, 2))

    df.to_csv('/src/data/topwar_iTunesVsSkan_monthly.csv', index=False)

if __name__ == '__main__':
    # getItunesInstallData()
    # print(getSkanData())
    # iTunesVsBi()
    # iTunesVsSkan()
    iTunesVsSsot()
