# 自然量对数

import os
import json
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def convert_date(date_str):
    # 使用'/'分割字符串
    year, month, day = date_str.split('/')
    # 格式化年、月、日
    formatted_date = f"{year.zfill(4)}{month.zfill(2)}{day.zfill(2)}"
    return formatted_date

def getItunesInstallData():
    iTunesDownloadsTotalDf = pd.read_csv('iTunesDownloadsTotal.csv')
    iTunesDownloadsFirstDf = pd.read_csv('iTunesDownloadsFirst.csv')

    # Date列是类似 “2024/1/1” 的字符串，需要转换为 “20240101” 的字符串
    iTunesDownloadsTotalDf['Date'] = iTunesDownloadsTotalDf['Date'].apply(convert_date)
    iTunesDownloadsFirstDf['Date'] = iTunesDownloadsFirstDf['Date'].apply(convert_date)

    iTunesDownloadsDf = pd.merge(iTunesDownloadsTotalDf, iTunesDownloadsFirstDf, on='Date', how='left', suffixes=(' total', ' first'))

    return iTunesDownloadsDf

def getSensorTowerInstallData():
    jsonFile = 'downloadsBySources.json'
    with open(jsonFile, 'r') as f:
        jsonData = json.load(f)
    
    DateList = []
    OrganicList = []
    BrowserList = []
    PaidList = []

    breakdownList = jsonData['data'][0]['breakdown']
    for breakdown in breakdownList:
        date = breakdown['date']
        organic_abs = breakdown['organic_abs']
        browser_abs = breakdown['browser_abs']
        paid_abs = breakdown['paid_abs']
        # print(f"Date: {date}, Organic: {organic_abs}, Browser: {browser_abs}, Paid: {paid_abs}")
        DateList.append(date)
        OrganicList.append(organic_abs)
        BrowserList.append(browser_abs)
        PaidList.append(paid_abs)

    sensorTowerDf = pd.DataFrame({
        'Date': DateList,
        'Organic': OrganicList,
        'Browser': BrowserList,
        'Paid': PaidList
    })

    # Date列是类似 “2024-01-01” 的字符串，需要转换为 “20240101” 的字符串
    sensorTowerDf['Date'] = sensorTowerDf['Date'].apply(lambda x: x.replace('-', ''))

    return sensorTowerDf

def getBIData():
    biData = pd.read_csv('biData.csv', dtype={'Date':str})
    return biData

def getSSData():
    ssData = pd.read_csv('ssData.csv')
    ssData.rename(columns={
        '时间':'Date',
        'app_launch.总次数':'first_launch_total',
    }, inplace=True)
    ssData = ssData[['Date', 'first_launch_total']]
    # Date 类似 2024-01-01 的字符串，转为类似 20240101 的字符串
    ssData['Date'] = ssData['Date'].apply(lambda x: x.replace('-', ''))
    # 将first_launch_total转为int类型
    ssData['first_launch_total'] = ssData['first_launch_total'].astype(int)

    return ssData

# 获得融合归因数据
def getMergeData():
    mergeDataTotal = pd.read_csv('mergeDataTotal.csv', dtype={'Date':str})
    mergeDataTotal['installs'] = mergeDataTotal['installs'].apply(lambda x: int(x.replace(',','')))

    mergeDataOrganic = pd.read_csv('mergeDataOrganic.csv', dtype={'Date':str})
    mergeDataOrganic['installs'] = mergeDataOrganic['installs'].apply(lambda x: int(x.replace(',','')))

    mergeDataDf = pd.merge(mergeDataTotal, mergeDataOrganic, on='Date', how='left', suffixes=(' total', ' organic'))
    return mergeDataDf

# iTunes的安装数值和数数中首次激活设备数对比。
def iTunesVsSS():
    iTunesData = getItunesInstallData()
    SSData = getSSData()

    # total 是所有 以‘ total’结尾的列的数据的和
    iTunesData['total sum'] = iTunesData.filter(like=' total').sum(axis=1)
    iTunesData['first sum'] = iTunesData.filter(like=' first').sum(axis=1)

    df = pd.merge(iTunesData, SSData, on='Date', how='left')
    
    # # 额外添加一行，Date列写SUM
    # sumRow = df.sum()
    # sumRow['Date'] = 'SUM'
    # df = df.append(sumRow, ignore_index=True)

    # 得到结论，iTunes的全部安装数值和数数中首次激活设备数数值接近，差距在6%左右。
    # df.to_csv('/src/data/iTunesVsSS.csv', index=False)

    # 按月进行汇总
    df['Month'] = df['Date'].apply(lambda x: x[:6])
    df = df.groupby('Month').agg({
        'total sum': 'sum',
        'first sum': 'sum',
        'first_launch_total': 'sum'
    }).reset_index()
    df['total sum / first_launch_total'] = df['total sum'] / df['first_launch_total']
    df.to_csv('/src/data/iTunesVsSS2.csv', index=False)

# iTunes的安装数值和sensor tower的安装数值对比。
def iTunesVsSensorTower():
    iTunesData = getItunesInstallData()
    sensorTowerData = getSensorTowerInstallData()

    # 按月进行汇总
    iTunesData['Month'] = iTunesData['Date'].apply(lambda x: x[:6])
    iTunesMonthData = iTunesData.groupby('Month').sum().reset_index()
    sensorTowerData['Month'] = sensorTowerData['Date'].apply(lambda x: x[:6])
    df = pd.merge(iTunesMonthData, sensorTowerData, on='Month', how='left')
    
    df['Paid total'] = df['APP 引荐来源 total']
    df['Browser total'] = df['网页引荐来源 total']
    df['Organic total'] = df['App Store浏览 total'] + df['App Store搜索 total'] + df['未知来源 total']

    df['Paid first'] = df['APP 引荐来源 first']
    df['Browser first'] = df['网页引荐来源 first']
    df['Organic first'] = df['App Store浏览 first'] + df['App Store搜索 first'] + df['未知来源 first']
    
    df.rename(columns={
        'Paid':'Paid st',
        'Browser':'Browser st',
        'Organic':'Organic st'
    }, inplace=True)

    df = df[['Month','Paid total','Browser total','Organic total','Paid first','Browser first','Organic first','Paid st','Browser st','Organic st']]
    df['itunes installs total'] = df['Paid total'] + df['Browser total'] + df['Organic total']
    df['itunes installs first'] = df['Paid first'] + df['Browser first'] + df['Organic first']
    df['st installs'] = df['Paid st'] + df['Browser st'] + df['Organic st']


    df['Paid total / Paid st'] = df['Paid total'] / df['Paid st']
    df['Paid first / Paid st'] = df['Paid first'] / df['Paid st']
    df['Browser total / Browser st'] = df['Browser total'] / df['Browser st']
    df['Browser first / Browser st'] = df['Browser first'] / df['Browser st']
    df['Organic total / Organic st'] = df['Organic total'] / df['Organic st']
    df['Organic first / Organic st'] = df['Organic first'] / df['Organic st']
    df['itunes installs total/st installs'] = df['itunes installs total'] / df['st installs']
    df['itunes installs first/st installs'] = df['itunes installs first'] / df['st installs']

    df['Paid total / Paid st'] = df['Paid total / Paid st'].apply(lambda x: round(x, 2))
    df['Paid first / Paid st'] = df['Paid first / Paid st'].apply(lambda x: round(x, 2))
    df['Browser total / Browser st'] = df['Browser total / Browser st'].apply(lambda x: round(x, 2))
    df['Browser first / Browser st'] = df['Browser first / Browser st'].apply(lambda x: round(x, 2))
    df['Organic total / Organic st'] = df['Organic total / Organic st'].apply(lambda x: round(x, 2))
    df['Organic first / Organic st'] = df['Organic first / Organic st'].apply(lambda x: round(x, 2))
    df['itunes installs total/st installs'] = df['itunes installs total/st installs'].apply(lambda x: round(x, 2))
    df['itunes installs first/st installs'] = df['itunes installs first/st installs'].apply(lambda x: round(x, 2))

    df.to_csv('/src/data/iTunesVsSensorTower.csv', index=False)

# 数数中首次激活设备数和BI二次归因新用户数对比。
def SSFirstLaunchVsBI():
    SSData = getSSData()
    biData = getBIData()

    # 按月进行汇总
    SSData['Month'] = SSData['Date'].apply(lambda x: x[:6])
    SSData = SSData.groupby('Month').sum().reset_index()
    biData['Month'] = biData['Date'].apply(lambda x: x[:6])
    biData = biData.groupby('Month').sum().reset_index()
    df = pd.merge(SSData, biData, on='Month', how='left')
    df['first_launch_total/installs'] = df['first_launch_total'] / df['installs']
    df['first_launch_total/installs'] = df['first_launch_total/installs'].apply(lambda x: round(x, 2))
    df.to_csv('/src/data/SSFirstLaunchVsBI.csv', index=False)

def iTunesVsBI():
    iTunesData = getItunesInstallData()
    biData = getBIData()

    # 按月进行汇总
    iTunesData['Month'] = iTunesData['Date'].apply(lambda x: x[:6])
    iTunesMonthData = iTunesData.groupby('Month').sum().reset_index()
    biData['Month'] = biData['Date'].apply(lambda x: x[:6])
    biMonthData = biData.groupby('Month').sum().reset_index()
    df = pd.merge(iTunesMonthData, biMonthData, on='Month', how='left')
    
    df['Paid total'] = df['APP 引荐来源 total']
    df['Browser total'] = df['网页引荐来源 total']
    df['Organic total'] = df['App Store浏览 total'] + df['App Store搜索 total'] + df['未知来源 total']
    df['total'] = df['Paid total'] + df['Browser total'] + df['Organic total']

    df['Paid first'] = df['APP 引荐来源 first']
    df['Browser first'] = df['网页引荐来源 first']
    df['Organic first'] = df['App Store浏览 first'] + df['App Store搜索 first'] + df['未知来源 first']
    df['first'] = df['Paid first'] + df['Browser first'] + df['Organic first']

    df.rename(columns={
        'installs':'installs bi'
    }, inplace=True)

    df['total/installs bi'] = df['total'] / df['installs bi']
    df['first/installs bi'] = df['first'] / df['installs bi']

    df['total/installs bi'] = df['total/installs bi'].apply(lambda x: round(x, 2))
    df['first/installs bi'] = df['first/installs bi'].apply(lambda x: round(x, 2))

    df.to_csv('/src/data/iTunesVsBI.csv', index=False)
    
def sensortowerVsBI():
    sensorTowerData = getSensorTowerInstallData()
    biData = getBIData()

    # 按月进行汇总
    sensorTowerData['Month'] = sensorTowerData['Date'].apply(lambda x: x[:6])
    sensorTowerData['installs'] = sensorTowerData['Organic'] + sensorTowerData['Browser'] + sensorTowerData['Paid']
    sensorTowerData = sensorTowerData[['Month', 'installs']]
    biData['Month'] = biData['Date'].apply(lambda x: x[:6])
    biMonthData = biData.groupby('Month').sum().reset_index()
    df = pd.merge(sensorTowerData, biMonthData, on='Month', how='left',suffixes=(' st', ' bi'))
    df['installs st/installs bi'] = df['installs st'] / df['installs bi']
    df['installs st/installs bi'] = df['installs st/installs bi'].apply(lambda x: round(x, 2))

    # print(df)
    df.to_csv('/src/data/sensortowerVsBI.csv', index=False)
    
def mergeVsSensortower():
    mergeData = getMergeData()
    mergeData.rename(columns={
        'installs total':'installs total merge',
        'installs organic':'installs organic merge'
    }, inplace=True)

    sensorTowerData = getSensorTowerInstallData()

    # 按月进行汇总
    mergeData['Month'] = mergeData['Date'].apply(lambda x: x[:6])
    mergeMonthData = mergeData.groupby('Month').sum().reset_index()
    sensorTowerData['Month'] = sensorTowerData['Date'].apply(lambda x: x[:6])
    sensorTowerData['installs'] = sensorTowerData['Organic'] + sensorTowerData['Browser'] + sensorTowerData['Paid']
    sensorTowerData.rename(columns={
        'installs':'installs total st',
        'Organic':'installs organic st',
    }, inplace=True)
    sensorTowerData = sensorTowerData[['Month', 'installs total st', 'installs organic st']]
    df = pd.merge(mergeMonthData, sensorTowerData, on='Month', how='left')

    df['installs total st/installs total merge'] = df['installs total st'] / df['installs total merge']
    df['installs organic st/installs organic merge'] = df['installs organic st'] / df['installs organic merge']
    df['installs total st/installs total merge'] = df['installs total st/installs total merge'].apply(lambda x: round(x, 2))
    df['installs organic st/installs organic merge'] = df['installs organic st/installs organic merge'].apply(lambda x: round(x, 2))
    
    df.to_csv('/src/data/mergeVsSensortower.csv', index=False)

def mergeVsITunes():
    mergeData = getMergeData()
    mergeData.rename(columns={
        'installs total':'installs total merge',
        'installs organic':'installs organic merge'
    }, inplace=True)

    iTunesData = getItunesInstallData()

    # 按月进行汇总
    mergeData['Month'] = mergeData['Date'].apply(lambda x: x[:6])
    mergeMonthData = mergeData.groupby('Month').sum().reset_index()
    iTunesData['Month'] = iTunesData['Date'].apply(lambda x: x[:6])
    iTunesMonthData = iTunesData.groupby('Month').sum().reset_index()
    df = pd.merge(mergeMonthData, iTunesMonthData, on='Month', how='left')

    df['Paid total'] = df['APP 引荐来源 total']
    df['Browser total'] = df['网页引荐来源 total']
    df['Organic total'] = df['App Store浏览 total'] + df['App Store搜索 total'] + df['未知来源 total']
    df['itunes installs total'] = df['Paid total'] + df['Browser total'] + df['Organic total']

    df['Paid first'] = df['APP 引荐来源 first']
    df['Browser first'] = df['网页引荐来源 first']
    df['Organic first'] = df['App Store浏览 first'] + df['App Store搜索 first'] + df['未知来源 first']
    df['itunes installs first'] = df['Paid first'] + df['Browser first'] + df['Organic first']

    df['itunes installs total/installs total merge'] = df['itunes installs total'] / df['installs total merge']
    df['itunes installs first/installs total merge'] = df['itunes installs first'] / df['installs total merge']
    df['Organic total/installs organic merge'] = df['Organic total'] / df['installs organic merge']
    df['Organic first/installs organic merge'] = df['Organic first'] / df['installs organic merge']

    df['itunes installs total/installs total merge'] = df['itunes installs total/installs total merge'].apply(lambda x: round(x, 2))
    df['itunes installs first/installs total merge'] = df['itunes installs first/installs total merge'].apply(lambda x: round(x, 2))
    df['Organic total/installs organic merge'] = df['Organic total/installs organic merge'].apply(lambda x: round(x, 2))
    df['Organic first/installs organic merge'] = df['Organic first/installs organic merge'].apply(lambda x: round(x, 2))
    
    df.to_csv('/src/data/mergeVsITunes.csv', index=False)

if __name__ == '__main__':
    # iTunesVsSS()
    # iTunesVsSensorTower()
    # SSFirstLaunchVsBI()
    # iTunesVsBI()
    # sensortowerVsBI()
    mergeVsSensortower()
    mergeVsITunes()