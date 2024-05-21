# 自然量对数



# iTunes的安装数值和sensor tower的安装数值对比。

# 数数中首次激活设备数和BI二次归因新用户数对比。

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

    return sensorTowerDf

def getBIData():
    biData = pd.read_csv('biData.csv')
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

# iTunes的安装数值和数数中首次激活设备数对比。
def iTunesVsSS():
    iTunesData = getItunesInstallData()
    SSData = getSSData()

    # total 是所有 以‘ total’结尾的列的数据的和
    iTunesData['total sum'] = iTunesData.filter(like=' total').sum(axis=1)
    iTunesData['first sum'] = iTunesData.filter(like=' first').sum(axis=1)

    df = pd.merge(iTunesData, SSData, on='Date', how='left')
    
    # 额外添加一行，Date列写SUM
    sumRow = df.sum()
    sumRow['Date'] = 'SUM'
    df = df.append(sumRow, ignore_index=True)

    # 得到结论，iTunes的全部安装数值和数数中首次激活设备数数值接近，差距在6%左右。
    df.to_csv('/src/data/iTunesVsSS.csv', index=False)

if __name__ == '__main__':
    # getItunesInstallData()
    # print(getSensorTowerInstallData())
    # print(getBIData())
    iTunesVsSS()