# iOS 海外，用融合归因结论作为归因结论的报告
# 临时分析

import os
import datetime
import subprocess
import pandas as pd

import sys
sys.path.append('/src')

from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMedia
from src.report.report.report import toPdf,headStr,getReport,getCsv


def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)


# 获得目前的UTC0日期，格式20231018
today = datetime.datetime.utcnow()
todayStr = today.strftime('%Y%m%d')

# for test
# todayStr = '20231107'
# today = datetime.datetime.strptime(todayStr,'%Y%m%d')

print('今日日期：',todayStr)
# 获得N天的数据
N = 30
# 获得一周前的UTC0日期，格式20231011，往前一天，不获取今天的不完整数据。
startDayStr = (today - datetime.timedelta(days=N+1)).strftime('%Y%m%d')
endDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
print('查询日期：',startDayStr,'~',endDayStr)

directory = f'/src/data/report/iOSWeekly{startDayStr}_{endDayStr}'

def main():
    if not os.path.exists(directory):
        os.makedirs(directory)

    # 为了可以环比，需要额外多获得N天的数据
    # 另外，由于收入输入采用融合归因结论，融合归因只能获得大前天的数据，所以再多往前获得2天的数据，保险起见，直接多获得5天的数据
    startDayStrFix = (today - datetime.timedelta(days=N*2+5)).strftime('%Y%m%d')
    endDayStrFix = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')

    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))

    df = df.fillna(0)

    # 生成报告需求
    # 1、基础数据，目前所有的基础数据：CPM，CTR，CVR，CPI，Cost，ROI1，ROI3，ROI7
    # 2、分维度：大盘，媒体，国家
    # 3、在上面中的媒体中，为每个媒体单独的生成一份报告，在媒体的报告中，分国家、分campaign进行分析

    # 不涉及融合归因的部分就用这个时间段，往前一天，不获取今天的不完整数据。
    startDayStr1 = startDayStr
    endDayStr1 = endDayStr
    # 环比，将上面日期往前推N天
    startDayStr2 = (datetime.datetime.strptime(startDayStr1, "%Y%m%d") - datetime.timedelta(days=N)).strftime("%Y%m%d")
    endDayStr2 = (datetime.datetime.strptime(endDayStr1, "%Y%m%d") - datetime.timedelta(days=N)).strftime("%Y%m%d")

    # ROI1 涉及融合归因结论，所以需要再往前2天
    startDayStr1ROI1 = (datetime.datetime.strptime(startDayStr1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr1ROI1 = (datetime.datetime.strptime(endDayStr1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    startDayStr2ROI1 = (datetime.datetime.strptime(startDayStr2, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr2ROI1 = (datetime.datetime.strptime(endDayStr2, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")

    # ROI3 为了获得3天的数据，需要再往前2天
    startDayStr1ROI3 = (datetime.datetime.strptime(startDayStr1ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr1ROI3 = (datetime.datetime.strptime(endDayStr1ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    startDayStr2ROI3 = (datetime.datetime.strptime(startDayStr2ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr2ROI3 = (datetime.datetime.strptime(endDayStr2ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")

    # ROI7 为了获得7天的数据，需要再往前4天
    startDayStr1ROI7 = (datetime.datetime.strptime(startDayStr1ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    endDayStr1ROI7 = (datetime.datetime.strptime(endDayStr1ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    startDayStr2ROI7 = (datetime.datetime.strptime(startDayStr2ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    endDayStr2ROI7 = (datetime.datetime.strptime(endDayStr2ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")

    csvFilenameList = []
    header = f'target,group,{startDayStr1ROI1}~{endDayStr1ROI1},{startDayStr2ROI1}~{endDayStr2ROI1},环比\n'
    # 大盘
    csvStr = '' + header
    csvStr += getCsv(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    # csvStr += getCsv(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    # csvStr += getCsv(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    # csvStr += getCsv(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    # csvStr += getCsv(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    filename = getFilename('reportAll','csv')
    with open(filename,'w') as f:
        f.write(csvStr)
    print('已存储在%s'%filename)
    csvFilenameList.append(filename)

    # 分媒体
    csvStr = '' + header
    csvStr += getCsv(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'CostRate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'revenue_1d','op':'','targetList':['revenue_1d'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'revenue_1dRate','op':'rate','targetList':['revenue_1d'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    csvStr += getCsv(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
    filename = getFilename('reportMedia','csv')
    with open(filename,'w') as f:
        f.write(csvStr)
    print('已存储在%s'%filename)
    csvFilenameList.append(filename)

    # 分媒体后分国家
    mediaList = df['media'].unique().tolist()
    for media in mediaList:
        if media == 'other':
            continue
        mediaDf = df[df['media'] == media].copy()

        csvStr = '' + header
        csvStr += getCsv(mediaDf,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'CostRate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'revenue_1d','op':'','targetList':['revenue_1d'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'revenue_1dRate','op':'rate','targetList':['revenue_1d'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        csvStr += getCsv(mediaDf,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1)
        filename = getFilename(f'reportMedia{media}','csv')
        with open(filename,'w') as f:
            f.write(csvStr)
        print('已存储在%s'%filename)
        csvFilenameList.append(filename)

    # 对上面所有的生成csv进行收集与统计，将所有环比绝对值大于20%的数据挑出来，并存储到fastRead.csv中
    
    fastDf = pd.DataFrame()
    for csvFilename in csvFilenameList:
        df0 = pd.read_csv(csvFilename)
        df0['filename'] = os.path.basename(csvFilename)
        df0['环比'] = pd.to_numeric(df0['环比'].str.rstrip('%')) / 100
        fastDf0 = df0.loc[(df0['环比'] > 0.2) | (df0['环比'] < -0.2)]
        fastDf = pd.concat([fastDf,fastDf0],axis=0)
    # 将环比列转换回百分比字符串格式
    fastDf['环比'] = fastDf['环比'].apply(lambda x: f"{x:.2%}")
    # 将group列 == 'other'的行去掉
    fastDf = fastDf.loc[fastDf['group'] != 'other']
    # fastDf = fastDf.sort_values(by=['target','group','环比'],ascending=[True,True,False])
    fastDf.to_csv(getFilename('fastRead','csv'),index=False)

    # 大盘部分开始 ##########################################################################################
    if True:
    # if False:
        reportStr = '' + headStr
        reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 之 大盘汇总\n\n'

        reportStr += '## ROI1\n\n'
        reportStr += getReport(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI3\n\n'
        reportStr += getReport(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=df,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI7\n\n'
        reportStr += getReport(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=df,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## Cost\n\n'
        reportStr += getReport(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CPM\n\n'
        reportStr += getReport(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CTR\n\n'
        reportStr += getReport(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)
        
        reportStr += '## CVR\n\n'
        reportStr += getReport(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CPI\n\n'
        reportStr += getReport(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)


        filename = getFilename('reportAll','md')
        with open(filename,'w') as f:
            f.write(reportStr)
        print('已存储在%s'%filename)
        toPdf(directory,'reportAll')

    # 大盘部分结束 ##########################################################################################

    # 分媒体部分 ##########################################################################################
    if True:
    # if False:
        reportStr = '' + headStr
        reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 之 分媒体\n\n'

        reportStr += '## ROI1\n\n'
        reportStr += getReport(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI3\n\n'
        reportStr += getReport(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=df,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI7\n\n'
        reportStr += getReport(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=df,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## revenue_1d\n\n'
        reportStr += getReport(df,{'name':'revenue_1d','op':'','targetList':['revenue_1d'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## revenue_3d\n\n'
        reportStr += getReport(df,{'name':'revenue_3d','op':'','targetList':['revenue_3d'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=df,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## revenue_7d\n\n'
        reportStr += getReport(df,{'name':'revenue_7d','op':'','targetList':['revenue_7d'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=df,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## Cost\n\n'
        reportStr += getReport(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CostRate\n\n'
        reportStr += getReport(df,{'name':'CostRate','op':'rate','targetList':['cost'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CPM\n\n'
        reportStr += getReport(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)
        
        reportStr += '## CTR\n\n'
        reportStr += getReport(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CVR\n\n'
        reportStr += getReport(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CPI\n\n'
        reportStr += getReport(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        filename = getFilename('reportMedia','md')
        with open(filename,'w') as f:
            f.write(reportStr)
        print('已存储在%s'%filename)
        toPdf(directory,'reportMedia')
    # 分媒体部分结束 ##########################################################################################

    # 分国家部分 ##########################################################################################
    # if True:
    if False:
        reportStr = '' + headStr
        reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 之 分国家\n\n'

        reportStr += '## CPM\n\n'
        reportStr += getReport(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CTR\n\n'
        reportStr += getReport(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CVR\n\n'
        reportStr += getReport(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## CPI\n\n'
        reportStr += getReport(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## Cost\n\n'
        reportStr += getReport(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI1\n\n'
        reportStr += getReport(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI3\n\n'
        reportStr += getReport(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=df,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

        reportStr += '## ROI7\n\n'
        reportStr += getReport(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=df,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

        filename = getFilename('reportGeo','md')
        with open(filename,'w') as f:
            f.write(reportStr)
        print('已存储在%s'%filename)
        toPdf(directory,'reportGeo')
    # 分国家部分结束 ##########################################################################################

    # 分媒体之后分国家部分 ##########################################################################################
    if True:
    # if False:
        mediaList = df['media'].unique().tolist()
        for media in mediaList:
            print('media:',media)

            reportStr = '' + headStr
            reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 之 {media}分国家\n\n'

            mediaDf = df[df['media'] == media].copy()

            reportStr += '## ROI1\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## ROI3\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=mediaDf,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## ROI7\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=mediaDf,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## Cost\n\n'
            reportStr += getReport(mediaDf,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CostRate\n\n'
            reportStr += getReport(mediaDf,{'name':'CostRate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CPM\n\n'
            reportStr += getReport(mediaDf,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CTR\n\n'
            reportStr += getReport(mediaDf,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CVR\n\n'
            reportStr += getReport(mediaDf,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CPI\n\n'
            reportStr += getReport(mediaDf,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            filename = getFilename(f'reportMedia_{media}_geo','md')
            with open(filename,'w') as f:
                f.write(reportStr)
            print('已存储在%s'%filename)
            toPdf(directory,f'reportMedia_{media}_geo')
    # 分媒体之后分国家部分结束 ##########################################################################################

    # 分campaign部分 ##########################################################################################
    # if True:
    if False:
        mediaList = df['media'].unique().tolist()
        for media in mediaList:
            print('media:',media)

            reportStr = '' + headStr
            reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 之 {media}分campaign\n\n'

            mediaDf = df[df['media'] == media].copy()
            
            reportStr += '## CPM\n\n'
            reportStr += getReport(mediaDf,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CTR\n\n'
            reportStr += getReport(mediaDf,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CVR\n\n'
            reportStr += getReport(mediaDf,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CPI\n\n'
            reportStr += getReport(mediaDf,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## Cost\n\n'
            reportStr += getReport(mediaDf,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## CostRate\n\n'
            reportStr += getReport(mediaDf,{'name':'CostRate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr,endDayStr1=endDayStr,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## ROI1\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=mediaDf,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## ROI3\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=mediaDf,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=False,path=directory)

            reportStr += '## ROI7\n\n'
            reportStr += getReport(mediaDf,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='campaign_name',startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=mediaDf,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=False,path=directory)

            filename = getFilename(f'reportMedia_{media}_campaign','md')
            with open(filename,'w') as f:
                f.write(reportStr)
            print('已存储在%s'%filename)
            toPdf(directory,f'reportMedia_{media}_campaign')
    # 分campaign部分结束 ##########################################################################################

def debug():
    df = pd.read_csv('/src/data/report/iOSWeekly20231030_20231106/revenue20231019_20231106_GroupByCampaignAndGeoAndMedia.csv')
    df = df.groupby(['install_date','media'],as_index=False).sum().reset_index(drop=True)
    print(df)

if __name__ == '__main__':
    main()
    # debug()
