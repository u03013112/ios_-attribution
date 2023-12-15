# iOS 海外报告
import os
import datetime
import subprocess
import pandas as pd
import time

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMedia
from src.report.report.report import toPdf,headStr,getReport,getCsv
def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

# 自定义一个函数，用于检查字符串是否为百分比格式
def is_percentage(s):
    return isinstance(s, str) and s.endswith('%') and s[:-1].replace('.', '', 1).isdigit()

# 使用apply函数对数据框的行进行操作
def calculate(row, column_name):
    value = row[column_name]
    kpi = row['KPI']
    
    if is_percentage(value) and is_percentage(kpi):
        value_float = float(value.rstrip('%'))
        kpi_float = float(kpi.rstrip('%'))
        return '{:.2f}%'.format((value_float - kpi_float) / kpi_float * 100)
    else:
        return '-'
    
def main(startDayStr,endDayStr):
    # 获得目前的UTC0日期，格式20231018
    today = datetime.datetime.utcnow()
    todayStr = today.strftime('%Y%m%d')

    print('今日日期：',todayStr)

    print('查询日期：',startDayStr,'~',endDayStr)

    global directory
    directory = f'/src/data/report/海外iOS速读AI版_{startDayStr}_{endDayStr}'

    if not os.path.exists(directory):
        os.makedirs(directory)

    # 对查询日期进行修正
    
    endDayStrFix = endDayStr
    
    # 其中由于归因要用到融合归因的数据，只能获得T-3的数据，去掉不能获得的数据
    # 检查目前时间，如果超过北京时间15点(UTC 7点），那么可以获取到T-2的数据，否则只能获取到T-3的数据
    n = 3
    if today.hour >= 7:
        n = 2

    endDayMaxStr = (today - datetime.timedelta(days=n)).strftime('%Y%m%d')

    if endDayStr > endDayMaxStr:
        endDayStrFix = endDayMaxStr
    # 先计算startDayStr到endDayStrFix共有多少天
    # 为了计算环比，需要往前推N天
    startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
    endDayFix = datetime.datetime.strptime(endDayStrFix, '%Y%m%d')
    N = (endDayFix - startDay).days
    startDayStrFix = (endDayFix - datetime.timedelta(days=N*2+1)).strftime('%Y%m%d')
    print('修正查询日期：',startDayStrFix,'~',endDayStrFix)

    # 对上述数据进行一些修正，其中部分数据并不完整，比如3日回收和7日回收，在不满3日或者7日的时候，数据是不完整的，只是前几天收入的和，不适合环比之前完整的数据
    # 所以将这些数据置为空，然后在报告中如果涉及到这些数据，就不显示，或者显示‘数据不全’
    # 3日回收
    day3MaxStr = (today - datetime.timedelta(days=3)).strftime('%Y%m%d')
    print(f'超过{day3MaxStr}（不包含）的3日回收数据置为空')
    # 7日回收
    day7MaxStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    print(f'超过{day7MaxStr}（不包含）的7日回收数据置为空')

    # 
    startDayStr1 = (endDayFix - datetime.timedelta(days=N)).strftime('%Y%m%d')
    endDayStr1 = endDayStrFix
    startDayStr2 = startDayStrFix
    endDayStr2 = (endDayFix - datetime.timedelta(days=N+1)).strftime('%Y%m%d')
    print(f'环比日期：{startDayStr1}~{endDayStr1}，{startDayStr2}~{endDayStr2}')

    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))

    df = df.fillna(0)

    df.loc[df['install_date'] > day3MaxStr,'revenue_3d'] = None
    df.loc[df['install_date'] > day7MaxStr,'revenue_7d'] = None

    header = f'target,group,{startDayStr1}~{endDayStr1},{startDayStr2}~{endDayStr2},环比\n'

    # 生成第一个报告，快速获得目前的整体情况
    # 大盘改为分国家，这里不再使用完整的大盘，因为KPI是分国家的
    # 应该分列：
    # target（ROI1，ROI3，ROI7，Cost）
    # group:(国家分组)
    # {startDayStr1}~{endDayStr1}：目前时间范围内均值
    # {startDayStr2}~{endDayStr2}：前一个时间段均值
    # 环比：环比比例
    # 备注：比如ROI7的行，后面备注KPI指标，ROI1，ROI3可以给出参考值，按照这段时间内所有可参考数据平均值进行估算
    csvStr = '' + header
    csvStr += getCsv(df,{'name':'ROI24h','op':'/','targetList':['revenue_24h','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    # csvStr += getCsv(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)

    filename = getFilename('report1_0','csv')
    with open(filename,'w') as f:
        f.write(csvStr)
    print('已存储在%s'%filename)

     # KPI指标
    kpiDf = pd.DataFrame(columns=['target','group','KPI'])
    kpi = {
        'US':0.065,
        'KR':0.065,
        'JP':0.055,
        'GCC':0.06,
        'other':0.07
    }
    
    for geo in ['US','KR','JP','GCC','other']:
        kpiDf = kpiDf.append({'target':'ROI7','group':geo,'KPI':'%.2f%%'%(kpi[geo]*100)},ignore_index=True)

        geoDf = df[(df['geoGroup'] == geo) & (df['revenue_7d'].isnull() == False)& (df['revenue_3d'].isnull() == False)]
        p7_24 = geoDf['revenue_7d'].sum() / geoDf['revenue_24h'].sum()
        kpiDf = kpiDf.append({'target':'ROI24h','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p7_24*100)},ignore_index=True)
        
        # p71 = geoDf['revenue_7d'].sum() / geoDf['revenue_1d'].sum()
        # kpiDf = kpiDf.append({'target':'ROI1','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p71*100)},ignore_index=True)

        p73 = geoDf['revenue_7d'].sum() / geoDf['revenue_3d'].sum()
        kpiDf = kpiDf.append({'target':'ROI3','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p73*100)},ignore_index=True)

    df1 = pd.read_csv(filename)
    df1 = pd.merge(df1,kpiDf,on=['target','group'],how='left')

    

    df1[f'{startDayStr1}~{endDayStr1} KPI比较'] = df1.apply(calculate,args=(f'{startDayStr1}~{endDayStr1}',), axis=1)
    df1[f'{startDayStr2}~{endDayStr2} KPI比较'] = df1.apply(calculate,args=(f'{startDayStr2}~{endDayStr2}',), axis=1)

    filename = getFilename('report1_1','csv')
    df1.to_csv(filename,index=False)
    print('已存储在%s'%filename)

    # 生成第二个报告，分媒体
    # 分媒体
    csvStr = '' + header
    csvStr += getCsv(df,{'name':'ROI24h','op':'/','targetList':['revenue_24h','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    # csvStr += getCsv(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'Cost rate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'revenue 1day','op':'','targetList':['revenue_1d'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'revenue 1day rate','op':'rate','targetList':['revenue_1d'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    csvStr += getCsv(df,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='media',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
    filename = getFilename('report2_1','csv')
    with open(filename,'w') as f:
        f.write(csvStr)
    print('已存储在%s'%filename)

    # 生成第三个报告，分媒体分国家
    mediaList = df['media'].unique()

    for media in mediaList:
        if media in ('organic','other'):
            continue

        mediaDf = df[df['media'] == media].copy()

        csvStr = '' + header
        csvStr += getCsv(mediaDf,{'name':'ROI24h','op':'/','targetList':['revenue_24h','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        csvStr += getCsv(mediaDf,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        csvStr += getCsv(mediaDf,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        csvStr += getCsv(mediaDf,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        csvStr += getCsv(mediaDf,{'name':'Cost rate','op':'rate','targetList':['cost'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        
        filename = getFilename(f'report3_0_{media}','csv')
        with open(filename,'w') as f:
            f.write(csvStr)
        print('已存储在%s'%filename)

        kpiDf = pd.DataFrame(columns=['target','group','KPI'])
        for geo in ['US','KR','JP','GCC','other']:
            kpiDf = kpiDf.append({'target':'ROI7','group':geo,'KPI':'%.2f%%'%(kpi[geo]*100)},ignore_index=True)

            geoDf = mediaDf[(mediaDf['geoGroup'] == geo) & (mediaDf['revenue_7d'].isnull() == False)& (mediaDf['revenue_3d'].isnull() == False)]
            
            p7_24 = geoDf['revenue_7d'].sum() / geoDf['revenue_24h'].sum()
            kpiDf = kpiDf.append({'target':'ROI24h','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p7_24*100)},ignore_index=True)

            # p71 = geoDf['revenue_7d'].sum() / geoDf['revenue_1d'].sum()
            # kpiDf = kpiDf.append({'target':'ROI1','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p71*100)},ignore_index=True)

            p73 = geoDf['revenue_7d'].sum() / geoDf['revenue_3d'].sum()
            kpiDf = kpiDf.append({'target':'ROI3','group':geo,'KPI':'%.2f%%'%(kpi[geo]/p73*100)},ignore_index=True)

        df1 = pd.read_csv(filename)
        df1 = pd.merge(df1,kpiDf,on=['target','group'],how='left')

        df1[f'{startDayStr1}~{endDayStr1} KPI比较'] = df1.apply(calculate,args=(f'{startDayStr1}~{endDayStr1}',), axis=1)
        df1[f'{startDayStr2}~{endDayStr2} KPI比较'] = df1.apply(calculate,args=(f'{startDayStr2}~{endDayStr2}',), axis=1)

        filename = getFilename(f'report3_1_{media}','csv')
        df1.to_csv(filename,index=False)

        # csvStr += getCsv(mediaDf,{'name':'revenue 1day','op':'','targetList':['revenue_1d'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'revenue 1day rate','op':'rate','targetList':['revenue_1d'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'revenue 7day rate','op':'rate','targetList':['revenue_7d'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'CVR','op':'/','targetList':['install_ad','click'],'format':'.2f%'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)
        # csvStr += getCsv(mediaDf,{'name':'CPI','op':'/','targetList':['cost','install_ad'],'format':'.2f'},groupBy='geoGroup',startDayStr1=startDayStr1,endDayStr1=endDayStr1,df2=mediaDf,startDayStr2=startDayStr2,endDayStr2=endDayStr2)

        
    
def debug():
    df = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/revenue20231022_20231110_GroupByCampaignAndGeoAndMedia.csv')
    df = df.fillna(0)
    geoList = df['geoGroup'].unique()
    for geo in geoList:
        
        geoDf = df[df['geoGroup'] == geo]
        sum24h = geoDf['revenue_24h'].sum()
        sum1d = geoDf['revenue_1d'].sum()
        if sum1d > sum24h:
            print(geo)
            print('24h:',geoDf['revenue_24h'].sum())
            print('1d:',geoDf['revenue_1d'].sum())

            print(geoDf.loc[geoDf['revenue_1d'] > geoDf['revenue_24h']])

# 测试融合归因结论是否已经准备好 
def check(endDayStr):
    sql = f'''
        SELECT
            *
        FROM
            rg_bi.topwar_ios_funplus02_adv_uid_mutidays_campaign2
        WHERE
            day = '{endDayStr}'
    '''
    print(sql)
    df = execSql(sql)
    if len(df) == 0:
        print('没有数据')
        return False
    
    return True

# 周报，获取一周数据
def weekly():
    # endDay 是从今天往前推2天
    today = datetime.datetime.utcnow()
    endDayStr = (today - datetime.timedelta(days=2)).strftime('%Y%m%d')
    # startDay 是从endDay往前推7+2天
    startDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')

    retryMax = 10
    for _ in range(retryMax):    
        if not check(endDayStr):
            print('数据还没有准备好，等待5分钟后重试')
            # 等待5分钟
            time.sleep(300)
            continue

        # print('查询日期：',startDayStr,'~',endDayStr)
        main(startDayStr,endDayStr)

        # 将目录写到文件中/src/data/report/todoList.txt，供其他程序使用
        # 这里就不做互斥了，先简单的做，之后可以搞个消息中心啥的
        filename = '/src/data/report/todoList.txt'
        with open(filename,'a') as f:
            f.write(f'{directory}\n')
        
        break
    

if __name__ == '__main__':
    # main('20231118','20231125')
    weekly()
