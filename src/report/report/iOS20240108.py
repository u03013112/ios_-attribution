# 思路乱了，重新整理一下
# 今天先出一个改进版本v1，之后的慢慢再加
# 
# 1、改变阅读思路，上来是结论，然后后面是针对结论的分析与数据
# 2、目前的结论是符合KPI的花费总金额，分国家总金额
# 3、然后是针对这个结论的分析与数据，是分媒体+分国家的花费金额与ROI7D
# 4、最后，要是有时间再出一个自然量比例分析，即自然量占比的分国家版本。再出一个分析。
# 下一个版本中再加入过程与趋势分析，比如目前的ROI24H与ROI3D，能够更加敏捷的发现趋势，但是得到结论不稳定。


import os
import datetime
import numpy as np
import pandas as pd
import time

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.report.data.milestones import getMilestonesStartDate
from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia,getAdCostDataIOSGroupByGeo
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMediaNew,getRevenueMilestones

def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

# KPI指标
kpi = {
    'US':0.065,
    'KR':0.065,
    'JP':0.055,
    'GCC':0.06,
    'other':0.07
}

# 第一段，即结论
# 符合KPI的花费总金额，分国家总金额
def report1(days = 7):
    print('report1')
    today = datetime.datetime.utcnow()
    # N 是获得满N日数据的周期
    N = 7

    startDayStr1 = (today - datetime.timedelta(days=2*(days-1)+N+1)).strftime('%Y%m%d')
    endDayStr1 = (today - datetime.timedelta(days=days+N)).strftime('%Y%m%d')
    startDayStr2 = (today - datetime.timedelta(days=days+N-1)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=N)).strftime('%Y%m%d')

    print('查询日期：',startDayStr1,'~',endDayStr1,'和',startDayStr2,'~',endDayStr2)

    revenueDf1 = getRevenueMilestones(startDayStr1,endDayStr1,directory)
    revenueDf2 = getRevenueMilestones(startDayStr2,endDayStr2,directory)

    df = pd.merge(revenueDf1,revenueDf2,on=['country_group'],how='outer',suffixes=('_1','_2'))

    adCostDf1 = getAdCostDataIOSGroupByGeo(startDayStr1,endDayStr1,directory)
    adCostDf2 = getAdCostDataIOSGroupByGeo(startDayStr2,endDayStr2,directory)

    df2 = pd.merge(adCostDf1,adCostDf2,on=['country_group'],how='outer',suffixes=('_1','_2'))

    df = pd.merge(df,df2,on=['country_group'],how='outer') 
    
    df['roi7_1'] = df['r7usd_1']/df['cost_1']
    df['roi7_2'] = df['r7usd_2']/df['cost_2']

    df.rename(columns={
        'country_group':'geoGroup',
    },inplace=True)
    
    kpiDf = pd.DataFrame(list(kpi.items()), columns=['geoGroup', 'KPI'])

    df = pd.merge(df,kpiDf,on=['geoGroup'],how='left')
    
    # 备份原始花费
    df['cost_1_0'] = df['cost_1']
    df['cost_2_0'] = df['cost_2']
    # 不合格的花费置为0
    df.loc[df['roi7_1']<df['KPI'],'cost_1'] = 0
    df.loc[df['roi7_2']<df['KPI'],'cost_2'] = 0

    # 加入一行，汇总，geoGroup就写all，cost1和cost2就是所有国家的花费，其他列留空白
    df = df.append({
        'geoGroup':'所有国家汇总','cost_1':df['cost_1'].sum(),'cost_2':df['cost_2'].sum(),'cost_1_0':df['cost_1_0'].sum(),'cost_2_0':df['cost_2_0'].sum()
    },ignore_index=True)

    df['花费环比'] = (df['cost_2'] - df['cost_1'])/df['cost_1']

    # 整理格式
    df = df[['geoGroup','cost_1','roi7_1','cost_2','roi7_2','花费环比','KPI','cost_1_0','cost_2_0']]
    df.rename(columns={
        'geoGroup':'国家',
        'cost_1':f'{startDayStr1}~{endDayStr1}花费',
        'roi7_1':f'{startDayStr1}~{endDayStr1}ROI7D',
        'cost_2':f'{startDayStr2}~{endDayStr2}花费',
        'roi7_2':f'{startDayStr2}~{endDayStr2}ROI7D',
        'KPI':'KPI',
        'cost_1_0':f'{startDayStr1}~{endDayStr1}原始花费(包含未达标花费)',
        'cost_2_0':f'{startDayStr2}~{endDayStr2}原始花费(包含未达标花费)'
    },inplace=True)

    df[f'{startDayStr1}~{endDayStr1}花费'] = df[f'{startDayStr1}~{endDayStr1}花费'].apply(lambda x:'%.2f'%x)
    df[f'{startDayStr2}~{endDayStr2}花费'] = df[f'{startDayStr2}~{endDayStr2}花费'].apply(lambda x:'%.2f'%x)
    df[f'{startDayStr1}~{endDayStr1}原始花费(包含未达标花费)'] = df[f'{startDayStr1}~{endDayStr1}原始花费(包含未达标花费)'].apply(lambda x:'%.2f'%x)
    df[f'{startDayStr2}~{endDayStr2}原始花费(包含未达标花费)'] = df[f'{startDayStr2}~{endDayStr2}原始花费(包含未达标花费)'].apply(lambda x:'%.2f'%x)

    df[f'{startDayStr1}~{endDayStr1}ROI7D'] = df[f'{startDayStr1}~{endDayStr1}ROI7D'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    df[f'{startDayStr2}~{endDayStr2}ROI7D'] = df[f'{startDayStr2}~{endDayStr2}ROI7D'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    df['花费环比'] = df['花费环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    df['KPI'] = df['KPI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

    filename = getFilename('report1','csv')
    df.to_csv(filename,index=False)
    print('report1 done,save to',filename)

# 从里程碑开始算
def report1Fix(days = 7):
    startDayStr = getMilestonesStartDate()
    print('report1')
    today = datetime.datetime.utcnow()
    # N 是获得满N日数据的周期
    N = 7

    startDayStr1 = (today - datetime.timedelta(days=2*(days-1)+N+1)).strftime('%Y%m%d')
    endDayStr1 = (today - datetime.timedelta(days=days+N)).strftime('%Y%m%d')
    startDayStr2 = (today - datetime.timedelta(days=days+N-1)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=N)).strftime('%Y%m%d')

    print('里程碑开始时间：',startDayStr)
    print('查询日期：',startDayStr1,'~',endDayStr1,'和',startDayStr2,'~',endDayStr2)

    revenueDf0 = getRevenueMilestones(startDayStr,startDayStr1,directory)
    revenueDf1 = getRevenueMilestones(startDayStr,endDayStr1,directory)
    revenueDf2 = getRevenueMilestones(startDayStr,endDayStr2,directory)

    df = pd.merge(revenueDf0,revenueDf1,on=['country_group'],how='outer',suffixes=('','_1'))
    df = pd.merge(df,revenueDf2,on=['country_group'],how='outer',suffixes=('','_2'))
    df = df.fillna(0)
    

    adCostDf0 = getAdCostDataIOSGroupByGeo(startDayStr,startDayStr1,directory)
    adCostDf1 = getAdCostDataIOSGroupByGeo(startDayStr,endDayStr1,directory)
    adCostDf2 = getAdCostDataIOSGroupByGeo(startDayStr,endDayStr2,directory)

    df2 = pd.merge(adCostDf0,adCostDf1,on=['country_group'],how='outer',suffixes=('','_1'))
    df2 = pd.merge(df2,adCostDf2,on=['country_group'],how='outer',suffixes=('','_2'))
    df2 = df2.fillna(0)

    df = pd.merge(df,df2,on=['country_group'],how='outer')     
    
    df['roi7'] = df['r7usd']/df['cost']
    df['roi7_1'] = df['r7usd_1']/df['cost_1']
    df['roi7_2'] = df['r7usd_2']/df['cost_2']

    df.rename(columns={
        'country_group':'geoGroup',
    },inplace=True)
    
    kpiDf = pd.DataFrame(list(kpi.items()), columns=['geoGroup', 'KPI'])

    df = pd.merge(df,kpiDf,on=['geoGroup'],how='left')
    
    # 备份原始花费
    df['cost_0 排除未达标'] = df['cost']
    df['cost_1 排除未达标'] = df['cost_1']
    df['cost_2 排除未达标'] = df['cost_2']
    # 不合格的花费置为0
    df.loc[df['roi7']<df['KPI'],'cost_0 排除未达标'] = 0
    df.loc[df['roi7_1']<df['KPI'],'cost_1 排除未达标'] = 0
    df.loc[df['roi7_2']<df['KPI'],'cost_2 排除未达标'] = 0

    # 差值计算
    df['r7usd 1-0'] = df['r7usd_1'] - df['r7usd']
    df['r7usd 2-1'] = df['r7usd_2'] - df['r7usd_1']

    df['cost 1-0'] = df['cost_1'] - df['cost']
    df['cost 2-1'] = df['cost_2'] - df['cost_1']

    df['roi 1-0'] = df['r7usd 1-0']/df['cost 1-0']
    df['roi 2-1'] = df['r7usd 2-1']/df['cost 2-1']

    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # 加入一行，汇总，geoGroup就写all，cost1和cost2就是所有国家的花费，其他列留空白
    df = df.append({
        'geoGroup':'所有国家汇总',
        'cost':df['cost'].sum(),
        'cost_1':df['cost_1'].sum(),
        'cost_2':df['cost_2'].sum(),
        'cost_0 排除未达标':df['cost_0 排除未达标'].sum(),
        'cost_1 排除未达标':df['cost_1 排除未达标'].sum(),
        'cost_2 排除未达标':df['cost_2 排除未达标'].sum(),
        'cost 1-0':df['cost 1-0'].sum(),
        'cost 2-1':df['cost 2-1'].sum()
    },ignore_index=True)

    df['花费环比'] = (df['cost 2-1'] - df['cost 1-0'])/df['cost 1-0']

    # 整理格式
    df = df[['geoGroup','KPI','cost','roi7','cost_1','roi7_1','cost_2','roi7_2','cost 1-0','roi 1-0','cost 2-1','roi 2-1','花费环比','cost_0 排除未达标','cost_1 排除未达标','cost_2 排除未达标']]
    df.rename(columns={
        'geoGroup':'国家',
        'KPI':'目标ROI',
        'cost':f'{startDayStr}~{startDayStr1}花费',
        'roi7':f'{startDayStr}~{startDayStr1}满7ROI',
        'cost_1':f'{startDayStr}~{endDayStr1}花费',
        'roi7_1':f'{startDayStr}~{endDayStr1}满7ROI',
        'cost_2':f'{startDayStr}~{endDayStr2}花费',
        'roi7_2':f'{startDayStr}~{endDayStr2}满7ROI',
        'cost 1-0':f'{startDayStr1}~{endDayStr1}花费',
        'roi 1-0':f'{startDayStr1}~{endDayStr1}满7ROI',
        'cost 2-1':f'{startDayStr2}~{endDayStr2}花费',
        'roi 2-1':f'{startDayStr2}~{endDayStr2}满7ROI',
        '花费环比':f'{startDayStr2}~{endDayStr2}与{startDayStr1}~{endDayStr1}花费环比',
        'cost_0 排除未达标':f'{startDayStr}~{startDayStr1}花费(排除未达标花费)',
        'cost_1 排除未达标':f'{startDayStr}~{endDayStr1}花费(排除未达标花费)',
        'cost_2 排除未达标':f'{startDayStr}~{endDayStr2}花费(排除未达标花费)'
    },inplace=True)

    df['目标ROI'] = df['目标ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

    # 整理格式

    # 花费保留两位小数，用索引是因为，列名是变化的，而且有可能会重名，当周期的开始正好是里程碑开始时间，会导致列重名
    for colIndex in [2,4,6,8,10,13,14,15]:
        df.iloc[:,colIndex] = df.iloc[:,colIndex].apply(lambda x:'%.2f'%x)

    # df[f'{startDayStr}~{startDayStr1}花费'] = df[f'{startDayStr}~{startDayStr1}花费'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr}~{endDayStr1}花费'] = df[f'{startDayStr}~{endDayStr1}花费'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr}~{endDayStr2}花费'] = df[f'{startDayStr}~{endDayStr2}花费'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr1}~{endDayStr1}花费'] = df[f'{startDayStr1}~{endDayStr1}花费'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr2}~{endDayStr2}花费'] = df[f'{startDayStr2}~{endDayStr2}花费'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr}~{startDayStr1}花费(排除未达标花费)'] = df[f'{startDayStr}~{startDayStr1}花费(排除未达标花费)'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr}~{endDayStr1}花费(排除未达标花费)'] = df[f'{startDayStr}~{endDayStr1}花费(排除未达标花费)'].apply(lambda x:'%.2f'%x)
    # df[f'{startDayStr}~{endDayStr2}花费(排除未达标花费)'] = df[f'{startDayStr}~{endDayStr2}花费(排除未达标花费)'].apply(lambda x:'%.2f'%x)

    # 与上面统一写法
    for colIndex in [3,5,7,9,11,12]:
        df.iloc[:,colIndex] = df.iloc[:,colIndex].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

    # df[f'{startDayStr2}~{endDayStr2}与{startDayStr1}~{endDayStr1}花费环比'] = df[f'{startDayStr2}~{endDayStr2}与{startDayStr1}~{endDayStr1}花费环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

    # df[f'{startDayStr}~{startDayStr1}满7ROI'] = df[f'{startDayStr}~{startDayStr1}满7ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    # df[f'{startDayStr}~{endDayStr1}满7ROI'] = df[f'{startDayStr}~{endDayStr1}满7ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    # df[f'{startDayStr}~{endDayStr2}满7ROI'] = df[f'{startDayStr}~{endDayStr2}满7ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    # df[f'{startDayStr1}~{endDayStr1}满7ROI'] = df[f'{startDayStr1}~{endDayStr1}满7ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    # df[f'{startDayStr2}~{endDayStr2}满7ROI'] = df[f'{startDayStr2}~{endDayStr2}满7ROI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    
    filename = getFilename('report1Fix','csv')
    df.to_csv(filename,index=False)
    print('report1Fix done,save to',filename)


# 第二段，即针对结论的分析与数据
# 分媒体+分国家的花费金额与ROI7D
def report2(days = 7):
    print('report2')
    today = datetime.datetime.utcnow()

    # N 是获得满N日数据的周期
    N = 7
    
    startDayStr1 = (today - datetime.timedelta(days=2*(days-1)+N+1)).strftime('%Y%m%d')
    endDayStr1 = (today - datetime.timedelta(days=days+N)).strftime('%Y%m%d')
    startDayStr2 = (today - datetime.timedelta(days=days+N-1)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=N)).strftime('%Y%m%d')

    print('查询日期：',startDayStr1,'~',endDayStr1,'和',startDayStr2,'~',endDayStr2)
    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr1,endDayStr2,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMediaNew(startDayStr1,endDayStr2,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))
    df = df.fillna(0)

    mediaList = df['media'].unique().tolist()

    for media in mediaList:
        if media == 'organic':
            continue

        mediaDf = df[df['media']==media]

        # 计算ROI7D，并将两个周期的内容拼在一起
        df1 = mediaDf[mediaDf['install_date']<=endDayStr1].groupby(['geoGroup']).agg(
            {
                'cost':'sum',
                'revenue_7d':'sum'
            }
        ).reset_index()
        df1['ROI7D'] = df1['revenue_7d']/df1['cost']

        df2 = mediaDf[mediaDf['install_date']>=startDayStr2].groupby(['geoGroup']).agg(
            {
                'cost':'sum',
                'revenue_7d':'sum'
            }
        ).reset_index()
        df2['ROI7D'] = df2['revenue_7d']/df2['cost']

        retDf = pd.merge(df1,df2,on=['geoGroup'],how='outer',suffixes=('_1','_2'))
        
        kpiDf = pd.DataFrame(list(kpi.items()), columns=['geoGroup', 'KPI'])
        retDf = pd.merge(retDf,kpiDf,on=['geoGroup'],how='left')
        retDf['ROI7D环比'] = (retDf['ROI7D_2'] - retDf['ROI7D_1'])/retDf['ROI7D_1']
        retDf[f'ROI7D与KPI比较_1'] = (retDf['ROI7D_1'] - retDf['KPI'])/retDf['KPI']
        retDf[f'ROI7D与KPI比较_2'] = (retDf['ROI7D_2'] - retDf['KPI'])/retDf['KPI']

        retDf['cost环比'] = (retDf['cost_2'] - retDf['cost_1'])/retDf['cost_1']

        retDf = retDf[['geoGroup','ROI7D_1','ROI7D_2','ROI7D环比','KPI','cost_1','cost_2','cost环比','ROI7D与KPI比较_1','ROI7D与KPI比较_2']]
        retDf.rename(columns={
            'geoGroup':'国家',
            'ROI7D_1':f'ROI7D {startDayStr1}~{endDayStr1}',
            'ROI7D_2':f'ROI7D {startDayStr2}~{endDayStr2}',
            'cost_1':f'{startDayStr1}~{endDayStr1}花费',
            'cost_2':f'{startDayStr2}~{endDayStr2}花费',
            'ROI7D与KPI比较_1':f'ROI7D与KPI比较 {startDayStr1}~{endDayStr1}',
            'ROI7D与KPI比较_2':f'ROI7D与KPI比较 {startDayStr2}~{endDayStr2}'
        },inplace=True)

        # 格式整理
        retDf[f'ROI7D {startDayStr1}~{endDayStr1}'] = retDf[f'ROI7D {startDayStr1}~{endDayStr1}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf[f'ROI7D {startDayStr2}~{endDayStr2}'] = retDf[f'ROI7D {startDayStr2}~{endDayStr2}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf['ROI7D环比'] = retDf['ROI7D环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf['KPI'] = retDf['KPI'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf[f'{startDayStr1}~{endDayStr1}花费'] = retDf[f'{startDayStr1}~{endDayStr1}花费'].apply(lambda x:'%.2f'%x)
        retDf[f'{startDayStr2}~{endDayStr2}花费'] = retDf[f'{startDayStr2}~{endDayStr2}花费'].apply(lambda x:'%.2f'%x)
        retDf['cost环比'] = retDf['cost环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf[f'ROI7D与KPI比较 {startDayStr1}~{endDayStr1}'] = retDf[f'ROI7D与KPI比较 {startDayStr1}~{endDayStr1}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf[f'ROI7D与KPI比较 {startDayStr2}~{endDayStr2}'] = retDf[f'ROI7D与KPI比较 {startDayStr2}~{endDayStr2}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

        filename = getFilename(f'report2_{media}','csv')
        retDf.to_csv(filename,index=False)
        print('report2 done,save to',filename)

# 即自然量比例分析，也是针对第一段的一个补充
def reportOrganic(days = 7):
    print('reportOrganic')
    today = datetime.datetime.utcnow()

    # N 是获得满N日数据的周期
    N = 7
    
    startDayStr1 = (today - datetime.timedelta(days=2*(days-1)+N+1)).strftime('%Y%m%d')
    endDayStr1 = (today - datetime.timedelta(days=days+N)).strftime('%Y%m%d')
    startDayStr2 = (today - datetime.timedelta(days=days+N-1)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=N)).strftime('%Y%m%d')

    print('查询日期：',startDayStr1,'~',endDayStr1,'和',startDayStr2,'~',endDayStr2)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMediaNew(startDayStr1,endDayStr2,directory)

    df1 = revenueDf.loc[revenueDf['install_date'] <= endDayStr1].groupby(['geoGroup','media']).agg(
        {
            'revenue_7d':'sum'
        }
    ).reset_index()

    # 计算自然量占比
    df1['7日回收占比'] = df1['revenue_7d']/df1.groupby(['geoGroup'])['revenue_7d'].transform('sum')
    # organicDf1 = df1[df1['media']=='organic'].copy()

    df2 = revenueDf.loc[revenueDf['install_date'] >= startDayStr2].groupby(['geoGroup','media']).agg(
        {
            'revenue_7d':'sum'
        }
    ).reset_index()

    # 计算自然量占比
    df2['7日回收占比'] = df2['revenue_7d']/df2.groupby(['geoGroup'])['revenue_7d'].transform('sum')
    # organicDf2 = df2[df2['media']=='organic'].copy()

    retDf = pd.merge(df1,df2,on=['geoGroup','media'],how='outer',suffixes=('_1','_2'))
    retDf['7日回收占比环比'] = (retDf['7日回收占比_2'] - retDf['7日回收占比_1'])/retDf['7日回收占比_1']

    retDf = retDf[['geoGroup','media','7日回收占比_1','7日回收占比_2','7日回收占比环比']]
    retDf = retDf.sort_values(by=['geoGroup','media'],ascending=[True,False])
    retDf.rename(columns={
        'geoGroup':'国家',
        'media':'媒体',
        '7日回收占比_1':f'7日回收占比 {startDayStr1}~{endDayStr1}',
        '7日回收占比_2':f'7日回收占比 {startDayStr2}~{endDayStr2}',
        '7日回收占比环比':'7日回收占比环比'
    },inplace=True)

    # 格式整理
    retDf[f'7日回收占比 {startDayStr1}~{endDayStr1}'] = retDf[f'7日回收占比 {startDayStr1}~{endDayStr1}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    retDf[f'7日回收占比 {startDayStr2}~{endDayStr2}'] = retDf[f'7日回收占比 {startDayStr2}~{endDayStr2}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    retDf['7日回收占比环比'] = retDf['7日回收占比环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))

    filename = getFilename(f'reportOrganic1','csv')
    retDf.to_csv(filename,index=False)
    print('reportOrganic done,save to',filename)

    # 
    retDf = retDf[retDf['媒体']=='organic']
    filename = getFilename(f'reportOrganic2','csv')
    retDf.to_csv(filename,index=False)
    print('reportOrganic done,save to',filename)

# 测试融合归因结论是否已经准备好 
def check(endDayStr):
    sql = f'''
        SELECT
            *
        FROM
            rg_bi.topwar_ios_funplus02_adv_uid_mutidays_campaign2
        WHERE
            day = '{endDayStr}'
        LIMIT 10
        ;
    '''
    print(sql)
    df = execSql(sql)
    if len(df) == 0:
        print('没有数据')
        return False
    
    return True



# 第三段，针对最近几天的数据，进行分析
# 直接使用24小时的数据，不进行任何推测
def report3():
    print('report3')
    today = datetime.datetime.utcnow()

    # 尝试获得T-2的数据，如果没有，就使用T-3的数据
    endDayStr = (today - datetime.timedelta(days=2)).strftime('%Y%m%d')
    if not check(endDayStr):
        print('T-2 数据无法获得')
        endDayStr = (today - datetime.timedelta(days=3)).strftime('%Y%m%d')

    print('查询日期：',endDayStr)

    endDayStr2 = endDayStr
    # 从目前可以获得的时间，一直到T-7（已获得完整7日收入数据的时间）是目前周期
    startDayStr2 = (today - datetime.timedelta(days=7-1)).strftime('%Y%m%d')

    # days = 天数 endDayStr2 - startDayStr2
    days = (datetime.datetime.strptime(endDayStr2,'%Y%m%d') - datetime.datetime.strptime(startDayStr2,'%Y%m%d')).days + 1
    endDayStr1 = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    startDayStr1 = (today - datetime.timedelta(days=7+days-1)).strftime('%Y%m%d')

    print('查询日期：',startDayStr1,'~',endDayStr1,'和',startDayStr2,'~',endDayStr2)

    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr1,endDayStr2,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMediaNew(startDayStr1,endDayStr2,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))
    df = df.fillna(0)

    mediaList = df['media'].unique().tolist()

    for media in mediaList:
        if media == 'organic':
            continue

        mediaDf = df[df['media']==media]

        df1 = mediaDf[mediaDf['install_date']<=endDayStr1].groupby(['geoGroup']).agg(
            {
                'cost':'sum',
                'revenue_24h':'sum',
                'revenue_7d':'sum'
            }
        ).reset_index()
        df1['ROI24H'] = df1['revenue_24h']/df1['cost']

        df2 = mediaDf[mediaDf['install_date']>=startDayStr2].groupby(['geoGroup']).agg(
            {
                'cost':'sum',
                'revenue_24h':'sum',
                'revenue_7d':'sum'
            }
        ).reset_index()
        df2['ROI24H'] = df2['revenue_24h']/df2['cost']

        retDf = pd.merge(df1,df2,on=['geoGroup'],how='outer',suffixes=('_1','_2'))
        
        retDf['ROI24H环比'] = (retDf['ROI24H_2'] - retDf['ROI24H_1'])/retDf['ROI24H_1']

        retDf['cost环比'] = (retDf['cost_2'] - retDf['cost_1'])/retDf['cost_1']

        retDf = retDf[['geoGroup','ROI24H_1','ROI24H_2','ROI24H环比','cost_1','cost_2','cost环比']]
        retDf.rename(columns={
            'geoGroup':'国家',
            'ROI24H_1':f'ROI24H {startDayStr1}~{endDayStr1}',
            'ROI24H_2':f'ROI24H {startDayStr2}~{endDayStr2}',
            'cost_1':f'{startDayStr1}~{endDayStr1}花费',
            'cost_2':f'{startDayStr2}~{endDayStr2}花费',
        },inplace=True)

        # 格式整理
        retDf[f'ROI24H {startDayStr1}~{endDayStr1}'] = retDf[f'ROI24H {startDayStr1}~{endDayStr1}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf[f'ROI24H {startDayStr2}~{endDayStr2}'] = retDf[f'ROI24H {startDayStr2}~{endDayStr2}'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        retDf['ROI24H环比'] = retDf['ROI24H环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        
        retDf[f'{startDayStr1}~{endDayStr1}花费'] = retDf[f'{startDayStr1}~{endDayStr1}花费'].apply(lambda x:'%.2f'%x)
        retDf[f'{startDayStr2}~{endDayStr2}花费'] = retDf[f'{startDayStr2}~{endDayStr2}花费'].apply(lambda x:'%.2f'%x)
        retDf['cost环比'] = retDf['cost环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
        
        filename = getFilename(f'report3_{media}','csv')
        retDf.to_csv(filename,index=False)
        print('report3 done,save to',filename)

def text3():
    mediaList = ['bytedanceglobal','facebook','google']
    for media in mediaList:
        filename = getFilename(f'report3_{media}','csv')
        mediaDf = pd.read_csv(filename)
        # 备份一份，不改变格式
        mediaDfCopy = mediaDf.copy()
        mediaDf['ROI24H环比'] = mediaDf['ROI24H环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf['cost环比'] = mediaDf['cost环比'].apply(lambda x:float(x[:-1])/100)

        ret1 = ''
        ret2 = ''

        for i in range(len(mediaDf)):
            costOp = '下降' if mediaDf['cost环比'].iloc[i] < 0 else '上升'
            if mediaDf['ROI24H环比'].iloc[i] > 0.5:
                ret2 += f'''{mediaDf["国家"].iloc[i]} ROI24H为{mediaDfCopy.iloc[i,2]},环比上升{mediaDfCopy['ROI24H环比'].iloc[i]}，花费{costOp}了{mediaDfCopy['cost环比'].iloc[i]}。\n'''
            if mediaDf['ROI24H环比'].iloc[i] < -0.5:
                ret1 += f'''{mediaDf["国家"].iloc[i]} ROI24H为{mediaDfCopy.iloc[i,2]},环比下降{mediaDfCopy['ROI24H环比'].iloc[i]}，花费{costOp}了{mediaDfCopy['cost环比'].iloc[i]}。\n'''
            
        filename = getFilename(f'report3Text_{media}_1','txt')
        with open(filename,'w') as f:
            f.write(ret1)

        filename = getFilename(f'report3Text_{media}_2','txt')
        with open(filename,'w') as f:
            f.write(ret2)

import rpyc
import json
from src.report.report.aiText import aiText1_1,aiText1_2,aiText1_3
# 尝试用AI来总结，并给出文案
def ai1():
    conn = rpyc.connect("192.168.40.62", 10002,config={"sync_request_timeout": 120})
    report1Text = '下面是本周的数据报告的csv\n'
    filename = getFilename('report1','csv')
    with open(filename,'r') as f:
        report1Text += f.read()
    message = [
        {"role":"system","content":"You are an AI assistant that helps people find information."},
        {"role":"user","content":aiText1_1},
        {"role":"user","content":aiText1_2},
        {"role":"user","content":aiText1_3},
        {"role":"user","content":report1Text},
    ]
    message_str = json.dumps(message)  # 将message转换为字符串
    x = conn.root.getAiResp(message_str)
    print(x)
    
# 直接写代码措辞，不用AI
def text1():
    filename = getFilename('report1','csv')
    df = pd.read_csv(filename)

    # 本周（20231228~20240103）里程碑达标花费为876211.80，环比上周（20231221~20231227）上升95.04%。
    # range1 是第一列截取，从类似'20231221~20231227花费'，截取到'20231221~20231227'
    range1 = df.columns[1][:-2]
    range2 = df.columns[3][:-2]
    ret1 = f'本周期（{range2}）里程碑达标花费为{df.iloc[-1,3]}，环比上周期（{range1}）上升{df.iloc[-1,5]}。\n'

    filename = getFilename('report1Text_1','txt')
    with open(filename,'w') as f:
        f.write(ret1)

    ret2 = ''
    # 环比阈值
    threshold = 0.5
    df2 = df.copy()
    # 花费环比 从类似 '0.9504%'，截取到 0.9504，再转换为float
    df2['花费环比'] = df2['花费环比'].apply(lambda x:float(x[:-1])/100)
    if len(df2) > 1:
        ret2 += '其中环比变化较大的国家：\n'
        for i in range(len(df)):
            if df.iloc[i,0] == '所有国家汇总':
                continue
            if df2.iloc[i,5] > threshold:
                ret2 += f'\t{df.iloc[i,0]}国家达标花费为{df.iloc[i,3]}，环比上升{df.iloc[i,5]}。\n'
            if df2.iloc[i,5] < -1 * threshold:
                ret2 += f'\t{df.iloc[i,0]}国家达标花费为{df.iloc[i,3]}，环比下降{df.iloc[i,5]}。\n'
    filename = getFilename('report1Text_2','txt')
    with open(filename,'w') as f:
        f.write(ret2)


    ret3 = ''
    df2.iloc[:,2] = df2.iloc[:,2].apply(lambda x:float(x[:-1])/100)
    df2.iloc[:,4] = df2.iloc[:,4].apply(lambda x:float(x[:-1])/100)
    df2.iloc[:,6] = df2.iloc[:,6].apply(lambda x:float(x[:-1])/100)
        
    for i in range(len(df)):
        if df.iloc[i,0] == '所有国家汇总':
            continue
        ok1 = True
        ok2 = True
        if df2.iloc[i,4] < df2.iloc[i,6]:
            ok1 = False
        if df2.iloc[i,2] < df2.iloc[i,6]:
            ok2 = False
        if ok1 == False or ok2 == False:
            ret3 += f'{df.iloc[i,0]} '
            if ok2 == False:
                ret3 += f'上周期ROI7D为{df.iloc[i,2]}，未达标。'
            else:
                ret3 += f'上周期ROI7D为{df.iloc[i,2]}，达标。'

            if ok1 == False:
                ret3 += f'本周期ROI7D为{df.iloc[i,4]}，未达标。存在一定风险。\n'
            else:
                ret3 += f'本周期ROI7D为{df.iloc[i,4]}，达标。风险减小\n'

    filename = getFilename('report1Text_3','txt')
    with open(filename,'w') as f:
        f.write(ret3)

    return

def text1Fix():
    filename = getFilename('report1Fix','csv')
    df = pd.read_csv(filename)
    dfCopy = df.copy()
    df['cost0'] = dfCopy.iloc[:,-3]
    df['cost1'] = dfCopy.iloc[:,-2]
    df['cost2'] = dfCopy.iloc[:,-1]
    df['cost 1-0'] = df['cost1'] - df['cost0']
    df['cost 2-1'] = df['cost2'] - df['cost1']
    df['花费环比'] = (df['cost 2-1'] - df['cost 1-0'])/df['cost 1-0']
    df['花费环比Str'] = df['花费环比'].apply(lambda x: '0%' if pd.isnull(x) else '%.2f%%' % (x*100))
    # print(df)
    LCBStartDateStr = df.columns[2][:8]
    LCBEndDateStr = df.columns[6][9:-2]
    range1 = df.columns[8][:-2]
    range2 = df.columns[10][:-2]

    # 计算LCBEndDate到LCBStartDate的天数
    LCBStartDate = datetime.datetime.strptime(LCBStartDateStr,'%Y%m%d')
    LCBEndDate = datetime.datetime.strptime(LCBEndDateStr,'%Y%m%d')
    LCBDays = (LCBEndDate - LCBStartDate).days + 1

    ret1 = f'目前里程碑于{LCBStartDateStr}开始，截止目前满7日数据（{LCBEndDateStr}），共计{LCBDays}天。\n'
    ret1 += f'本周期（{range2}）里程碑达标花费增长为{df["cost 2-1"].iloc[-1]:.2f}，环比上周期（{range1}）上升{df["花费环比Str"].iloc[-1]}。\n'

    print(ret1)
    filename = getFilename('report1Text_1Fix','txt')
    with open(filename,'w') as f:
        f.write(ret1)

    ret2 = ''
    # 环比阈值
    threshold = 0.5
    df2 = df.copy()
    # 花费环比 目前第12列 从类似 '0.9504%'，截取到 0.9504，再转换为float
    df2.iloc[:,12] = df2.iloc[:,12].apply(lambda x:float(x[:-1])/100)
    if LCBDays >= 14:
        if len(df2) > 1:
            ret2 += '达标花费环比变化较大的国家：\n'
            for i in range(len(df)):
                if df.iloc[i,0] == '所有国家汇总':
                    continue
                if df2.iloc[i,-2] > threshold:
                    ret2 += f'\t{df.iloc[i,0]}国家达标花费为{df.iloc[i,-3]:.2f}，环比上升{df.iloc[i,-1]}。\n'
                if df2.iloc[i,12] < -1 * threshold:
                    ret2 += f'\t{df.iloc[i,0]}国家达标花费为{df.iloc[i,-3]:.2f}，环比下降{df.iloc[i,-1]}。\n'
    else:
        ret2 += '目前里程碑数据不足14天（满7日数据），暂时不细分国家环比变化。\n'
    print(ret2)
    filename = getFilename('report1Text_2Fix','txt')
    with open(filename,'w') as f:
        f.write(ret2)

    ret3 = ''
    # KPI
    df2.iloc[:,1] = df2.iloc[:,1].apply(lambda x:float(x[:-1])/100)
    # 上周期结束时ROI7D
    df2.iloc[:,5] = df2.iloc[:,5].apply(lambda x:float(x[:-1])/100)
    # 本周期结束时ROI7D
    df2.iloc[:,7] = df2.iloc[:,7].apply(lambda x:float(x[:-1])/100)
    # 上周期ROI7D
    df2.iloc[:,9] = df2.iloc[:,9].apply(lambda x:float(x[:-1])/100)
    # 本周期ROI7D
    df2.iloc[:,11] = df2.iloc[:,11].apply(lambda x:float(x[:-1])/100)
    
    # 比较危险，需要关注的情况：
    # 1、本周期结束时ROI7D低于KPI，且本周期ROI7D低于KPI，并且花费环比上升，存在风险
    # 2、本周期结束时ROI7D低于KPI，且本周期ROI7D高于KPI，正在好转，但是仍旧有风险
    # 3、本周期结束时ROI7D高于KPI，但是不够高，且本周期ROI7D低于KPI，有一定风险

    for i in range(len(df)):
        if df.iloc[i,0] == '所有国家汇总':
            continue

        costOp = '下降' if df2.iloc[i,12] < 0 else '上升'
        # 本周期结束时ROI7D低于KPI
        if df2.iloc[i,7] < df2.iloc[i,1] :
            ret3 += f'{df.iloc[i,0]} '
            # 本周期ROI7D低于KPI
            if df2.iloc[i,11] < df2.iloc[i,1]:
                ret3 += f'里程碑开始至本周期结束的满7ROI为{df.iloc[i,7]}，不达标。本周期内满7ROI为{df.iloc[i,11]}依旧不达标，花费环比{costOp}了{df.iloc[i,12]}，存在风险。\n'
            else:
                ret3 += f'里程碑开始至本周期结束的满7ROI为{df.iloc[i,7]}，不达标。本周期内满7ROI为{df.iloc[i,11]}达标，正在好转，但是仍旧有风险。\n'
        # 本周期结束时ROI7D高于KPI
        if df2.iloc[i,7] > df2.iloc[i,1] :
            # 但是不够高
            if df2.iloc[i,7] < df2.iloc[i,1] * 1.05:
                kpiHB = (df2.iloc[i,7] - df2.iloc[i,1])/df2.iloc[i,1]
                # 且本周期ROI7D低于KPI
                if df2.iloc[i,11] < df2.iloc[i,1]:
                    ret3 += f'{df.iloc[i,0]} 里程碑开始至本周期结束的满7ROI为{df.iloc[i,7]}，比KPI高了{kpiHB*100:.2f}%。本周期内ROI7D为{df.iloc[i,11]}不达标，花费环比{costOp}了{df.iloc[i,12]}，存在潜在风险。\n'
                    
    print(ret3)
    filename = getFilename('report1Text_3Fix','txt')
    with open(filename,'w') as f:
        f.write(ret3)

    return

def textOrganic():
    ret1 = ''
    ret2 = ''

    filename = getFilename('report1Fix','csv')
    df1 = pd.read_csv(filename)

    filename = getFilename('reportOrganic2','csv')
    dfO = pd.read_csv(filename)

    costHBCol = df1.columns[12]
    print('花费环比列名：',costHBCol)


    df1[costHBCol] = df1[costHBCol].apply(lambda x:float(x[:-1])/100)
    dfO['7日回收占比环比'] = dfO['7日回收占比环比'].apply(lambda x:float(x[:-1])/100)

    # 当一个国家花费环比变高，并且自然量回收占比环比变低的时候，代表媒体的表现在变好，可能是行情变好
    # 当一个国家花费环比变低，并且自然量回收占比环比变高的时候，代表媒体的表现在变差，可能是行情变差
    # 变化设定阈值
    threshold1 = 0.5
    thresholdO = 0.2

    for i in range(len(df1)):
        if df1.iloc[i,0] == '所有国家汇总':
            continue
        if df1.iloc[i,12] > threshold1 and dfO.iloc[i,4] < -1 * thresholdO:
            ret2 += f'{df1.iloc[i,0]} 达标花费环比上升{df1.iloc[i,12]*100:.2f}%，自然量回收占比环比下降{dfO.iloc[i,4]*100:.2f}%，媒体表现变好。\n'
        if df1.iloc[i,12] < -1 * threshold1 and dfO.iloc[i,4] > thresholdO:
            ret1 += f'{df1.iloc[i,0]} 达标花费环比下降{df1.iloc[i,12]*100:.2f}%，自然量回收占比环比上升{dfO.iloc[i,4]*100:.2f}%，媒体表现变差。\n'
    
    # print(ret)
    filename = getFilename('reportOrganicText_1','txt')
    with open(filename,'w') as f:
        f.write(ret1)

    filename = getFilename('reportOrganicText_2','txt')
    with open(filename,'w') as f:
        f.write(ret2)

    return

def text2():
    mediaList = ['bytedanceglobal','facebook','google']
    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(f'report2_{media}','csv'))
        # 备份一份，不改变格式
        mediaDfCopy = mediaDf.copy()
        # 整理，先将mediaDf中的'ROI7D环比','cost环比',和最后两列中的类似 '0.9504%'，截取到 0.9504，再转换为float，里面有一些转换失败的填0
        mediaDf['ROI7D环比'] = mediaDf['ROI7D环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf['cost环比'] = mediaDf['cost环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,8] = mediaDf.iloc[:,8].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,9] = mediaDf.iloc[:,9].apply(lambda x:float(x[:-1])/100)
        
        ret1 = ''
        ret2 = ''

        # 目前主要关注：
        # 1、ROI上周期 和 ROI本周期 都低于KPI，且cost环比上升，存在风险
        # 2、ROI7D环比上升，且cost环比上升，表现优秀
        # 其他的再往里添加

        for i in range(len(mediaDf)):
            if mediaDf.iloc[i,0] == '所有国家汇总':
                continue
            if mediaDf.iloc[i,8] < -0.5 and mediaDf.iloc[i,8] < -0.5 and mediaDf.iloc[i,7] > 0.5:
                ret1 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比上升{mediaDfCopy.iloc[i,7]}，存在风险。\n'
            if mediaDf.iloc[i,3] > 0.5 and mediaDf.iloc[i,7] > 0.2:
                if mediaDf.iloc[i,3] > 10:
                    # 这个可能是异常数据，不用管
                    pass
                else:
                    ret2 += f'{mediaDf.iloc[i,0]} ROI7D环比上升{mediaDfCopy.iloc[i,3]}，cost环比上升{mediaDfCopy.iloc[i,7]}，在向好的方向发展。\n'
        
        filename = getFilename(f'report2Text_{media}_1','txt')
        with open(filename,'w') as f:
            f.write(ret1)

        filename = getFilename(f'report2Text_{media}_2','txt')
        with open(filename,'w') as f:
            f.write(ret2)

# 获得自然量占比的较大值与较小值
# 按照国家，统计自然量占比的最大值与最小值
# 目前的简单做法是直接获取上一周期和本周期数据，将较大的值作为最大值，较小的值作为最小值
def getOrganicRateMaxMin():
    df = pd.read_csv(getFilename('reportOrganic2','csv'))
    c1 = df.columns[2]
    c2 = df.columns[3]
    # 将原本的类似 0.1% 的数据转换成 0.001
    df[c1] = df[c1].apply(lambda x:float(x[:-1])/100)
    df[c2] = df[c2].apply(lambda x:float(x[:-1])/100)

    df['organic_rate_max'] = df[[c1,c2]].max(axis=1)
    df['organic_rate_min'] = df[[c1,c2]].min(axis=1)
    df = df[['国家','organic_rate_max','organic_rate_min']]
    return df

# 用标准KPI和自然量占比，推算出不含自然量的KPI的最大值与最小值
# 基础公式：KPIMax = KPI*（1-自然量占比最小值）；KPIMin = KPI*（1-自然量占比最大值）
def getKpiMaxMin():
    kpiDf = pd.DataFrame(list(kpi.items()), columns=['国家', 'KPI'])
    df = getOrganicRateMaxMin()

    df = pd.merge(df,kpiDf,on='国家',how='left')

    df['kpi_max'] = df['KPI'] * (1 - df['organic_rate_min'])
    df['kpi_min'] = df['KPI'] * (1 - df['organic_rate_max'])
    print(df)
    df = df[['国家','kpi_max','kpi_min']]

    return df

def text2Fix():
    kpiMaxMin = getKpiMaxMin()

    mediaList = ['bytedanceglobal','facebook','google']
    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(f'report2_{media}','csv'))

        mediaDfCopy = mediaDf.copy()

        mediaDf['ROI7D环比'] = mediaDf['ROI7D环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf['cost环比'] = mediaDf['cost环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,1] = mediaDf.iloc[:,1].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,2] = mediaDf.iloc[:,2].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,8] = mediaDf.iloc[:,8].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,9] = mediaDf.iloc[:,9].apply(lambda x:float(x[:-1])/100)
        
        mediaDf = mediaDf.merge(kpiMaxMin,on='国家',how='left')

        ret1 = ''
        ret2 = ''

        # 获得mediaDf中列kpi_min的列索引
        kpi_min_index = mediaDf.columns.get_loc('kpi_min')
        kpi_max_index = mediaDf.columns.get_loc('kpi_max')

        # print(mediaDf)
        for i in range(len(mediaDf)):
            if mediaDf.iloc[i,0] == '所有国家汇总':
                continue
            costOp = '下降' if mediaDf.iloc[i,7] < 0 else '上升'
            if mediaDf.iloc[i,1] < mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] < mediaDf.iloc[i,kpi_min_index]:
                # 上周期与本周期 都不达标
                ret1 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，存在风险。\n'
            elif mediaDf.iloc[i,1] < mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] >= mediaDf.iloc[i,kpi_max_index]:
                # 上周期不达标，本周期达标
                ret2 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，有所好转。\n'
            elif mediaDf.iloc[i,1] >= mediaDf.iloc[i,kpi_max_index] and mediaDf.iloc[i,2] < mediaDf.iloc[i,kpi_min_index]:
                # 上周期达标，本周期不达标
                ret1 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，存在风险。\n'
            elif mediaDf.iloc[i,1] >= mediaDf.iloc[i,kpi_max_index] and mediaDf.iloc[i,2] >= mediaDf.iloc[i,kpi_max_index]:
                # 上周期与本周期 都达标
                ret2 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，表现良好。\n'
            else:
                # 不做评价
                pass

        filename = getFilename(f'report2Text_{media}_1','txt')
        with open(filename,'w') as f:
            f.write(ret1)

        filename = getFilename(f'report2Text_{media}_2','txt')
        with open(filename,'w') as f:
            f.write(ret2)


from src.report.feishu.report2 import main as feishuMain
from src.report.feishu.feishu import sendMessageDebug
def main(days = 7):
    # 获得目前的UTC0日期，格式20231018
    today = datetime.datetime.utcnow()
    todayStr = today.strftime('%Y%m%d')
    print('今日日期：',todayStr)

    global directory
    directory = f'/src/data/report/海外iOS里程碑进度日报_{todayStr}'

    if not os.path.exists(directory):
        os.makedirs(directory)

    
    retryMax = 3
    for _ in range(retryMax):
        try:
            report1Fix(days)
            text1Fix()

            reportOrganic(days)
            textOrganic()

            report2(days)
            # text2Fix 需要自然量占比数据，所以要在reportOrganic之后
            text2Fix()
            
            report3()
            text3()

            feishuMain(directory)
            break
        except Exception as e:
            print(e)
            sendMessageDebug('报告生成失败'+str(e))
            # 等待5分钟
            time.sleep(300)
            continue

if __name__ == '__main__':
    main()
    
    

    
