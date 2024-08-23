# lastwar主要国家流水差距计算，结合主要媒体展示比例，推断流水差距原因。

# 找到各国的SLG游戏收入Top1 的 app_id（排除 lastwar）
# 计算与lastwar的收入差距，并按照收入差距（绝对值）排序，找到主要差距国家

# 获得上述主要国家的主要媒体的展示比例，估算流水差距的原因
# 比如，是广告投放不足（展示占比低），还是广告投放效果不好（展示占比高，但是收入低），还是投放媒体比例不合适等
# 其中广告投放总量，按照一定比例进行折算。比如，按照我们广告在全球的各媒体展示占比，算作媒体权重。

import os
import json
import requests
import pandas as pd

import sys
sys.path.append('/src')

from ad import getAdData
from top3 import getDataFromSt
from market import lwAppId,getCountryGroupList,getRevenueTopN
from src.config import sensortowerToken

def getSlgTop1Revenue():
    sdData = getDataFromSt(startMonth='202404',endMonth='202406')
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    sdData['isSLG'] = False
    sdData.loc[sdData['app_id'].isin(slgAppIdList), 'isSLG'] = True

    sdData = sdData[sdData['isSLG']]

    top1Df = getRevenueTopN(sdData,None, n = 1)
    return top1Df

def getSlgTop3Revenue():
    sdData = getDataFromSt(startMonth='202404',endMonth='202406')
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    sdData['isSLG'] = False
    sdData.loc[sdData['app_id'].isin(slgAppIdList), 'isSLG'] = True

    sdData = sdData[sdData['isSLG']]

    top1Df = getRevenueTopN(sdData,None, n = 3)
    return top1Df


def getLastwarRevenue():
    sdData = getDataFromSt(startMonth='202404',endMonth='202406')
    lwDf = sdData[sdData['app_id'] == lwAppId]
    return lwDf

def main1():
    top1Df = getSlgTop1Revenue()
    # top1Df = top1Df[['countryGroup','revenue']]
    top1Df.rename(columns={'countryGroup':'country'},inplace=True)
    lwDf = getLastwarRevenue()
    lwDf = lwDf[['country','revenue']]

    df = pd.merge(lwDf,top1Df,  on='country', how='left', suffixes=('_lw', '_top1'))
    df['revenue_diff'] = df['revenue_top1'] - df['revenue_lw']
    df = df.sort_values('revenue_diff', ascending=False).reindex()

    app_id_name_dict = {
        '567a0aee0f1225ea0e006fe9': 'Lords Mobile',
        '5cf092a745e8be7323fffd0d': 'State of Survival',
        '5ac2bdddcfc03208313848db': 'Rise of Kingdoms',
        '58bcde8f0211a6f6ab000570': 'Mafia City',
        '5cc98b703ea98357b8ed3ce0': 'topwar',
        '56d6407f64235a7413000001': 'انتقام السلاطين',
        '638ee532480da915a62f0b34': 'whiteout',
        '64075e77537c41636a8e1c58': 'lastwar',
        '5869720d0211a6180f000ebc': 'evony',
        '5ba1a19def71a76da561ca41': 'ageOfOrigins',
        '592a66af850abd359f007606': 'total battle',
    }
    # 创建 app_id 和 app_name 的 DataFrame
    app_id_name_df = pd.DataFrame(list(app_id_name_dict.items()), columns=['app_id', 'app_name'])
    df = df.merge(app_id_name_df, left_on='app_id', right_on='app_id', how='left')
    df = df[['country','app_name','app_id','revenue_lw','revenue_top1','revenue_diff']]
    
    df.to_csv('/src/data/revenue_diff.csv', index=False)

# 计算前3名的收入之和
def main1_fix():
    top3Df = getSlgTop3Revenue()
    top3Df = top3Df.groupby(['countryGroup']).agg({
        'revenue':'sum'
    }).reset_index()
    
    top3Df.rename(columns={'countryGroup':'country'},inplace=True)
    lwDf = getLastwarRevenue()
    lwDf = lwDf[['country','revenue']]

    df = pd.merge(lwDf,top3Df,  on='country', how='left', suffixes=('_lw', '_top1'))
    df['revenue_diff'] = df['revenue_top1'] - df['revenue_lw']
    df = df.sort_values('revenue_diff', ascending=False).reindex()
    df.rename(columns={'revenue_diff':'revenue top3-lw'},inplace=True)
    df['revenue (top3 - lastwar) / lastwar'] = df['revenue top3-lw'] / df['revenue_lw']
    df['revenue (top3 - lastwar) / lastwar'] = df['revenue (top3 - lastwar) / lastwar'].map(lambda x: '%.2f%%'%(x*100))
    df = df[['country','revenue_lw','revenue_top1','revenue top3-lw','revenue (top3 - lastwar) / lastwar']]
    
    df.to_csv('/src/data/revenue_diff3.csv', index=False)

# 不进行收入汇总，为了后续计算广告展示比例
def main1_fix2():
    top3Df = getSlgTop3Revenue()
    # top3Df = top3Df.groupby(['countryGroup']).agg({
    #     'revenue':'sum'
    # }).reset_index()
    
    top3Df.rename(columns={'countryGroup':'country'},inplace=True)
    # lwDf = getLastwarRevenue()
    # lwDf = lwDf[['country','revenue']]

    # df = pd.merge(lwDf,top3Df,  on='country', how='left', suffixes=('_lw', '_top1'))
    # df['revenue_diff'] = df['revenue_top1'] - df['revenue_lw']
    # df = df.sort_values('revenue_diff', ascending=False).reindex()
    # df.rename(columns={'revenue_diff':'revenue top3-lw'},inplace=True)
    # df['revenue (top3 - lastwar) / lastwar'] = df['revenue top3-lw'] / df['revenue_lw']
    # df['revenue (top3 - lastwar) / lastwar'] = df['revenue (top3 - lastwar) / lastwar'].map(lambda x: '%.2f%%'%(x*100))
    # df = df[['country','revenue_lw','revenue_top1','revenue top3-lw','revenue (top3 - lastwar) / lastwar']]
    
    top3Df.to_csv('/src/data/revenue_diff3_fix.csv', index=False)

def main2():
    # 获得主要国家
    revenueDiffDf = pd.read_csv('/src/data/revenue_diff.csv')
    countriesAllowLise = ['US', 'AU', 'CA', 'CN', 'FR', 'DE', 'GB', 'IT', 'JP', 'KR', 'RU', 'AR', 'AT', 'BE', 'BR', 'CL', 'CO', 'DK', 'EC', 'HK', 'IN', 'ID', 'IL', 'LU', 'MY', 'MX', 'NL', 'NZ', 'NO', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'SA', 'SG', 'ZA', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'UA', 'AE', 'VN']
    revenueDiffDf = revenueDiffDf[revenueDiffDf['country'].isin(countriesAllowLise)]
    revenueDiffDf = revenueDiffDf.head(20)
    countries = revenueDiffDf['country'].tolist()
    print(countries)

    lwDf = pd.DataFrame()
    for country in countries:
        lwDf = pd.concat([lwDf,pd.DataFrame({'country':[country],'app_id':[lwAppId],'app_name':'Lastwar'})],ignore_index=True)
    top1Df = pd.concat([revenueDiffDf,lwDf],ignore_index=True)
    
    slgAppIdList = top1Df['app_id'].unique().tolist()
    
    dateList = [
        {'name':'202401','start_date':'2024-01-01','end_date':'2024-01-31'},
        {'name':'202402','start_date':'2024-02-01','end_date':'2024-02-29'},
        {'name':'202403','start_date':'2024-03-01','end_date':'2024-03-31'},
        {'name':'202404','start_date':'2024-04-01','end_date':'2024-04-30'},
        {'name':'202405','start_date':'2024-05-01','end_date':'2024-05-31'},
        {'name':'202406','start_date':'2024-06-01','end_date':'2024-06-30'},
    ]

    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]

    l = len(slgAppIdList)//5
    if len(slgAppIdList) % 5 > 0:
        l += 1

    df = pd.DataFrame()
    for i in range(l):
        for d in dateList:
            filename = f'/src/data/ad3Top1_{i}_{d["name"]}.csv'
            if os.path.exists(filename):
                print('已存在%s'%filename)
                adDf = pd.read_csv(filename)
            else:
                minIndex = i * 5
                maxIndex = minIndex + 5

                slgAppIdList5 = slgAppIdList[minIndex:maxIndex]
                
                adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date=d['start_date'],end_date=d['end_date'])
                adDf.to_csv(filename,index=False)
            
            adDf = adDf.merge(top1Df[['app_id','app_name','country']],on=['app_id','country'],how='inner')
            df = pd.concat([df,adDf],ignore_index=True)
    
    # print(df)
    df = df.sort_values(['country','app_id','date'])
    df.to_csv('/src/data/ad3_main2.csv', index=False)


    # 设定各媒体权重
    # 参考lastwar 202406月 广告展示量
    mediaWeight = {
        'Admob': 7,
        'Applovin': 2,
        'Facebook': 5,
        'Instagram': 5,
        'Meta Audience Network': 0.2,
        'TikTok': 6,
        'Youtube': 7,
    }
    mediaWeightDf = pd.DataFrame(list(mediaWeight.items()), columns=['network', 'weight'])

    # 计算各媒体权重总和
    top1Df = df[df['app_id'] != lwAppId]
    top1Df = top1Df.merge(mediaWeightDf, on='network', how='left')
    top1Df['sov2'] = top1Df['sov'] * top1Df['weight']
    top1Df = top1Df.groupby(['country']).agg(
        {
            'sov2':'sum'
        }
    ).reset_index()

    lwDf = df[df['app_id'] == lwAppId]
    lwDf = lwDf.merge(mediaWeightDf, on='network', how='left')
    lwDf['sov2'] = lwDf['sov'] * lwDf['weight']
    lwDf = lwDf.groupby(['country']).agg(
        {
            'sov2':'sum'
        }
    ).reset_index()

    df = pd.merge(top1Df, lwDf, on='country', how='left', suffixes=('_top1', '_lw'))
    df['sov_diff'] = df['sov2_top1'] - df['sov2_lw']

    revenueDiffDf = revenueDiffDf.merge(df, on='country', how='left')
    revenueDiffDf['revenue_ratio'] = revenueDiffDf['revenue_diff'] / revenueDiffDf['revenue_lw']
    revenueDiffDf['sov_ratio'] = revenueDiffDf['sov_diff'] / revenueDiffDf['sov2_lw']
    revenueDiffDf = revenueDiffDf[['country','app_name','revenue_diff','sov_diff','revenue_ratio','sov_ratio']]
    revenueDiffDf.rename(columns={
        'app_name':'app_name_top1',
        'revenue_diff':'revenue top1 - lastwar',
        'sov_diff':'sov top1 - lastwar',
        'revenue_ratio':'revenue (top1 - lastwar) / lastwar',
        'sov_ratio':'sov (top1 - lastwar) / lastwar',
    },inplace=True)
    revenueDiffDf['revenue (top1 - lastwar) / lastwar'] = revenueDiffDf['revenue (top1 - lastwar) / lastwar'].map(lambda x: '%.2f%%'%(x*100))
    revenueDiffDf['sov (top1 - lastwar) / lastwar'] = revenueDiffDf['sov (top1 - lastwar) / lastwar'].map(lambda x: '%.2f%%'%(x*100))

    print(revenueDiffDf)
    revenueDiffDf.to_csv('/src/data/revenue_diff_main_202404_202406.csv', index=False)

def main2_fix():
    # 获得主要国家
    revenueDiffDf = pd.read_csv('/src/data/revenue_diff3_fix.csv')
    countriesAllowLise = ['US', 'AU', 'CA', 'CN', 'FR', 'DE', 'GB', 'IT', 'JP', 'KR', 'RU', 'AR', 'AT', 'BE', 'BR', 'CL', 'CO', 'DK', 'EC', 'HK', 'IN', 'ID', 'IL', 'LU', 'MY', 'MX', 'NL', 'NZ', 'NO', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'SA', 'SG', 'ZA', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'UA', 'AE', 'VN']
    revenueDiffDf = revenueDiffDf[revenueDiffDf['country'].isin(countriesAllowLise)]
    countries = revenueDiffDf['country'].unique().tolist()

    lwDf = pd.DataFrame()
    for country in countries:
        lwDf = pd.concat([lwDf,pd.DataFrame({'country':[country],'app_id':[lwAppId]})],ignore_index=True)
    top1Df = pd.concat([revenueDiffDf,lwDf],ignore_index=True)
    
    slgAppIdList = top1Df['app_id'].unique().tolist()
    
    dateList = [
        {'name':'202401','start_date':'2024-01-01','end_date':'2024-01-31'},
        {'name':'202402','start_date':'2024-02-01','end_date':'2024-02-29'},
        {'name':'202403','start_date':'2024-03-01','end_date':'2024-03-31'},
        {'name':'202404','start_date':'2024-04-01','end_date':'2024-04-30'},
        {'name':'202405','start_date':'2024-05-01','end_date':'2024-05-31'},
        {'name':'202406','start_date':'2024-06-01','end_date':'2024-06-30'},
    ]

    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]

    l = len(slgAppIdList)//5
    if len(slgAppIdList) % 5 > 0:
        l += 1

    df = pd.DataFrame()
    for i in range(l):
        for d in dateList:
            filename = f'/src/data/ad3Top2_{i}_{d["name"]}.csv'
            if os.path.exists(filename):
                print('已存在%s'%filename)
                adDf = pd.read_csv(filename)
            else:
                minIndex = i * 5
                maxIndex = minIndex + 5

                slgAppIdList5 = slgAppIdList[minIndex:maxIndex]
                
                adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date=d['start_date'],end_date=d['end_date'])
                adDf.to_csv(filename,index=False)
            
            adDf = adDf.merge(top1Df[['app_id','country']],on=['app_id','country'],how='inner')
            df = pd.concat([df,adDf],ignore_index=True)
    
    # print(df)
    df = df.sort_values(['country','app_id','date'])
    df.to_csv('/src/data/ad3_main2_fix.csv', index=False)


    # 设定各媒体权重
    # 参考lastwar 202406月 广告展示量
    mediaWeight = {
        'Admob': 7,
        'Applovin': 2,
        'Facebook': 5,
        'Instagram': 5,
        'Meta Audience Network': 0.2,
        'TikTok': 6,
        'Youtube': 7,
    }
    mediaWeightDf = pd.DataFrame(list(mediaWeight.items()), columns=['network', 'weight'])

    # 计算各媒体权重总和
    top1Df = df[df['app_id'] != lwAppId]
    top1Df = top1Df.merge(mediaWeightDf, on='network', how='left')
    top1Df['sov2'] = top1Df['sov'] * top1Df['weight']
    top1Df = top1Df.groupby(['country']).agg(
        {
            'sov2':'sum'
        }
    ).reset_index()

    lwDf = df[df['app_id'] == lwAppId]
    lwDf = lwDf.merge(mediaWeightDf, on='network', how='left')
    lwDf['sov2'] = lwDf['sov'] * lwDf['weight']
    lwDf = lwDf.groupby(['country']).agg(
        {
            'sov2':'sum'
        }
    ).reset_index()

    df = pd.merge(top1Df, lwDf, on='country', how='left', suffixes=('_top1', '_lw'))

    revenueDf = pd.read_csv('/src/data/revenue_diff3.csv')

    df = df.merge(revenueDf, on='country', how='left')

    df['sov_diff'] = df['sov2_top1'] - df['sov2_lw']
    
    df['sov_ratio'] = df['sov_diff'] / df['sov2_lw']
    df = df[['country','revenue top3-lw','sov_diff','revenue (top3 - lastwar) / lastwar','sov_ratio']]
    df.rename(columns={
        'sov_diff':'sov top3 - lastwar',
        'sov_ratio':'sov (top3 - lastwar) / lastwar',
    },inplace=True)
    df['sov (top3 - lastwar) / lastwar'] = df['sov (top3 - lastwar) / lastwar'].map(lambda x: '%.2f%%'%(x*100))

    df = df.sort_values('revenue top3-lw', ascending=False).reindex()
    df.to_csv('/src/data/revenue_diff_main_202404_202406_fix.csv', index=False)


if __name__ == '__main__':
    # main1()
    # main2()
    main1_fix()
    main1_fix2()
    main2_fix()



