# 改用每个国家 的 SLG Top3 来计算，排除lastwar

# 尝试使用lastwar bi数据作为权重，进行权重汇总，得到各媒体，各国家群组的汇总

# 然后再观察 lastwar 与 top3 的数据，总结出每个媒体 + 国家分组 或者 汇总至 国家分组 的相关系数。与之前的市场调查数据做对比，看看是否有相关性。

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

# 这里面调用了getRevenueTopN，是不包含lastwar的
def getSlgTop3():
    # 先找到各国的流水 top3 app_id
    sdData = getDataFromSt()
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

    top3Df = getRevenueTopN(sdData,None, n = 3)
    return top3Df

    

def getAllData():
    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]
    countries = ['US','KR','JP','TW']
    # 下面是国家抽样，主要取人口比较多的国家
    gccCountries = ['SA']
    t1Countries = ['DE','FR','GB']
    t2Countries = ['ID','TR','TH']
    t3Countries = ['IN','BR','MX']
    countries = countries + gccCountries + t1Countries + t2Countries + t3Countries

    # 先找到各国的流水 top3 app_id
    top3Df = getSlgTop3()

    top3Df = top3Df[['countryGroup','app_id']]
    top3Df = top3Df[top3Df['countryGroup'].isin(countries)]
    top3Df.rename(columns={'countryGroup':'country'},inplace=True)

    # top3Df 中每个国家都加入一个 lastwar 的 app_id
    lwDf = pd.DataFrame()
    for country in countries:
        lwDf = pd.concat([lwDf,pd.DataFrame({'country':[country],'app_id':[lwAppId]})],ignore_index=True)
    top3Df = pd.concat([top3Df,lwDf],ignore_index=True)

    slgAppIdList = top3Df['app_id'].unique().tolist()
    
    df = pd.DataFrame()

    dateList = [
        {'name':'202401','start_date':'2024-01-01','end_date':'2024-01-31'},
        {'name':'202402','start_date':'2024-02-01','end_date':'2024-02-29'},
        {'name':'202403','start_date':'2024-03-01','end_date':'2024-03-31'},
        {'name':'202404','start_date':'2024-04-01','end_date':'2024-04-30'},
        {'name':'202405','start_date':'2024-05-01','end_date':'2024-05-31'},
        {'name':'202406','start_date':'2024-06-01','end_date':'2024-06-30'},
    ]

    l = len(slgAppIdList)//5
    if len(slgAppIdList) % 5 > 0:
        l += 1

    for i in range(l):
        for d in dateList:
            filename = f'/src/data/ad2Top{i}_{d["name"]}.csv'
            if os.path.exists(filename):
                print('已存在%s'%filename)
                adDf = pd.read_csv(filename)
            else:
                minIndex = i * 5
                maxIndex = minIndex + 5

                slgAppIdList5 = slgAppIdList[minIndex:maxIndex]
                
                adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date=d['start_date'],end_date=d['end_date'])
                adDf.to_csv(filename,index=False)
            adDf = adDf.merge(top3Df,on=['app_id','country'],how='inner')
            df = pd.concat([df,adDf],ignore_index=True)
    return df

def main1():
    df = getAllData()
    networkListAll = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]
    # 媒体分组0，没有第三方版位
    networkList0 = ['Facebook','Instagram','TikTok','Youtube']
    # 媒体分组1，都是第三方版位
    networkList1 = ['Admob','Applovin','Meta Audience Network']
    
    # 暂时先放弃第三方版位
    # df = df[df['network'].isin(networkList0)]

    df = df.sort_values(by=['country','network','app_id'])
    df['name'] = 'unknown'
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
    
    # 按 country 和 network 分组
    grouped = df.groupby(['country', 'network'])

    results = []

    # 遍历每个分组
    for (country, network), group in grouped:
        # 按 date 排序
        group = group.sort_values(by='date')
        
        # 透视表，将 app_id 作为列，date 作为索引，sov 作为值
        pivot_table = group.pivot(index='date', columns='app_id', values='sov')
        pivot_table.fillna(0, inplace=True)

        # 删除所有app的sov均为0的行
        pivot_table = pivot_table.loc[~(pivot_table == 0).all(axis=1)]

        # 如果过滤后数据不足3列，跳过该组
        if pivot_table.shape[0] < 3:
            continue
        
        pivot_table.to_csv(f'/src/data/ad2_pivot_{country}_{network}.csv', index=True)
        
        # 计算相关系数矩阵
        corr_matrix = pivot_table.corr()

        # 获取与 lwAppId 相关的相关系数
        if lwAppId in corr_matrix.columns:
            for app_id in corr_matrix.columns:
                if app_id != lwAppId:
                    correlation = corr_matrix.at[lwAppId, app_id]
                    results.append({'country': country, 'network': network, 'app_id': app_id, 'correlation': correlation})

    result_df = pd.DataFrame(results)
    # 合并结果和app_id_name_df
    final_df = result_df.merge(app_id_name_df, on='app_id', how='left')
    
    # 如果 app_name 是 NaN，则替换为 'unknown'
    final_df['app_name'] = final_df['app_name'].fillna('unknown')
    
    # 删除 app_id 列，因为我们只需要 app_name
    final_df = final_df.drop(columns=['app_id'])

    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            final_df.loc[final_df['country'] == country, 'countryGroup'] = countryGroup['name']


    final_df = final_df[['country', 'countryGroup', 'network', 'app_name', 'correlation']]
    # print(final_df)
    final_df.to_csv('/src/data/ad2_final.csv', index=False)

    # 新增逻辑：将除 lastwar 以外的应用，按照国家、媒体、时间分组平均，然后再与 lastwar 计算相关性
    additional_results = []

    # 遍历每个分组
    for (country, network), group in grouped:
        # 按 date 排序
        group = group.sort_values(by='date')
        
        # 透视表，将 app_id 作为列，date 作为索引，sov 作为值
        pivot_table = group.pivot(index='date', columns='app_id', values='sov')
        pivot_table.fillna(0, inplace=True)

        # 删除所有app的sov均为0的行
        pivot_table = pivot_table.loc[~(pivot_table == 0).all(axis=1)]

        # 如果过滤后数据不足3列，跳过该组
        if pivot_table.shape[0] < 3:
            continue

        # 计算除 lastwar 以外的应用的均值
        if lwAppId in pivot_table.columns:
            lastwar_series = pivot_table[lwAppId]
            other_apps_mean = pivot_table.drop(columns=[lwAppId]).mean(axis=1)
            
            # 计算相关性
            correlation = lastwar_series.corr(other_apps_mean)
            additional_results.append({'country': country, 'network': network, 'app_name': 'average', 'correlation': correlation})

    additional_df = pd.DataFrame(additional_results)
    
    # 添加国家分组信息
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            additional_df.loc[additional_df['country'] == country, 'countryGroup'] = countryGroup['name']

    additional_df = additional_df[['country', 'countryGroup', 'network', 'app_name', 'correlation']]
    
    print(additional_df)
    additional_df.to_csv('/src/data/ad2_final2.csv', index=False)

if __name__ == '__main__':
    main1()
    