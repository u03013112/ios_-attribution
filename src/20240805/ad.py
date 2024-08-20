
# 获得主要竞品（10个，20个，还是100个） 
# 主要媒体（google、facebook、applovin） 
# 主要国家（US、KR、JP、TW，以及一些比较大的T1、T2、T3国家）
# 的广告展示占比

# 最终是一个列表，拥有列：app_id, media, country, showRate



# 计算US和其他国家的 各媒体 竞争 激烈程度
import os
import json
import requests
import pandas as pd

import sys
sys.path.append('/src')

from market import lwAppId,getCountryGroupList
from src.config import sensortowerToken

# 
def getAdData(app_ids=[],networks=[],countries=[],start_date='2024-06-01',end_date='2024-07-31'):
    # https://api.sensortower.com/v1/unified/ad_intel/network_analysis?app_ids=5570fc1cfe55ad5778000621&start_date=2024-01-01&end_date=2024-06-30&period=month&networks=Facebook&countries=US%2CJP%2CKR&auth_token=YOUR_AUTHENTICATION_TOKEN
    
    appIdsStr = '%2C'.join(app_ids)
    networksStr = '%2C'.join(networks)
    countriesStr = '%2C'.join(countries)
    url = f'https://api.sensortower.com/v1/unified/ad_intel/network_analysis?app_ids={appIdsStr}&start_date={start_date}&end_date={end_date}&period=month&networks={networksStr}&countries={countriesStr}&auth_token={sensortowerToken}'
    print(url)
    response = requests.get(url)
    data = response.json()
    # print(data)
    df = pd.DataFrame(data)

    return df


def getAllData():
    # ["Adcolony","Admob","Applovin","BidMachine","Chartboost","Digital Turbine","Facebook","InMobi","Instagram","Meta Audience Network","Mintegral","Mopub","Pinterest","Smaato","Snapchat","Supersonic","Tapjoy","TikTok","Twitter","Unity","Verve","Vungle","Youtube","Apple Search Ads"]
    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]
    countries = ['US','KR','JP','TW']
    # 下面是国家抽样，主要取人口比较多的国家
    gccCountries = ['SA']
    t1Countries = ['DE','FR','GB']
    t2Countries = ['ID','TR','TH']
    t3Countries = ['IN','BR','MX']
    countries = countries + gccCountries + t1Countries + t2Countries + t3Countries

    # Q1 & Q2
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    df = pd.DataFrame()
    for i in range(20):
        filename = f'/src/data/adTop{i}_202406.csv'
        if os.path.exists(filename):
            print('已存在%s'%filename)
            adDf = pd.read_csv(filename)
        else:
            minIndex = i * 5
            maxIndex = minIndex + 5

            slgAppIdList5 = slgAppIdList[minIndex:maxIndex]
            adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date='2024-06-01',end_date='2024-06-30')
            adDf.to_csv(filename,index=False)
        
        df = pd.concat([df,adDf],ignore_index=True)

    # print(df)
    return df

def main1():
    # 假设 getAllData() 函数返回的是一个 DataFrame
    df = getAllData()
    
    # 结果列表
    results = []

    # 按照 country 和 network 分组
    grouped = df.groupby(['country', 'network'])

    for (country, network), group in grouped:
        # 按照 sov 排序
        sorted_group = group.sort_values(by='sov', ascending=False)
        
        # 计算前3、10、20和100名的 sov 之和
        top3sovSum = sorted_group.head(3)['sov'].sum()
        top10sovSum = sorted_group.head(10)['sov'].sum()
        top20sovSum = sorted_group.head(20)['sov'].sum()
        top100sovSum = sorted_group.head(100)['sov'].sum()
        
        # 计算各种比例
        top3_to_top10_ratio = top3sovSum / top10sovSum if top10sovSum != 0 else 0
        top3_to_top20_ratio = top3sovSum / top20sovSum if top20sovSum != 0 else 0
        top3_to_top100_ratio = top3sovSum / top100sovSum if top100sovSum != 0 else 0
        top10_to_top100_ratio = top10sovSum / top100sovSum if top100sovSum != 0 else 0
        
        # 将结果添加到列表中
        results.append({
            'country': country,
            'network': network,
            'top3sovSum': top3sovSum,
            'top10sovSum': top10sovSum,
            'top20sovSum': top20sovSum,
            'top100sovSum': top100sovSum,
            'top3_to_top10_ratio': top3_to_top10_ratio,
            'top3_to_top20_ratio': top3_to_top20_ratio,
            'top3_to_top100_ratio': top3_to_top100_ratio,
            'top10_to_top100_ratio': top10_to_top100_ratio
        })

    # 将结果转换为 DataFrame
    result_df = pd.DataFrame(results)
    
    # 打印结果
    # print(result_df)
    result_df.to_csv('/src/data/ad_top3sov_to_top10sov_ratio.csv', index=False)

    return result_df
    

def debug2():
    # quarter
    start_date = '2024-04-01'
    end_date = '2024-06-30'
    appIdsStr = '64075e77537c41636a8e1c58'
    networksStr = 'Admob'
    countriesStr = 'US'
    url = f'https://api.sensortower.com/v1/unified/ad_intel/network_analysis?app_ids={appIdsStr}&start_date={start_date}&end_date={end_date}&period=quarter&networks={networksStr}&countries={countriesStr}&auth_token={sensortowerToken}'
    print(url)
    response = requests.get(url)
    data = response.json()
    print(data)



def debug():
    df = getAllData()
    # 找到US Meta Audience Network sov最高的app_id
    usMeta = df[(df['country'] == 'US') & (df['network'] == 'Meta Audience Network')]
    print(usMeta.sort_values(by='sov', ascending=False).head(10))

# 不再获得那么多名次，而是只获得前5名，但是要获得更多的时间
def getAllData2():
    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]
    countries = ['US','KR','JP','TW']
    # 下面是国家抽样，主要取人口比较多的国家
    gccCountries = ['SA']
    t1Countries = ['DE','FR','GB']
    t2Countries = ['ID','TR','TH']
    t3Countries = ['IN','BR','MX']
    countries = countries + gccCountries + t1Countries + t2Countries + t3Countries

    # Q1 & Q2
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    df = pd.DataFrame()
    dateList = [
        {'start_date':'2024-01-01','end_date':'2024-01-31'},
        {'start_date':'2024-02-01','end_date':'2024-02-29'},
        {'start_date':'2024-03-01','end_date':'2024-03-31'},
        {'start_date':'2024-04-01','end_date':'2024-04-30'},
        {'start_date':'2024-05-01','end_date':'2024-05-31'},
        {'start_date':'2024-06-01','end_date':'2024-06-30'},
    ]
    for d in dateList:
        filename = f'/src/data/adTop5_{d["start_date"]}_{d["end_date"]}.csv'
        if os.path.exists(filename):
            print('已存在%s'%filename)
            adDf = pd.read_csv(filename)
        else:
            slgAppIdList5 = slgAppIdList[:5]
            adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date=d['start_date'],end_date={d['end_date']})
            adDf.to_csv(filename,index=False)
        
        df = pd.concat([df,adDf],ignore_index=True)

    # print(df)
    return df



# 1、查看各媒体+各国家的top3之间的sov是否有正相关或负相关
def check1():
    df = getAllData2()
    # 将 app_id 转换为 app_name
    df['name'] = 'unknown'
    app_id_name_dict = {
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
        
        # 计算相关系数矩阵
        corr_matrix = pivot_table.corr()
        
        if country == 'TH' and network == 'Facebook':
            print(pivot_table)
            print(corr_matrix)

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
    print(final_df)
    final_df.to_csv('/src/data/ad_check1.csv', index=False)

# 2、查看各媒体+各国家的top3 合计sov与 lw的sov之间的比例
def check2():
    df = getAllData2()
    filtered_df = df[df['app_id'].isin(['638ee532480da915a62f0b34','64075e77537c41636a8e1c58','5869720d0211a6180f000ebc'])]
    # 按照 country 和 network 分组，计算每个分组的 sov 之和 与 lastwar(64075e77537c41636a8e1c58) 的 sov 差值
     # 按照 country 和 network 分组
    grouped = filtered_df.groupby(['country', 'network'])
    
    results = []

    for (country, network), group in grouped:
        # 计算每个分组的 sov 之和
        total_sov = group['sov'].sum()
        
        # 计算 lastwar 的 sov 差值
        lastwar_sov = group[group['app_id'] == '64075e77537c41636a8e1c58']['sov'].sum()
        totalPlastwar = total_sov / lastwar_sov
        totalPlastwar = f'{totalPlastwar * 100:.2f}%'
        results.append({'country': country, 'network': network, 'total_sov': total_sov, 'lastwar_sov':lastwar_sov,'total/lastwar': totalPlastwar})
    
    result_df = pd.DataFrame(results)
    print(result_df)
    result_df.to_csv('/src/data/ad_check2.csv', index=False)

# 3、查看各媒体+各国家的dau是否有明确的周末效应

if __name__ == '__main__':
    check1()   
    # check2()