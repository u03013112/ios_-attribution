# 畅销素材分析

import os
import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getVideoData1FromMC(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwVideoData1_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
select
    install_day,
    app_package,
    mediasource,
    country,
    country_levels,
    sum(cost_value_usd) as cost,
    sum(revenue_d7) as r7usd,
    max(video_url) as video_url,
    max(material_name) as material_name,
    max(original_name) as original_name,
    max(language) as language,
    max(earliest_day) as earliest_day,
    material_md5
from
    rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and material_type = '视频'
    and install_day between {installTimeStart} and {installTimeEnd}
group by
    install_day,
    app_package,
    mediasource,
    country,
    country_levels,
    material_md5
;
        '''
        
        df = execSql(sql)
        df.to_csv(filename,index=False)
        
    return df

def diffMedia(installTimeStart = '20240601',installTimeEnd = '20240630',period='week', cost_threshold=0.1):
    dataDf = getVideoData1FromMC(installTimeStart=installTimeStart, installTimeEnd=installTimeEnd)

    # # 不要Google的数据
    # dataDf = dataDf[dataDf['mediasource'] != 'Google']

    # 将install_day转换为datetime类型
    dataDf['install_day'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d')

    # 根据周期类型进行分组
    if period == 'day':
        dataDf['period'] = dataDf['install_day']
    elif period == 'week':
        dataDf['period'] = dataDf['install_day'].dt.to_period('W').apply(lambda r: r.start_time)
    elif period == 'month':
        dataDf['period'] = dataDf['install_day'].dt.to_period('M').apply(lambda r: r.start_time)
    else:
        raise ValueError("Invalid period. Choose from 'day', 'week', or 'month'.")

    grouped = dataDf.groupby(['period', 'mediasource', 'app_package'])

    result_list = []

    for (period, mediasource, app_package), group in grouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby('material_name').agg({
            'cost': 'sum',
            'material_name': 'first'
        }).reset_index(drop=True)

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

        top_materials = material_grouped[material_grouped['cost_ratio'] > cost_threshold]

        for _, row in top_materials.iterrows():
            result_list.append({
                'period_start': period,
                'mediasource': mediasource,
                'app_package': app_package,
                'material_name': row['material_name'],
                'cost_ratio': row['cost_ratio']
            })

    result_df = pd.DataFrame(result_list)
    return result_df

def diffCountry(installTimeStart = '20240601',installTimeEnd = '20240630',period='week', cost_threshold=0.1):
    dataDf = getVideoData1FromMC(installTimeStart=installTimeStart, installTimeEnd=installTimeEnd)

    # # 国家进行一下过滤，太多了，只对US,JP,KR,BR,DE,FR,MX,IN,GB,ID,TW,SA,TR,IT感兴趣，其他数据扔掉
    # dataDf = dataDf[dataDf['country'].isin([
    #     'US', 'JP', 'KR', 'BR', 'DE', 
    #     # 'FR', 'MX', 'IN', 'GB', 'ID', 
    #     # 'TW', 'SA', 'TR', 'IT'
    # ])]

    # # 不要Google的数据
    # dataDf = dataDf[dataDf['mediasource'] != 'Google']

    # 将install_day转换为datetime类型
    dataDf['install_day'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d')

    # 根据周期类型进行分组
    if period == 'day':
        dataDf['period'] = dataDf['install_day']
    elif period == 'week':
        dataDf['period'] = dataDf['install_day'].dt.to_period('W').apply(lambda r: r.start_time)
    elif period == 'month':
        dataDf['period'] = dataDf['install_day'].dt.to_period('M').apply(lambda r: r.start_time)
    else:
        raise ValueError("Invalid period. Choose from 'day', 'week', or 'month'.")

    grouped = dataDf.groupby(['period', 'country_levels'])

    result_list = []

    for (period, country), group in grouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby('material_name').agg({
            'cost': 'sum',
            'material_name': 'first'
        }).reset_index(drop=True)

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

        top_materials = material_grouped[material_grouped['cost_ratio'] > cost_threshold]

        for _, row in top_materials.iterrows():
            result_list.append({
                'period_start': period,
                'country': country,
                'material_name': row['material_name'],
                'cost_ratio': row['cost_ratio']
            })

    result_df = pd.DataFrame(result_list)
    return result_df

def diffCountryLanguage(installTimeStart = '20240601',installTimeEnd = '20240630',period='week', cost_threshold=0.1):
    dataDf = getVideoData1FromMC(installTimeStart=installTimeStart, installTimeEnd=installTimeEnd)

    # # 国家进行一下过滤，太多了，只对US,JP,KR,BR,DE,FR,MX,IN,GB,ID,TW,SA,TR,IT感兴趣，其他数据扔掉
    # dataDf = dataDf[dataDf['country'].isin([
    #     'US', 'JP', 'KR', 'BR', 'DE', 
    #     # 'FR', 'MX', 'IN', 'GB', 'ID', 
    #     # 'TW', 'SA', 'TR', 'IT'
    # ])]

    # 不要Google的数据
    dataDf = dataDf[dataDf['mediasource'] != 'Google']

    # 将install_day转换为datetime类型
    dataDf['install_day'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d')

    # 根据周期类型进行分组
    if period == 'day':
        dataDf['period'] = dataDf['install_day']
    elif period == 'week':
        dataDf['period'] = dataDf['install_day'].dt.to_period('W').apply(lambda r: r.start_time)
    elif period == 'month':
        dataDf['period'] = dataDf['install_day'].dt.to_period('M').apply(lambda r: r.start_time)
    else:
        raise ValueError("Invalid period. Choose from 'day', 'week', or 'month'.")

    grouped = dataDf.groupby(['period', 'country_levels'])

    result_list = []

    for (period, country), group in grouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby(['language']).agg({
            'cost': 'sum',
            'language': 'first'
        }).reset_index(drop=True)

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

        top_materials = material_grouped[material_grouped['cost_ratio'] > cost_threshold]

        for _, row in top_materials.iterrows():
            result_list.append({
                'period_start': period,
                'country': country,
                'language': row['language'],
                'cost_ratio': row['cost_ratio']
            })

    result_df = pd.DataFrame(result_list)
    return result_df

# def popTime(installTimeStart='20240601', installTimeEnd='20240630'):
#     dataDf = getVideoData1FromMC(installTimeStart=installTimeStart, installTimeEnd=installTimeEnd)

#     # # 不要Google的数据
#     # dataDf = dataDf[dataDf['mediasource'] != 'Google']

#     # 将install_day转换为datetime类型
#     dataDf['install_day'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d')
#     dataDf['earliest_day'] = pd.to_datetime(dataDf['earliest_day'], format='%Y%m%d')

#     # 根据周期类型进行分组
#     dataDf['period'] = dataDf['install_day'].dt.to_period('W').apply(lambda r: r.start_time)

#     # 分媒体统计畅销素材，先按照媒体进行汇总，然后将每个媒体的畅销素材找出来
#     mediaGrouped = dataDf.groupby(['period', 'mediasource', 'app_package'])

#     mediaResultList = []

#     for (period, mediasource, app_package), group in mediaGrouped:
#         total_cost = group['cost'].sum()

#         material_grouped = group.groupby('material_name').agg({
#             'cost': 'sum',
#             'earliest_day': 'min'
#         }).reset_index()

#         material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

#         top_materials = material_grouped[material_grouped['cost_ratio'] > 0.1]

#         for _, row in top_materials.iterrows():
#             mediaResultList.append({
#                 'period_start': period,
#                 'mediasource': mediasource,
#                 'app_package': app_package,
#                 'material_name': row['material_name'],
#                 'cost_ratio': row['cost_ratio'],
#                 'earliest_day': row['earliest_day']
#             })

#     mediaResultDf = pd.DataFrame(mediaResultList)
    
#     # 按照媒体+平台汇总，统计每一个畅销素材的最早出现日期和流行到的日期（这个日期是最后一个统计周期的开始日期，按周为单位，所以要+7天）
#     mediaResultDf = mediaResultDf.groupby(['mediasource', 'app_package', 'material_name']).agg({
#         'period_start': 'max',
#         'earliest_day': 'min'
#     }).reset_index()

#     mediaResultDf['pop_days'] = (mediaResultDf['period_start'] - mediaResultDf['earliest_day']).dt.days + 7

#     mediaResultDf.to_csv(f'/src/data/zk2/lwPopTime_media_{installTimeStart}_{installTimeEnd}.csv', index=False)

#     print('分媒体+平台 平均畅销素材流行天数')
#     print(mediaResultDf.groupby(['mediasource', 'app_package']).agg({
#         'pop_days': 'mean'
#     }).reset_index())
    

#     # 分国家统计畅销素材，先按照国家进行汇总，然后将每个国家的畅销素材找出来
#     countryGrouped = dataDf.groupby(['period', 'country_levels'])

#     countryResultList = []

#     for (period, country), group in countryGrouped:
#         total_cost = group['cost'].sum()

#         material_grouped = group.groupby('material_name').agg({
#             'cost': 'sum',
#             'earliest_day': 'min'
#         }).reset_index()

#         material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

#         top_materials = material_grouped[material_grouped['cost_ratio'] > 0.1]

#         for _, row in top_materials.iterrows():
#             countryResultList.append({
#                 'period_start': period,
#                 'country': country,
#                 'material_name': row['material_name'],
#                 'cost_ratio': row['cost_ratio'],
#                 'earliest_day': row['earliest_day']
#             })

#     countryResultDf = pd.DataFrame(countryResultList)

#     # 按照国家汇总，统计每一个畅销素材的最早出现日期和流行到的日期（这个日期是最后一个统计周期的开始日期，按周为单位，所以要+7天）
#     countryResultDf = countryResultDf.groupby(['country', 'material_name']).agg({
#         'period_start': 'max',
#         'earliest_day': 'min'
#     }).reset_index()

#     countryResultDf['pop_days'] = (countryResultDf['period_start'] - countryResultDf['earliest_day']).dt.days + 7

#     countryResultDf.to_csv(f'/src/data/zk2/lwPopTime_country_{installTimeStart}_{installTimeEnd}.csv', index=False)

#     print('分国家 平均畅销素材流行天数')
#     print(countryResultDf.groupby(['country']).agg({
#         'pop_days': 'mean'
#     }).reset_index())

def popTime(installTimeStart='20240601', installTimeEnd='20240630',cost_threshold=0.1):
    dataDf = getVideoData1FromMC(installTimeStart=installTimeStart, installTimeEnd=installTimeEnd)

    # 将install_day转换为datetime类型
    dataDf['install_day'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d')
    dataDf['earliest_day'] = pd.to_datetime(dataDf['earliest_day'], format='%Y%m%d')

    # 根据周期类型进行分组
    dataDf['period'] = dataDf['install_day'].dt.to_period('W').apply(lambda r: r.start_time)

    # 分媒体统计畅销素材，先按照媒体进行汇总，然后将每个媒体的畅销素材找出来
    mediaGrouped = dataDf.groupby(['period', 'mediasource', 'app_package'])

    mediaResultList = []

    for (period, mediasource, app_package), group in mediaGrouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby('material_name').agg({
            'cost': 'sum',
            'earliest_day': 'min'
        }).reset_index()

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

        top_materials = material_grouped[material_grouped['cost_ratio'] > cost_threshold]

        for _, row in top_materials.iterrows():
            mediaResultList.append({
                'period_start': period,
                'mediasource': mediasource,
                'app_package': app_package,
                'material_name': row['material_name'],
                'cost_ratio': row['cost_ratio'],
                'earliest_day': row['earliest_day']
            })

    mediaResultDf = pd.DataFrame(mediaResultList)
    
    # 按照媒体+平台汇总，统计每一个畅销素材的最早出现日期和出现的周期数
    mediaResultDf = mediaResultDf.groupby(['mediasource', 'app_package', 'material_name']).agg({
        'period_start': 'count',
        'earliest_day': 'min'
    }).reset_index()

    # 计算流行天数
    mediaResultDf['pop_days'] = mediaResultDf['period_start'] * 7

    mediaResultDf.to_csv(f'/src/data/zk2/lwPopTime_media_{installTimeStart}_{installTimeEnd}.csv', index=False)

    print('分媒体+平台 平均畅销素材流行天数')
    print(mediaResultDf.groupby(['mediasource', 'app_package']).agg({
        'pop_days': 'mean'
    }).reset_index())
    

    # 分国家统计畅销素材，先按照国家进行汇总，然后将每个国家的畅销素材找出来
    countryGrouped = dataDf.groupby(['period', 'country_levels'])

    countryResultList = []

    for (period, country), group in countryGrouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby('material_name').agg({
            'cost': 'sum',
            'earliest_day': 'min'
        }).reset_index()

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost

        top_materials = material_grouped[material_grouped['cost_ratio'] > cost_threshold]

        for _, row in top_materials.iterrows():
            countryResultList.append({
                'period_start': period,
                'country': country,
                'material_name': row['material_name'],
                'cost_ratio': row['cost_ratio'],
                'earliest_day': row['earliest_day']
            })

    countryResultDf = pd.DataFrame(countryResultList)

    # 按照国家汇总，统计每一个畅销素材的最早出现日期和出现的周期数
    countryResultDf = countryResultDf.groupby(['country', 'material_name']).agg({
        'period_start': 'count',
        'earliest_day': 'min'
    }).reset_index()

    # 计算流行天数
    countryResultDf['pop_days'] = countryResultDf['period_start'] * 7

    countryResultDf.to_csv(f'/src/data/zk2/lwPopTime_country_{installTimeStart}_{installTimeEnd}.csv', index=False)

    print('分国家 平均畅销素材流行天数')
    print(countryResultDf.groupby(['country']).agg({
        'pop_days': 'mean'
    }).reset_index())

if __name__ == "__main__":
    
    # diffMediaDf = diffMedia(installTimeStart='20240101', installTimeEnd='20240630', period='month', cost_threshold=0.1)
    # print(diffMediaDf)
    # diffMediaDf.to_csv('/src/data/lw20240722DiffMedia.csv', index=False)

    diffMediaDf = pd.read_csv('/src/data/lw20240722DiffMedia.csv')
    diffMediaDf = diffMediaDf.groupby([
        'period_start', 
        # 'mediasource',
        # 'app_package'
        ]).agg({
        'cost_ratio':'mean'
    }).reset_index()
    print(diffMediaDf)
    # diffMediaDf.to_csv('/src/data/lw20240722DiffMedia2.csv', index=False)


    # diffCountryDf = diffCountry(installTimeStart='20240101', installTimeEnd='20240630', period='month', cost_threshold=0.1)
    # print(diffCountryDf)
    # diffCountryDf.to_csv('/src/data/lw20240722DiffCountry.csv', index=False)

    # diffCountryLanguageDf = diffCountryLanguage(installTimeStart='20240101', installTimeEnd='20240630', period='month', cost_threshold=0.1)
    # print(diffCountryLanguageDf)
    # diffCountryLanguageDf.to_csv('/src/data/lw20240722DiffCountryLanguage.csv', index=False)


    # popTime(installTimeStart='20240101', installTimeEnd='20240630', cost_threshold=0.2)
