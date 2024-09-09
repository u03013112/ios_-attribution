# 找到助攻比例变化

# 希望得到结论：
# 随着助攻比安装例的变高，ROI会变化。比如 applovin 被 Facebook 助攻的比例变高，那么 Facebook 的 ROI 会变低。
# 支付比例类似

# 上述情况要出现在花费比例类似的情况下才能有明显的趋势，如果applovin的花费比例过低，那么即使被Facebook助攻的比例变高，也不会对Facebook的ROI产生明显的影响。
# 怎么将这部分因素算进去？进行过滤，将花费比例过低的数据过滤掉，只保留花费比例相近的数据。


import os
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from a import getMediaCostDataFromMC

def getData():
    filename = '/src/data/lastwar_contributor_20240101.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        data = pd.read_csv(filename)
    else:

        sql = f'''
-- Step 1: 获取初始数据
@initial_data :=
SELECT
       get_json_object(
              base64decode(base64decode(push_data)),
              '$.customer_user_id'
       ) AS customer_user_id,
       get_json_object(
              base64decode(base64decode(push_data)),
              '$.contributor_1_media_source'
       ) AS contributor_1_media_source,
       get_json_object(
              base64decode(base64decode(push_data)),
              '$.app_id'
       ) AS app_package
FROM
       rg_bi.ods_platform_appsflyer_push_event_total
WHERE
       ds >= '20240101'
       AND get_json_object(
              base64decode(base64decode(push_data)),
              '$.event_name'
       ) = 'install'
       AND get_json_object(
              base64decode(base64decode(push_data)),
              '$.app_id'
       ) = 'com.fun.lastwar.gp';

-- Step 2: 获取安装时间，媒体，7日收入和国家
@revenue_data :=
SELECT
       game_uid,
       MAX(install_day) AS install_day,
       MAX(mediasource) AS mediasource,
       MAX(country) AS country,
       COALESCE(
              SUM(
                     CASE
                            WHEN event_time - install_timestamp BETWEEN 0
                            AND 7 * 24 * 3600 THEN revenue_value_usd
                            ELSE 0
                     END
              ),
              0
       ) AS r7usd
FROM
       dwd_overseas_revenue_allproject
WHERE
       app = '502'
       AND zone = 0
       AND app_package = 'com.fun.lastwar.gp'
       AND install_day >= '20240101'
GROUP BY
       game_uid
HAVING
       r7usd > 0;

-- Step 3: 最终输出并汇总
SELECT
       r.install_day,
       r.mediasource,
       i.contributor_1_media_source AS contributor_media_source,
       r.country,
       COUNT(DISTINCT i.customer_user_id) AS installs,
       SUM(r.r7usd) AS r7usd
FROM
       @initial_data i
       JOIN @revenue_data r ON i.customer_user_id = r.game_uid
GROUP BY
       r.install_day,
       r.mediasource,
       i.contributor_1_media_source,
       r.country;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)
    return data

def step1():
    dataDf = getData()
    # 按周汇总，先忽略国家，将所有国家汇总到一起
    dataDf['install_week'] = pd.to_datetime(dataDf['install_day'], format='%Y%m%d').dt.to_period('W').apply(lambda r: r.start_time)
    
    # 填充空值
    dataDf['contributor_media_source'].fillna('Organic', inplace=True)
    
    # 按周和媒体汇总安装数和收入
    weekly_summary = dataDf.groupby(['install_week', 'mediasource', 'contributor_media_source']).agg({
        'installs': 'sum',
        'r7usd': 'sum'
    }).reset_index()
    
    # 计算每一周内的媒体之间的助攻安装数占比
    weekly_summary['contributor_media_install_ratio'] = weekly_summary.groupby(['install_week', 'mediasource'])['installs'].transform(lambda x: x / x.sum())
    
    # 计算每一周内的媒体之间的助攻收入占比
    weekly_summary['contributor_media_revenue_ratio'] = weekly_summary.groupby(['install_week', 'mediasource'])['r7usd'].transform(lambda x: x / x.sum())

    return weekly_summary

def step2():
    # 读取花费数据
    costDf = getMediaCostDataFromMC(installTimeStart='20240101', installTimeEnd='20240830')
    costDf = costDf[costDf['app_package']=='com.fun.lastwar.gp']
    
    # 按周汇总，先忽略国家，将所有国家汇总到一起
    costDf['install_week'] = pd.to_datetime(costDf['install_day'], format='%Y%m%d').dt.to_period('W').apply(lambda r: r.start_time)
    
    # 按周和媒体汇总花费
    weekly_cost_summary = costDf.groupby(['install_week', 'mediasource']).agg({
        'cost': 'sum',
        'r7usd': 'sum'
    }).reset_index()
    weekly_cost_summary['ROI'] = weekly_cost_summary['r7usd'] / weekly_cost_summary['cost']

    # 计算每一周内的媒体之间的花费占比
    weekly_cost_summary['media_cost_ratio'] = weekly_cost_summary.groupby(['install_week'])['cost'].transform(lambda x: x / x.sum())

    return weekly_cost_summary

def main():
    dataDf = step1()
    costDf = step2()

    # 进行过滤，由于助攻数据从2024-03-04开始才有，所以只保留这个日期之后的数据
    dataDf = dataDf[(dataDf['install_week'] >= '2024-03-04') & (dataDf['install_week'] <= '2024-08-25')]
    costDf = costDf[(costDf['install_week'] >= '2024-03-04') & (costDf['install_week'] <= '2024-08-25')]
    # costDf = costDf[(costDf['install_week'] >= '2024-01-01') & (costDf['install_week'] <= '2024-08-25')]

    # 只对3个主要媒体感兴趣，过滤掉其他媒体
    mediaList = ['Facebook Ads', 'googleadwords_int', 'applovin_int']

    dataDf = dataDf[
        (dataDf['mediasource'].isin(mediaList))&
        (dataDf['contributor_media_source'].isin(mediaList))
    ]

    # 创建一个新的 DataFrame 来存储每周的助攻组合的安装数占比和收入占比
    new_data = {'install_week': dataDf['install_week'].unique()}
    new_data = pd.DataFrame(new_data)

    for media in mediaList:
        for contributor in mediaList:
            if media != contributor:
                install_ratio_col = f'{media}_contributor_{contributor}_install_ratio'
                revenue_ratio_col = f'{media}_contributor_{contributor}_revenue_ratio'
                
                temp_df = dataDf[(dataDf['mediasource'] == media) & (dataDf['contributor_media_source'] == contributor)]
                temp_df = temp_df.groupby('install_week').agg({
                    'contributor_media_install_ratio': 'sum',
                    'contributor_media_revenue_ratio': 'sum'
                }).reset_index()
                
                temp_df.rename(columns={
                    'contributor_media_install_ratio': install_ratio_col,
                    'contributor_media_revenue_ratio': revenue_ratio_col
                }, inplace=True)
                
                new_data = pd.merge(new_data, temp_df, on='install_week', how='left')

    # 有的周没有数据，需要填充为0
    new_data = new_data.fillna(0)

    costDf = costDf[costDf['mediasource'].isin(mediaList)]

    # 将costDf整理为每周每个媒体的ROI和花费比例
    cost_pivot = costDf.pivot(index='install_week', columns='mediasource', values=['ROI', 'media_cost_ratio','cost']).reset_index()
    cost_pivot.columns = ['install_week'] + [f'{metric}_{media}' for metric, media in cost_pivot.columns[1:]]

    # print(cost_pivot)
    # print(cost_pivot.corr())
    # return

    # merge
    mergedDf = pd.merge(new_data, cost_pivot, on='install_week', how='left')

    # # 计算所有媒体的ROI 与助攻安装数占比的相关性
    # for media in mediaList:
    #     correlation_install_ratio = mergedDf[f'contributor_media_install_ratio'][mergedDf['mediasource'] == media].corr(mergedDf[f'ROI_{media}'])
    #     print(f'{media} 助攻安装数占比与ROI的相关性: {correlation_install_ratio}')

    # # 计算所有媒体的ROI 与助攻收入占比的相关性
    # for media in mediaList:
    #     correlation_revenue_ratio = mergedDf[f'contributor_media_revenue_ratio'][mergedDf['mediasource'] == media].corr(mergedDf[f'ROI_{media}'])
    #     print(f'{media} 助攻收入占比与ROI的相关性: {correlation_revenue_ratio}')

    corrDf = mergedDf.corr()[[
        'ROI_Facebook Ads', 'ROI_googleadwords_int', 'ROI_applovin_int',
        'media_cost_ratio_Facebook Ads', 'media_cost_ratio_googleadwords_int', 'media_cost_ratio_applovin_int',
    ]]
    print(corrDf)
    corrDf.to_csv('/src/data/lastwar_contributor_corr.csv')

    # 过滤掉applovin的花费比例过低的数据，低于10%的 那一周，所有媒体数据都过滤掉
    # filteredDf = mergedDf[mergedDf['media_cost_ratio_applovin_int'] >= 0.1]

    # # 计算所有媒体的ROI 与助攻安装数占比的相关性 2
    # for media in mediaList:
    #     correlation_install_ratio_filtered = filteredDf[f'contributor_media_install_ratio'][filteredDf['mediasource'] == media].corr(filteredDf[f'ROI_{media}'])
    #     print(f'{media} 助攻安装数占比与ROI的相关性（过滤后）: {correlation_install_ratio_filtered}')

    # # 计算所有媒体的ROI 与助攻收入占比的相关性 2
    # for media in mediaList:
    #     correlation_revenue_ratio_filtered = filteredDf[f'contributor_media_revenue_ratio'][filteredDf['mediasource'] == media].corr(filteredDf[f'ROI_{media}'])
    #     print(f'{media} 助攻收入占比与ROI的相关性（过滤后）: {correlation_revenue_ratio_filtered}')
    

if __name__ == '__main__':
    main()
