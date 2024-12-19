import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def getLichengbeiData():
    filename = '/src/data/20241218_lichengbei.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
    startday,
    endday,
    app_package_sys,
    country_group,
    target_usd,
    target_d7roi
from ads_application_lastwar_milestones
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getAndroidData():
    filename = '/src/data/20241218_android.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
    install_day,
    country,
    sum(usd) as cost,
    sum(d7) as revenue_d7
from tmp_lw_cost_and_roi_by_day
where
    install_day >= 20240401
group by
    install_day,
    country
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getIosData():
    filename = '/src/data/20241218_ios.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
    install_day,
    country,
    sum(usd) as cost,
    sum(d7) as revenue_d7
from tmp_lw_cost_and_roi_by_day_ios
where
    install_day >= 20240401
group by
    install_day,
    country
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def main():
    getLichengbeiDf = getLichengbeiData()
    # 排重，将所有列都相同的去掉
    # getLichengbeiDf = getLichengbeiDf.drop_duplicates()
    getLichengbeiDf = getLichengbeiDf.groupby(['startday', 'endday', 'app_package_sys', 'country_group', 'target_usd']).agg({
        'target_d7roi': 'min'
    }).reset_index()
    # lichengbeiPlatformList = getLichengbeiDf['app_package_sys'].unique()
    getLichengbeiDf = getLichengbeiDf[getLichengbeiDf['app_package_sys'].isin(['AOS', 'IOS'])]
    
    androidDf = getAndroidData()
    androidDf['platform'] = 'AOS'

    iosDf = getIosData()
    iosDf['platform'] = 'IOS'

    df = pd.concat([androidDf, iosDf], axis=0)

    # 做一些数据处理
    lichengbeiCountryList = getLichengbeiDf['country_group'].unique()
    # print(lichengbeiCountryList)
    # df中的country 只保留在lichengbei中的，剩余的都统一为other
    df['country'] = df['country'].apply(lambda x: x if x in lichengbeiCountryList else 'OTHER')
    df = df.groupby(['install_day', 'platform', 'country']).agg({
        'cost': 'sum',
        'revenue_d7': 'sum'
    }).reset_index()


    # 结果Df需要列： day, platform, country, cost, revenue_d7, target_usd, target_d7roi, sum_cost, sum_revenue_d7, sum_d7roi
    # 其中day：安装日期，platform：平台，country：国家，cost：当日成本，revenue_d7：当日d7收入，
    # target_usd：lichengbei中的目标usd,lichengbei与df的关联条件是platform和country相同，startday <= day <= endday
    # target_d7roi：lichengbei中的目标d7roi,
    # sum_cost：当日成本累计，累计条件式同一个里程碑内的cost的和
    # sum_revenue_d7：当日d7收入累计
    # sum_d7roi：当日d7roi累计，sum_revenue_d7 / sum_cost

    # 初始化结果 DataFrame
    result_df = pd.DataFrame()

    # 遍历每个里程碑
    for _, row in getLichengbeiDf.iterrows():
        startday = row['startday']
        endday = row['endday']
        platform = row['app_package_sys']
        country = row['country_group']
        target_usd = row['target_usd']
        target_d7roi = row['target_d7roi']

        # 过滤出符合条件的 df 数据
        mask = (
            (df['install_day'] >= startday) &
            (df['install_day'] <= endday) &
            (df['platform'] == platform) &
            (df['country'] == country)
        )
        filtered_df = df[mask].copy()

        # 按照 install_day 排序
        filtered_df = filtered_df.sort_values(by='install_day')

        # 计算累计值
        filtered_df['sum_cost'] = filtered_df['cost'].cumsum()
        filtered_df['sum_revenue_d7'] = filtered_df['revenue_d7'].cumsum()
        filtered_df['sum_d7roi'] = filtered_df['sum_revenue_d7'] / filtered_df['sum_cost']

        
        filtered_df['startday'] = startday
        filtered_df['endday'] = endday
        filtered_df['target_usd'] = target_usd
        filtered_df['target_d7roi'] = target_d7roi

        # 合并到结果 DataFrame
        result_df = pd.concat([result_df, filtered_df], axis=0)

    # 重置索引
    result_df.reset_index(drop=True, inplace=True)

    # sum_d7roi < target_d7roi 的记录, sum_cost = 0
    mask = result_df['sum_d7roi'] < result_df['target_d7roi']
    result_df.loc[mask, 'sum_cost'] = 0

    result_df.to_csv('/src/data/20241218_lichengbei_result.csv', index=False)

    # for test:
    testDf = result_df.groupby(['install_day','platform','country']).agg({
        'cost': 'sum',
        'revenue_d7': 'sum',
        'sum_cost': 'sum',
        'startday':'max',
        'endday':'max',
        'target_usd':'max'
    }).reset_index()
    testDf.to_csv('/src/data/20241218_lichengbei_result_test.csv', index=False)

    result_df['install_day'] = pd.to_datetime(result_df['install_day'], format='%Y%m%d')

    # 大盘
    totalDf = result_df.groupby(['install_day']).agg({
        'cost': 'sum',
        'sum_cost': 'sum',
        'startday':'max',
        'endday':'max',
        'target_usd':'max'
    }).reset_index()
    totalDf.to_csv('/src/data/20241218_lichengbei_result_total.csv', index=False)
    # TODO:
    # totalDf按照 startday 和 endday 分组，每个分组画一张图
    # install_day 作为 x 轴，
    # 双 y 轴，左边 y 轴是 cost，右边 y 轴是 sum_cost 和 target_usd
    # 保存图片到 /src/data/20241218_lichengbei_result_total_{startday}_{endday}.png
    for (startday, endday), group in totalDf.groupby(['startday', 'endday']):
        fig, ax1 = plt.subplots(figsize=(12, 6))

        ax1.set_xlabel('install_day')
        ax1.set_ylabel('cost', color='tab:blue')
        ax1.plot(group['install_day'], group['cost'], color='tab:blue', label='cost')
        ax1.tick_params(axis='y', labelcolor='tab:blue')

        ax2 = ax1.twinx()
        ax2.set_ylabel('sum_cost / target_usd', color='tab:red')
        ax2.plot(group['install_day'], group['sum_cost'], color='tab:red', label='sum_cost')
        ax2.plot(group['install_day'], group['target_usd'], color='tab:green', linestyle='--', label='target_usd')
        ax2.tick_params(axis='y', labelcolor='tab:red')

        # 设置不使用科学计数法
        ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        ax2.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

        # 设置 x 轴标签倾斜
        plt.xticks(rotation=45)

        fig.tight_layout()
        plt.title(f'Total Cost and Target USD from {startday} to {endday}')
        plt.legend(loc='upper left')
        plt.savefig(f'/src/data/20241218_lichengbei_result_total_{startday}_{endday}.png')
        plt.close()


    # result_df 按照 platform、country、startday 和 endday 分组，每个里程碑画2张图
    # 1. install_day 作为 x 轴，y 轴是 sum_d7roi 和 target_d7roi
    # 2. install_day 作为 x 轴，双 y 轴，左边 y 轴是 cost，右边 y 轴是 sum_cost
    for (platform, country, startday, endday), group in result_df.groupby(['platform', 'country', 'startday', 'endday']):
        # 图1: sum_d7roi 和 target_d7roi
        plt.figure(figsize=(12, 6))
        plt.plot(group['install_day'], group['sum_d7roi'], label='sum_d7roi', color='tab:blue')
        plt.plot(group['install_day'], group['target_d7roi'], label='target_d7roi', color='tab:red', linestyle='--')
        plt.xlabel('install_day')
        plt.ylabel('ROI')
        plt.title(f'{platform} {country} ROI from {startday} to {endday}')
        plt.legend()
        plt.xticks(rotation=45)
        plt.savefig(f'/src/data/20241218_lichengbei_result_{platform}_{country}_{startday}_{endday}_roi.png')
        plt.close()

        # 图2: cost 和 sum_cost
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax1.set_xlabel('install_day')
        ax1.set_ylabel('cost', color='tab:blue')
        ax1.plot(group['install_day'], group['cost'], color='tab:blue', label='cost')
        ax1.tick_params(axis='y', labelcolor='tab:blue')

        ax2 = ax1.twinx()
        ax2.set_ylabel('sum_cost', color='tab:red')
        ax2.plot(group['install_day'], group['sum_cost'], color='tab:red', label='sum_cost')
        ax2.tick_params(axis='y', labelcolor='tab:red')

        # 设置不使用科学计数法
        ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        ax2.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

        # 设置 x 轴标签倾斜
        plt.xticks(rotation=45)

        fig.tight_layout()
        plt.title(f'{platform} {country} Cost from {startday} to {endday}')
        plt.legend(loc='upper left')
        plt.savefig(f'/src/data/20241218_lichengbei_result_{platform}_{country}_{startday}_{endday}_cost.png')
        plt.close()
    
if __name__ == '__main__':
    main()
