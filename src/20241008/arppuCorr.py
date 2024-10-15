import os
import pandas as pd
import numpy as np

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData(startDate, endDate, limit):
    filename = f'/src/data/lw_{startDate}_{endDate}_{limit}.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
select
    to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyymmdd') as date,
    sum(least(user_revenue_24h, {limit})) as 24hours_revenue_capped,
    country,
    mediasource
from (
    select
        game_uid,
        country,
        mediasource,
        install_timestamp,
        sum(case when event_time - cast(install_timestamp as bigint) between 0 and 86400
            then revenue_value_usd else 0 end) as user_revenue_24h
    from dwd_overseas_revenue_allproject
    where
        app = 502
        and app_package = 'com.fun.lastwar.gp'
        and zone = 0
        and day between '{startDate}' and '{endDate}'
        and install_day >= '{startDate}'
    group by game_uid, install_timestamp,country,mediasource
) as user_revenue_summary
group by to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyymmdd'),country,mediasource
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def corr():
    df = pd.read_csv('lw.csv')
    df = df[df['date'] >= 20240701]
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    
    df['cost'] = df['cost'].str.replace(',', '').astype(float)
    df['installs'] = df['installs'].str.replace(',', '').astype(int)
    df['CPM'] = df['CPM'].replace(',', '').astype(float)
    df['CPI'] = df['CPI'].replace(',', '').astype(float)
    df['payusers_d1'] = df['payusers_d1'].replace(',', '').astype(float)

    df['pay_rate_d1'] = df['pay_rate_d1'].str.replace('%', '').astype(float)
    df['CTR'] = df['CTR'].str.replace('%', '').astype(float)
    df['CVR'] = df['CVR'].str.replace('%', '').astype(float)
    df['ROI_24H'] = df['ROI_24H'].str.replace('%', '').astype(float)

    df['cpup_d1'] = df['cost'] / df['payusers_d1']
    df['arppu_d1'] = df['revenue_24H'] / df['payusers_d1']
    df['arpu_d1'] = df['revenue_24H'] / df['installs']

    print(df.corr())

def Corr2():
    df = pd.read_csv('lw.csv')
    df = df[df['date'] >= 20240701]
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    
    df['cost'] = df['cost'].str.replace(',', '').astype(float)
    df['installs'] = df['installs'].str.replace(',', '').astype(int)
    df['CPM'] = df['CPM'].replace(',', '').astype(float)
    df['CPI'] = df['CPI'].replace(',', '').astype(float)
    df['payusers_d1'] = df['payusers_d1'].replace(',', '').astype(float)

    df['pay_rate_d1'] = df['pay_rate_d1'].str.replace('%', '').astype(float)
    df['CTR'] = df['CTR'].str.replace('%', '').astype(float)
    df['CVR'] = df['CVR'].str.replace('%', '').astype(float)
    df['ROI_24H'] = df['ROI_24H'].str.replace('%', '').astype(float)

    # 定义首日付费金额上限
    limits = [100, 200, 300, 400, 500, 3000]
    # limits = [5000]
    startDate = '20240601'
    endDate = '20241015'
    
    for limit in limits:
        historical_data = getHistoricalData(startDate, endDate, limit)
        historical_data = historical_data.groupby('date').sum().reset_index()
        historical_data['date'] = pd.to_datetime(historical_data['date'], format='%Y%m%d')
        
        # 合并数据
        merged_df = pd.merge(df, historical_data, on='date', how='left')
        merged_df[f'arppu_d1_capped_{limit}'] = merged_df['24hours_revenue_capped'] / merged_df['payusers_d1']
        
        # 计算被削弱的安装数占比和收入金额占比
        total_revenue = merged_df['revenue_24H'].sum()
        capped_revenue = merged_df['24hours_revenue_capped'].sum()
        revenue_reduction_ratio = (total_revenue - capped_revenue) / total_revenue
        
        print(f'Limit: {limit}')
        print(f'Revenue reduction ratio: {revenue_reduction_ratio:.2%}')
        
        # # 计算削弱后的ARPPU相关系数
        # capped_corr = merged_df.corr()[f'arppu_d1_capped_{limit}']
        # print(f'Correlation with capped ARPPU (limit {limit}):')
        # print(capped_corr)
        print(merged_df.corr()[['24hours_revenue_capped',f'arppu_d1_capped_{limit}']])
        print()

if __name__ == '__main__':
    # corr()
    Corr2()
