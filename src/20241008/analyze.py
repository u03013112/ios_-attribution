import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData():
    filename = '/src/data/xiaoyu_historical_data_20240401_20241007_analyze.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
	install_day,
	mediasource,
	country,
	sum(usd) as usd,
	sum(d1) as d1,
	sum(ins) as ins,
	sum(pud1) as pud1
from
	tmp_lw_cost_and_roi_by_day
where
	install_day between 20240401
	and 20241007
group by
	install_day,
	mediasource,
	country;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

# 计算每个月的cppu，是否有明显的波动
def payUserAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    groupByMonthDf = df.groupby(['month']).agg({
        'usd': 'sum',
        'ins': 'sum',
        'pud1': 'sum'
    }).reset_index()

    groupByMonthDf['cpi'] = groupByMonthDf['usd'] / groupByMonthDf['ins']
    groupByMonthDf['cppu'] = groupByMonthDf['usd'] / groupByMonthDf['pud1']

    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_payuser_analysis1.csv', index=False)

def roiAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    groupByMonthDf = df.groupby(['month']).agg({
        'usd': 'sum',
        'd1': 'sum',
    }).reset_index()

    groupByMonthDf['roi'] = groupByMonthDf['d1'] / groupByMonthDf['usd']

    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_roi_analysis1.csv', index=False)

def payUserCountryAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    groupByMonthDf = df.groupby(['month', 'country']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 计算每个国家 在每个月中 的 usd 占比 和 pud1 占比
    groupByMonthDf0 = groupByMonthDf.groupby(['month']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    groupByMonthDf0.rename(columns={'usd': 'usdTotal', 'pud1': 'pud1Total'}, inplace=True)

    groupByMonthDf = pd.merge(groupByMonthDf, groupByMonthDf0, on='month', how='left')
    groupByMonthDf['usdRate'] = groupByMonthDf['usd'] / groupByMonthDf['usdTotal']
    groupByMonthDf['pud1Rate'] = groupByMonthDf['pud1'] / groupByMonthDf['pud1Total']

    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_country_analysis1.csv', index=False)

def payUserCountryAnalysis2():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    
    countryList = ['JP','KR','US','T1',"T2"]
    # 如果不在列表中，就设置为 'others'
    df['country'] = df['country'].apply(lambda x: x if x in countryList else 'others')

    test_df = df[(df['install_day'] >= '2024-09-13') & (df['install_day'] <= '2024-10-07')]
    test_df = test_df.sort_values('install_day', ascending=True)
    # 按天统计国家分布
    test_df = test_df[['install_day', 'country', 'usd', 'pud1']]
    test_df = test_df.groupby(['install_day', 'country']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    test_df0 = test_df.groupby(['install_day']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    test_df0.rename(columns={'usd': 'usdTotal', 'pud1': 'pud1Total'}, inplace=True)



    test_df = pd.merge(test_df, test_df0, on='install_day', how='left')
    test_df['usdRate'] = test_df['usd'] / test_df['usdTotal']
    test_df['pud1Rate'] = test_df['pud1'] / test_df['pud1Total']

    print(test_df)
    test_df.to_csv('/src/data/xy_country_analysis2.csv', index=False)

def payUserMediaAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    # 媒体太多，过一些过滤
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    # 如果不在列表中，就设置为 'others'
    df['mediasource'] = df['mediasource'].apply(lambda x: x if x in mediaList else 'others')

    groupByMonthDf = df.groupby(['month', 'mediasource']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 计算每个媒体 在每个月中 的 usd 占比 和 pud1 占比
    groupByMonthDf0 = groupByMonthDf.groupby(['month']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    groupByMonthDf0.rename(columns={'usd': 'usdTotal', 'pud1': 'pud1Total'}, inplace=True)

    groupByMonthDf = pd.merge(groupByMonthDf, groupByMonthDf0, on='month', how='left')
    groupByMonthDf['usdRate'] = groupByMonthDf['usd'] / groupByMonthDf['usdTotal']
    groupByMonthDf['pud1Rate'] = groupByMonthDf['pud1'] / groupByMonthDf['pud1Total']

    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_media_analysis1.csv', index=False)

def payUserMediaAnalysis2():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    # 如果不在列表中，就设置为 'others'
    df['mediasource'] = df['mediasource'].apply(lambda x: x if x in mediaList else 'others')

    test_df = df[(df['install_day'] >= '2024-09-13') & (df['install_day'] <= '2024-10-07')]
    test_df = test_df.sort_values('install_day', ascending=True)
    # 按天统计媒体分布
    test_df = test_df[['install_day', 'mediasource', 'usd', 'pud1']]
    test_df = test_df.groupby(['install_day', 'mediasource']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    test_df0 = test_df.groupby(['install_day']).agg({
        'usd': 'sum',
        'pud1': 'sum',
    }).reset_index()
    test_df0.rename(columns={'usd': 'usdTotal', 'pud1': 'pud1Total'}, inplace=True)



    test_df = pd.merge(test_df, test_df0, on='install_day', how='left')
    test_df['usdRate'] = test_df['usd'] / test_df['usdTotal']
    test_df['pud1Rate'] = test_df['pud1'] / test_df['pud1Total']

    print(test_df)
    test_df.to_csv('/src/data/xy_media_analysis2.csv', index=False)

def arppuCountryAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    groupByMonthDf = df.groupby(['month', 'country']).agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 计算每个国家 在不同的月 的arppu，看看是否存在波动
    groupByMonthDf['arppu'] = groupByMonthDf['d1'] / groupByMonthDf['pud1']
    groupByMonthDf = groupByMonthDf.sort_values(['country', 'month'], ascending=True)
    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_arppu_country_analysis1.csv', index=False)


def arppuMediaAnalysis():
    df = getHistoricalData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['month'] = df['install_day'].dt.to_period('M')

    groupByMonthDf = df.groupby(['month', 'mediasource']).agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 计算每个媒体 在不同的月 的arppu，看看是否存在波动
    groupByMonthDf['arppu'] = groupByMonthDf['d1'] / groupByMonthDf['pud1']
    groupByMonthDf = groupByMonthDf.sort_values(['mediasource', 'month'], ascending=True)
    print(groupByMonthDf)
    groupByMonthDf.to_csv('/src/data/xy_arppu_media_analysis1.csv', index=False)

if __name__ == "__main__":
    # payUserAnalysis()
    # roiAnalysis()
    # payUserCountryAnalysis()
    # payUserMediaAnalysis()

    # payUserCountryAnalysis2()
    # payUserMediaAnalysis2()
    
    arppuCountryAnalysis()
    arppuMediaAnalysis()
