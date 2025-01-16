# 多app进行比较
# 比较内容主要有：
# 1、国家花费分布
# 2、媒体花费分布
# TODO：上面结果的差异，尝试找到差异原因。

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 时间，统一使用
def getStartEndDay():
    return '20240101', '20241231'

def getDataForTopwar():
    startDayStr, endDayStr = getStartEndDay()

    filename = '/src/data/20250103_data_topwar.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
select
    install_day,
    app_package,
    mediasource,
    country,
    sum(cost_value_usd) as cost,
    sum(impression) as impression,
    sum(install) as install,
    sum(revenue_d7) as revenue_d7
from
    dws_overseas_roi_realtime
where
    app = 102
    and zone = 0
    and window_cycle = 9999
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package,
    mediasource,
    country
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getDataForLastwar():
    startDayStr, endDayStr = getStartEndDay()

    filename = '/src/data/20250103_data_lastwar.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
select
    install_day,
    app_package,
    mediasource,
    country,
    sum(cost_value_usd) as cost,
    sum(impression) as impression,
    sum(installs) as install,
    sum(revenue_d7) as revenue_d7
from
    dws_overseas_public_roi
where
    app = 502
    and zone = 0
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}'
    and '{endDayStr}'
group by
    install_day,
    app_package,
    mediasource,
    country
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getDataForTopHeros():
    startDayStr, endDayStr = getStartEndDay()

    filename = '/src/data/20250103_data_topheros.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
select
    install_day,
    app_package,
    mediasource,
    country,
    sum(cost_value_usd) as cost,
    sum(impression) as impression,
    sum(installs) as install,
    sum(revenue_d7) as revenue_d7
from
    dws_overseas_public_roi
where
    app = 116
    and zone = 0
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}'
    and '{endDayStr}'
group by
    install_day,
    app_package,
    mediasource,
    country
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getCountryGroup():
    filename = '/src/data/20250103_country_group.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
    country,
    countrygroup
from cdm_laswwar_country_map
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def main():
    topwarDf = getDataForTopwar()
    lastwarDf = getDataForLastwar()
    topherosDf = getDataForTopHeros()

    # 先将country汇总到countrygroup
    countryGroupDf = getCountryGroup()
    topwarDf = topwarDf.merge(countryGroupDf, on='country', how='left')
    lastwarDf = lastwarDf.merge(countryGroupDf, on='country', how='left')
    topherosDf = topherosDf.merge(countryGroupDf, on='country', how='left')

    
    # 然后topwar、lastwar、topheros 三个app进行比较
    # 拆分 app_package作为平台（安卓、ios）
    # 1、国家花费分布
    # 输出格式：app, app_package, countrygroup, cost_ratio

    # 2、媒体花费分布
    # 输出格式：app, app_package, mediasource, cost_ratio
    # Merge country group information
    
    # Function to calculate cost ratio by country group
    def calculate_cost_ratio_by_country(df, app_name):
        country_group_cost = df.groupby(['app_package', 'countrygroup'])['cost'].sum().reset_index()
        total_cost = country_group_cost.groupby('app_package')['cost'].transform('sum')
        country_group_cost['cost_ratio'] = country_group_cost['cost'] / total_cost
        country_group_cost['app'] = app_name
        return country_group_cost[['app', 'app_package', 'countrygroup', 'cost_ratio']]

    # Function to calculate cost ratio by media source
    def calculate_cost_ratio_by_media(df, app_name):
        media_cost = df.groupby(['app_package', 'mediasource'])['cost'].sum().reset_index()
        total_cost = media_cost.groupby('app_package')['cost'].transform('sum')
        media_cost['cost_ratio'] = media_cost['cost'] / total_cost
        media_cost['app'] = app_name
        return media_cost[['app', 'app_package', 'mediasource', 'cost_ratio']]

    # Calculate cost ratios for each app
    topwar_country_cost_ratio = calculate_cost_ratio_by_country(topwarDf, 'Topwar')
    lastwar_country_cost_ratio = calculate_cost_ratio_by_country(lastwarDf, 'Lastwar')
    topheros_country_cost_ratio = calculate_cost_ratio_by_country(topherosDf, 'TopHeros')

    topwar_media_cost_ratio = calculate_cost_ratio_by_media(topwarDf, 'Topwar')
    lastwar_media_cost_ratio = calculate_cost_ratio_by_media(lastwarDf, 'Lastwar')
    topheros_media_cost_ratio = calculate_cost_ratio_by_media(topherosDf, 'TopHeros')

    # Combine results
    country_cost_ratios = pd.concat([topwar_country_cost_ratio, lastwar_country_cost_ratio, topheros_country_cost_ratio])
    media_cost_ratios = pd.concat([topwar_media_cost_ratio, lastwar_media_cost_ratio, topheros_media_cost_ratio])

    # Output results
    country_cost_ratios.to_csv('/src/data/country_cost_ratios.csv', index=False)
    media_cost_ratios.to_csv('/src/data/media_cost_ratios.csv', index=False)

def testFacebook():
    lastwarDf = getDataForLastwar()
    countryGroupDf = getCountryGroup()

    lastwarDf = lastwarDf.merge(countryGroupDf, on='country', how='left')

    # Function to calculate cost ratio by country group and media
    def calculate_cost_ratio_by_country_and_media(df, app_name):
        # Group by app_package, countrygroup, and mediasource to get the sum of costs
        country_media_cost = df.groupby(['app_package', 'countrygroup', 'mediasource'])['cost'].sum().reset_index()
        
        # Calculate the total cost for each app_package and countrygroup
        total_cost = country_media_cost.groupby(['app_package', 'countrygroup'])['cost'].transform('sum')
        
        # Calculate the cost ratio for each media
        country_media_cost['cost_ratio'] = country_media_cost['cost'] / total_cost
        country_media_cost['app'] = app_name
        
        return country_media_cost[['app', 'app_package', 'countrygroup', 'mediasource', 'cost_ratio']]
    
    # lastwarDf 中的媒体做一下整理，我只关心 facebook 的数据，将除了 facebook 之外的媒体都归为其他
    lastwarDf['mediasource'] = lastwarDf['mediasource'].apply(lambda x: 'Facebook' if x == 'Facebook Ads' else 'Other')

    # Calculate the cost ratio for each media within each app_package and countrygroup
    lastwar_country_media_cost_ratio = calculate_cost_ratio_by_country_and_media(lastwarDf, 'Lastwar')

    print(lastwar_country_media_cost_ratio)

def main2():
    topwarDf = getDataForTopwar()
    lastwarDf = getDataForLastwar()
    topherosDf = getDataForTopHeros()

    # 先将country汇总到countrygroup
    countryGroupDf = getCountryGroup()
    topwarDf = topwarDf.merge(countryGroupDf, on='country', how='left')
    lastwarDf = lastwarDf.merge(countryGroupDf, on='country', how='left')
    topherosDf = topherosDf.merge(countryGroupDf, on='country', how='left')

    # Function to calculate CPM, CPI, ROI
    def calculate_metrics(df, app_name):
        df['cpm'] = df['cost'] / df['impression'] * 1000
        df['cpi'] = df['cost'] / df['install']
        df['roi'] = df['revenue_d7'] / df['cost']
        df['app'] = app_name

        N = 0.02

        df['cpm'] = df['cpm'].clip(lower=df['cpm'].quantile(N), upper=df['cpm'].quantile(1-N))
        df['cpi'] = df['cpi'].clip(lower=df['cpi'].quantile(N), upper=df['cpi'].quantile(1-N))
        df['roi'] = df['roi'].clip(lower=df['roi'].quantile(N), upper=df['roi'].quantile(1-N))

        df['cpm'] = df['cpm'].fillna(0)
        df['cpi'] = df['cpi'].fillna(0)
        df['roi'] = df['roi'].fillna(0)

        return df[['app', 'app_package', 'countrygroup', 'mediasource', 'install_day', 'cpm', 'cpi', 'roi']]

    # Calculate metrics for each app
    topwar_metrics = calculate_metrics(topwarDf, 'Topwar')
    lastwar_metrics = calculate_metrics(lastwarDf, 'Lastwar')
    topheros_metrics = calculate_metrics(topherosDf, 'TopHeros')

    # Combine results
    all_metrics = pd.concat([topwar_metrics, lastwar_metrics, topheros_metrics])

    # Save results
    all_metrics.to_csv('/src/data/20250103_metrics.csv', index=False)

    all_metrics['install_day'] = pd.to_datetime(all_metrics['install_day'], format='%Y%m%d')
    all_metrics = all_metrics[all_metrics['mediasource'].isin(['Facebook Ads', 'googleadwords_int', 'applovin_int'])]
    all_metrics = all_metrics[all_metrics['app_package'].isin(['com.topwar.gp','id1479198816','com.fun.lastwar.gp','id6448786147','com.greenmushroom.boomblitz.gp','id6450953550'])]
    
    # Plotting
    def plot_metric(metric, ylabel, filename):
        for (countrygroup, mediasource), group in all_metrics.groupby(['countrygroup', 'mediasource']):
            
            plt.figure(figsize=(18, 6))
            for (app, app_package), sub_group in group.groupby(['app', 'app_package']):
                sub_group = sub_group.sort_values('install_day', ascending=True)
                
                df0 = sub_group.copy()
                df0['draw'] = df0[metric].rolling(window=14).mean()

                plt.plot(df0['install_day'], df0['draw'], label=f'{app}-{app_package}')
            plt.xlabel('Install Day')
            plt.ylabel(ylabel)
            plt.title(f'{ylabel} Over Time for {countrygroup} - {mediasource}')
            plt.legend()
            print(f'/src/data/20250103_{countrygroup}_{mediasource}_{filename}.png')
            plt.savefig(f'/src/data/20250103_{countrygroup}_{mediasource}_{filename}.png')
            plt.close()

    plot_metric('cpm', 'CPM', 'cpm')
    plot_metric('cpi', 'CPI', 'cpi')
    plot_metric('roi', 'ROI', 'roi')


if __name__ == '__main__':
    # main()
    # testFacebook()

    main2()