import os
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getMediaCostDataFromMC(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwMediaCostData_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
select
    install_day,
    country,
    mediasource,
    app_package,
    sum(cost_value_usd) as cost,
    sum(revenue_d7) as r7usd
from
    rg_bi.dws_overseas_public_roi
where
    app = '502'
    and zone = 0
    and facebook_segment in ('N/A', 'country')
    and install_day between {installTimeStart} and {installTimeEnd}
group by
    install_day,
    country,
    mediasource,
    app_package
        '''
        df = execSql(sql)
        df.to_csv(filename,index=False)

    return df

def categorize_proportion(proportion):
    if proportion <= 0.01:
        return 1
    elif proportion <= 0.05:
        return 2
    elif proportion <= 0.10:
        return 3
    elif proportion <= 0.20:
        return 4
    elif proportion <= 0.50:
        return 5
    else:
        return 6

def detect_and_plot_changes(df):
    # 过滤媒体
    interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int','Organic']
    df = df[df['mediasource'].isin(interested_medias)]
    
    # 标注平台
    df['platform'] = df['app_package'].map({
        'com.fun.lastwar.gp': 'android',
        'id6448786147': 'iOS'
    })

    # 只保留安卓平台数据
    df = df[df['platform'] == 'android']
    
    # 按国家和应用包分组
    grouped = df.groupby(['country', 'app_package'])
    
    changes = []
    
    for (country, app_package), group in grouped:
        group = group.sort_values(by='install_day')
        applovin_group = group[group['mediasource'] == 'applovin_int']
        
        for i in range(1, len(applovin_group)):
            prev_row = applovin_group.iloc[i - 1]
            curr_row = applovin_group.iloc[i]
            
            if prev_row['cost_proportion_category'] != curr_row['cost_proportion_category']:
                start_date = prev_row['install_day'] - pd.Timedelta(weeks=1)
                end_date = curr_row['install_day'] + pd.Timedelta(weeks=1)
                
                surrounding_weeks = group[(group['install_day'] >= start_date) & (group['install_day'] <= end_date)]
                
                # 过滤掉收入比例有空值的数据
                if surrounding_weeks['revenue_proportion'].isnull().any():
                    continue
                
                # 计算花费金额差异
                def calc_cost_diff(media_data):
                    max_cost = media_data['cost_proportion'].max()
                    min_cost = media_data['cost_proportion'].min()
                    return abs(max_cost - min_cost) / max_cost
                
                applovin_diff = calc_cost_diff(surrounding_weeks[surrounding_weeks['mediasource'] == 'applovin_int'])
                facebook_diff = calc_cost_diff(surrounding_weeks[surrounding_weeks['mediasource'] == 'Facebook Ads'])
                google_diff = calc_cost_diff(surrounding_weeks[surrounding_weeks['mediasource'] == 'googleadwords_int'])
                
                # 过滤条件
                if applovin_diff > 0.2 and facebook_diff < 0.2 and google_diff < 0.2:
                    changes.append((country, app_package, surrounding_weeks))
    
    print(f'Found {len(changes)} changes')
    for idx, (country, app_package, data) in enumerate(changes):
        # 调用 drawPic2 函数
        output_prefix = f'/src/data/20240826_{idx}'
        data.rename(columns={'r7usd':'revenue'},inplace=True)
        drawPic2(data, f'{output_prefix}_{country}')
        # print(f'Processed change {idx} for {country} - {app_package}')
    

def step1():
    df = getMediaCostDataFromMC(installTimeStart='20240101', installTimeEnd='20240730')

    # Convert install_day to datetime
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    
    # 获取所有可能的 install_day（从开始日期到结束日期）
    all_days = pd.date_range(start=df['install_day'].min(), end=df['install_day'].max(), freq='D')

    # 创建一个 DataFrame，包含所有可能的组合
    all_combinations = pd.MultiIndex.from_product([df['country'].unique(),
                                                df['app_package'].unique(),
                                                all_days,
                                                df['mediasource'].unique()],
                                                names=['country', 'app_package', 'install_day', 'mediasource']).to_frame(index=False)

    # 合并原始数据和所有可能的组合
    merged_df = pd.merge(all_combinations, df, on=['country', 'app_package', 'install_day', 'mediasource'], how='left')

    # 填补缺失值
    merged_df['cost'] = merged_df['cost'].fillna(0)
    merged_df['r7usd'] = merged_df['r7usd'].fillna(0)

    # 重新计算 week 列
    merged_df['week'] = merged_df['install_day'].dt.isocalendar().week

    # 按 week, country, app_package, and mediasource 汇总数据
    weekly_df = merged_df.groupby(['country', 'app_package', 'week', 'mediasource']).agg({
        'install_day': 'min',
        'cost': 'sum',
        'r7usd': 'sum'
    }).reset_index()
    
    # Group by country, app_package, and week to calculate proportions
    grouped = weekly_df.groupby(['country', 'app_package', 'week','install_day'])
    
    # Calculate the cost and revenue proportions
    weekly_df['cost_proportion'] = grouped['cost'].apply(lambda x: x / x.sum()).values
    weekly_df['revenue_proportion'] = grouped['r7usd'].apply(lambda x: x / x.sum()).values
    
    # Categorize the proportions
    weekly_df['cost_proportion_category'] = weekly_df['cost_proportion'].apply(categorize_proportion)
    weekly_df['revenue_proportion_category'] = weekly_df['revenue_proportion'].apply(categorize_proportion)
    
    # print(weekly_df)

    # print(weekly_df[weekly_df['mediasource'] == 'applovin_int'])
    
    weekly_df.to_csv('/src/data/20240826_weekly_df.csv', index=False)

    # Detect changes and plot
    detect_and_plot_changes(weekly_df)

def drawPic(df,saveFilename):
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y-%m-%d')
    # 确保只包含我们感兴趣的媒体
    interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int']
    
    # 按install_day和mediasource分组并汇总
    grouped = df.groupby(['install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    # 计算比例
    grouped['cost_proportion'] = grouped['cost'] / grouped.groupby('install_day')['cost'].transform('sum')
    grouped['revenue_proportion'] = grouped['revenue'] / grouped.groupby('install_day')['revenue'].transform('sum')
    
    # 过滤媒体
    grouped = grouped[grouped['mediasource'].isin(interested_medias)]
    
    plt.figure(figsize=(14, 7))
    
    ax1 = plt.gca()
    ax2 = ax1.twinx()
    
    colors = {
        'applovin_int': 'red',
        'Facebook Ads': 'blue',
        'googleadwords_int': 'yellow'
    }
    
    for mediasource in interested_medias:
        media_data = grouped[grouped['mediasource'] == mediasource]
        if not media_data.empty:
            ax1.plot(media_data['install_day'], media_data['cost_proportion'], marker='o', color=colors[mediasource], label=f'{mediasource} Cost')
            ax2.plot(media_data['install_day'], media_data['revenue_proportion'], marker='x', color=colors[mediasource], label=f'{mediasource} Revenue', linestyle='dashed')
    
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Cost Proportion')
    ax2.set_ylabel('Revenue Proportion')
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    plt.title('Cost and Revenue Proportions')
    plt.savefig(saveFilename)
    plt.close()
    print(f'Saved plot to {saveFilename}')

def drawPic2(df, saveFilenamePrefix):
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y-%m-%d')
    interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int','Organic']
    
    # 按install_day和mediasource分组并汇总
    grouped = df.groupby(['install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    # 计算比例
    grouped['cost_proportion'] = grouped['cost'] / grouped.groupby('install_day')['cost'].transform('sum')
    grouped['revenue_proportion'] = grouped['revenue'] / grouped.groupby('install_day')['revenue'].transform('sum')
    
    # 过滤媒体
    grouped = grouped[grouped['mediasource'].isin(interested_medias)]
    
    # 保存比例图数据
    proportions_csv_path = f'{saveFilenamePrefix}_proportions.csv'
    grouped[['install_day', 'mediasource', 'cost_proportion', 'revenue_proportion']].to_csv(proportions_csv_path, index=False)
    print(f'Saved proportions data to {proportions_csv_path}')
    
    # 绘制比例图
    plt.figure(figsize=(14, 7))
    ax1 = plt.gca()
    ax2 = ax1.twinx()
    
    colors = {
        'applovin_int': 'red',
        'Facebook Ads': 'blue',
        'googleadwords_int': 'green',
        'Organic': 'purple'
    }
    
    for mediasource in interested_medias:
        media_data = grouped[grouped['mediasource'] == mediasource]
        if not media_data.empty:
            if mediasource != 'Organic':
                ax1.plot(media_data['install_day'], media_data['cost_proportion'], marker='o', color=colors[mediasource], label=f'{mediasource} Cost')
            ax2.plot(media_data['install_day'], media_data['revenue_proportion'], marker='x', color=colors[mediasource], label=f'{mediasource} Revenue', linestyle='dashed')
    
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Cost Proportion')
    ax2.set_ylabel('Revenue Proportion')
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    plt.title('Cost and Revenue Proportions')
    proportions_img_path = f'{saveFilenamePrefix}_proportions.png'
    plt.savefig(proportions_img_path)
    plt.close()
    print(f'Saved proportions plot to {proportions_img_path}')
    
    # 保存金额图数据
    usd_csv_path = f'{saveFilenamePrefix}_usd.csv'
    grouped[['install_day', 'mediasource', 'cost', 'revenue']].to_csv(usd_csv_path, index=False)
    print(f'Saved USD data to {usd_csv_path}')
    
    # 绘制金额图
    plt.figure(figsize=(14, 7))
    ax1 = plt.gca()
    ax2 = ax1.twinx()
    
    for mediasource in interested_medias:
        media_data = grouped[grouped['mediasource'] == mediasource]
        if not media_data.empty:
            if mediasource != 'Organic':
                ax1.plot(media_data['install_day'], media_data['cost'], marker='o', color=colors[mediasource], label=f'{mediasource} Cost')
            ax2.plot(media_data['install_day'], media_data['revenue'], marker='x', color=colors[mediasource], label=f'{mediasource} Revenue', linestyle='dashed')
    
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Cost (USD)')
    ax2.set_ylabel('Revenue (USD)')
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    plt.title('Cost and Revenue in USD')
    usd_img_path = f'{saveFilenamePrefix}_usd.png'
    plt.savefig(usd_img_path)
    plt.close()
    print(f'Saved USD plot to {usd_img_path}')

def debug():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df2 = df[
        (df['country'] == 'UK') &
        (df['app_package'] == 'com.fun.lastwar.gp')
    ]
    df2 = df2[df2['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int'])]
    df2.rename(columns={'r7usd':'revenue'},inplace=True)
    # 按照applovin花费比例分类，计算各媒体ROI
    df2 = df2[['mediasource','install_day','cost','revenue','cost_proportion','revenue_proportion']]
    typeList = [
        {'type':'0~10%','min':0,'max':0.1},
        {'type':'10%~20%','min':0.1,'max':0.2},
        {'type':'20%~30%','min':0.2,'max':0.3},
        {'type':'30%~40%','min':0.3,'max':0.4},
        {'type':'40%~50%','min':0.4,'max':0.5},
        {'type':'50%~100%','min':0.5,'max':1}
    ]

    # 按 install_day 分组
    grouped = df2.groupby('install_day')
    
    # 初始化分类结果的列表
    classified_data = []

    for install_day, group in grouped:
        # 获取 applovin_int 的 cost_proportion
        applovin_cost_proportion = group[group['mediasource'] == 'applovin_int']['cost_proportion'].values
        if len(applovin_cost_proportion) > 0:
            applovin_cost_proportion = applovin_cost_proportion[0]
            
            # 确定分类类型
            for t in typeList:
                if t['min'] <= applovin_cost_proportion < t['max']:
                    group['type'] = t['type']
                    classified_data.append(group)
                    break
    
    # 合并分类结果
    classified_df = pd.concat(classified_data)
    
    # 按类型进行汇总
    summary = classified_df.groupby(['type', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    # 计算 ROI
    summary['ROI'] = summary['revenue'] / summary['cost']
    
    summary = summary.sort_values(by=['mediasource','type'], ascending=True)
    # 打印结果
    print(summary)

# 按照applovin花费比例，进行分国家、分媒体汇总，然后对汇总数据进行相关性分析
def debug_more():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    # df = df[df['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int', 'Organic'])]
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 定义类型列表
    # typeList = [
    #     {'type': '0~10%', 'min': 0, 'max': 0.1},
    #     {'type': '10%~20%', 'min': 0.1, 'max': 0.2},
    #     {'type': '20%~30%', 'min': 0.2, 'max': 0.3},
    #     {'type': '30%~40%', 'min': 0.3, 'max': 0.4},
    #     {'type': '40%~50%', 'min': 0.4, 'max': 0.5},
    #     {'type': '50%~100%', 'min': 0.5, 'max': 1}
    # ]
    typeList = [
        {'type': '00%~05%', 'min': 0, 'max': 0.05},
        {'type': '05%~10%', 'min': 0.05, 'max': 0.1},
        {'type': '10%~15%', 'min': 0.1, 'max': 0.15},
        {'type': '15%~20%', 'min': 0.15, 'max': 0.2},
        {'type': '20%~25%', 'min': 0.2, 'max': 0.25},
        {'type': '25%~30%', 'min': 0.25, 'max': 0.3},
        {'type': '30%~100%', 'min': 0.3, 'max': 1}
    ]

    results = []

    countries = df['country'].unique()

    # 遍历所有国家
    for country in countries:
        df_country = df[df['country'] == country]
        df_country = df_country[['mediasource', 'install_day', 'cost', 'revenue', 'cost_proportion']]
        
        # 按 install_day 分组
        grouped = df_country.groupby('install_day')
        
        # 初始化分类结果的列表
        classified_data = []

        for install_day, group in grouped:
            # 计算每个 install_day 内的总花费
            total_cost = group['cost'].sum()
            
            # 计算 applovin_int 的 cost_proportion
            applovin_cost = group[group['mediasource'] == 'applovin_int']['cost'].sum()
            applovin_cost_proportion = applovin_cost / total_cost if total_cost > 0 else 0
            
            # 确定分类类型
            for t in typeList:
                if t['min'] <= applovin_cost_proportion < t['max']:
                    group['type'] = t['type']
                    classified_data.append(group)
                    break
        
        # 合并分类结果
        if classified_data:
            classified_df = pd.concat(classified_data)
            
            # 按类型进行汇总
            summary = classified_df.groupby(['type', 'mediasource']).agg({
                'cost': 'sum',
                'revenue': 'sum'
            }).reset_index()
            
            # 计算 ROI
            summary['ROI'] = summary['revenue'] / summary['cost']
            
            # 计算花费占比
            total_cost = summary.groupby('type')['cost'].transform('sum')
            summary['cost_proportion'] = summary['cost'] / total_cost
            
            # 只保留主要媒体数据
            summary = summary[summary['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int'])]

            # 重塑数据框架
            pivot = summary.pivot(index='type', columns='mediasource', values=['ROI', 'cost', 'cost_proportion']).reset_index()
            pivot.columns = ['type'] + [f'{col[1]}_{col[0]}' for col in pivot.columns[1:]]
            
            # 添加国家列
            pivot['country'] = country
            results.append(pivot)
    
    # 合并所有国家的结果
    final_df = pd.concat(results, ignore_index=True)
    # print(final_df[['type','applovin_int_cost_proportion']])

    # 计算每个国家的3个主要媒体总花费
    country_total_costs = final_df.groupby('country')[['applovin_int_cost', 'Facebook Ads_cost', 'googleadwords_int_cost']].sum()
    country_total_costs['total_cost'] = country_total_costs.sum(axis=1)
    
    # 按总花费降序排列国家
    sorted_countries = country_total_costs.sort_values(by='total_cost', ascending=False).index

    # 遍历每个国家，计算相关系数
    for country in sorted_countries:
        country_df = final_df[final_df['country'] == country]
        # print(country_df)
        country_df.to_csv(f'/src/data/20240826_debug2_{country}_df.csv', index=False)
        country_df = country_df.drop(columns=['type', 'country'])
        correlation_matrix = country_df.corr()

        if country_df.shape[0] <= 3:
            continue

        # 打印国家及其相关系数矩阵
        print(f"\n与applovin花费比例相关系数 {country}:")
        # 打印相关性结果
        # 打印相关性结果
        print(correlation_matrix.loc[['Facebook Ads_ROI', 'googleadwords_int_ROI'], 'applovin_int_cost_proportion'])

# debug_more 不分国家版本
def debug_more_all():
    print('debug_more_all')
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 定义类型列表
    # typeList = [
    #     {'type': '0~10%', 'min': 0, 'max': 0.1},
    #     {'type': '10%~20%', 'min': 0.1, 'max': 0.2},
    #     {'type': '20%~30%', 'min': 0.2, 'max': 0.3},
    #     {'type': '30%~40%', 'min': 0.3, 'max': 0.4},
    #     {'type': '40%~50%', 'min': 0.4, 'max': 0.5},
    #     {'type': '50%~100%', 'min': 0.5, 'max': 1}
    # ]
    typeList = [
        {'type': '00%~05%', 'min': 0, 'max': 0.05},
        {'type': '05%~10%', 'min': 0.05, 'max': 0.1},
        {'type': '10%~15%', 'min': 0.1, 'max': 0.15},
        {'type': '15%~20%', 'min': 0.15, 'max': 0.2},
        {'type': '20%~25%', 'min': 0.2, 'max': 0.25},
        {'type': '25%~30%', 'min': 0.25, 'max': 0.3},
        {'type': '30%~100%', 'min': 0.3, 'max': 1}
    ]

    results = []

    df['country'] = 'all'
    df = df.groupby(['country', 'install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()

    countries = df['country'].unique()

    # 遍历所有国家
    for country in countries:
        df_country = df[df['country'] == country]
        df_country = df_country[['mediasource', 'install_day', 'cost', 'revenue']]
        
        # 按 install_day 分组
        grouped = df_country.groupby('install_day')
        
        # 初始化分类结果的列表
        classified_data = []

        for install_day, group in grouped:
            # 计算每个 install_day 内的总花费
            total_cost = group['cost'].sum()
            
            # 计算 applovin_int 的 cost_proportion
            applovin_cost = group[group['mediasource'] == 'applovin_int']['cost'].sum()
            applovin_cost_proportion = applovin_cost / total_cost if total_cost > 0 else 0
            
            # 确定分类类型
            for t in typeList:
                if t['min'] <= applovin_cost_proportion < t['max']:
                    group['type'] = t['type']
                    classified_data.append(group)
                    break
        
        # 合并分类结果
        if classified_data:
            classified_df = pd.concat(classified_data)
            
            # 按类型进行汇总
            summary = classified_df.groupby(['type', 'mediasource']).agg({
                'cost': 'sum',
                'revenue': 'sum'
            }).reset_index()
            
            # 计算 ROI
            summary['ROI'] = summary['revenue'] / summary['cost']
            
            # 计算花费占比
            total_cost = summary.groupby('type')['cost'].transform('sum')
            summary['cost_proportion'] = summary['cost'] / total_cost
            
            # 只保留主要媒体数据
            summary = summary[summary['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int'])]

            # 重塑数据框架
            pivot = summary.pivot(index='type', columns='mediasource', values=['ROI', 'cost', 'cost_proportion']).reset_index()
            pivot.columns = ['type'] + [f'{col[1]}_{col[0]}' for col in pivot.columns[1:]]
            
            # 添加国家列
            pivot['country'] = country
            results.append(pivot)
    
    # 合并所有国家的结果
    final_df = pd.concat(results, ignore_index=True)
    print(final_df[['type','applovin_int_cost_proportion','applovin_int_ROI','Facebook Ads_ROI','googleadwords_int_ROI']])


    # 遍历每个国家，计算相关系数
    for country in final_df['country'].unique():
        country_df = final_df[final_df['country'] == country].drop(columns=['type', 'country'])
        correlation_matrix = country_df.corr()
        
        # print(country_df)
        country_df.to_csv(f'/src/data/20240826_debug2_{country}_df.csv', index=False)

        # if country_df.shape[0] <= 3:
        #     continue

        # 打印国家及其相关系数矩阵
        print(f"\n与applovin花费比例相关系数 {country}:")
        # 打印相关性结果
        # 打印相关性结果
        print(correlation_matrix.loc[['Facebook Ads_ROI', 'googleadwords_int_ROI', 'Facebook Ads_cost', 'googleadwords_int_cost'], 'applovin_int_cost_proportion'])


# 直接计算相关性
def debug_more2():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 只保留主要媒体数据
    df = df[df['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int'])]
    
    results = []

    # countries = df['country'].unique()
    # countries = ['US', 'KR', 'JP', 'TW', 'UK']

    df0 = df.groupby(['country']).agg({
        'cost': 'sum',
    }).reset_index()
    df0 = df0.sort_values(by='cost',ascending=False)
    countries = df0['country'].values.tolist()
    countries = countries[:10]

    # 遍历所有国家
    for country in countries:
        df_country = df[df['country'] == country]
        
        # 计算每个 install_day 的汇总数据
        summary = df_country.groupby(['install_day', 'mediasource']).agg({
            'cost': 'sum',
            'revenue': 'sum'
        }).reset_index()
        
        # 计算 ROI
        summary['ROI'] = summary['revenue'] / summary['cost']
        
        # 计算每个 install_day 中 applovin 的花费占比
        total_cost_per_day = summary.groupby('install_day')['cost'].sum().reset_index()
        total_cost_per_day.rename(columns={'cost': 'total_cost'}, inplace=True)
        summary = summary.merge(total_cost_per_day, on='install_day')
        summary['applovin_cost_ratio'] = summary.apply(
            lambda row: row['cost'] / row['total_cost'] if row['mediasource'] == 'applovin_int' else 0, axis=1
        )

        # 重塑数据框架
        pivot = summary.pivot(index='install_day', columns='mediasource', values=['cost', 'revenue', 'ROI']).reset_index()
        pivot.columns = ['install_day'] + [f'{col[1]}_{col[0]}' for col in pivot.columns[1:]]
        
        # 添加 applovin_cost_ratio 列
        applovin_cost_ratio = summary[summary['mediasource'] == 'applovin_int'][['install_day', 'applovin_cost_ratio']]
        pivot = pivot.merge(applovin_cost_ratio, on='install_day', how='left')

        # 添加国家列
        pivot['country'] = country
        results.append(pivot)
    
    # 合并所有国家的结果
    final_df = pd.concat(results, ignore_index=True)
    
    # 遍历每个国家，计算相关系数
    for country in final_df['country'].unique():
        country_df = final_df[final_df['country'] == country].drop(columns=['install_day', 'country'])
        country_df = country_df[['applovin_cost_ratio','applovin_int_ROI','Facebook Ads_ROI','googleadwords_int_ROI']]
        correlation_matrix = country_df.corr()
        
        # 打印国家及其相关系数矩阵
        print(f'>>applovin 花费比例 相关系数 {country}:')
        print(correlation_matrix['applovin_cost_ratio'])
        print(f'>>applovin ROI 相关系数: {country}')
        print(correlation_matrix['applovin_int_ROI'])

# debug_more2 不分国家 版本
def debug_more2_all():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 只保留主要媒体数据
    df = df[df['mediasource'].isin(['applovin_int', 'Facebook Ads', 'googleadwords_int'])]
    
    results = []

    df['country'] = 'all'
    df = df.groupby(['country', 'install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()

    countries = ['all']

    # 遍历所有国家
    for country in countries:
        df_country = df[df['country'] == country]
        
        # 计算每个 install_day 的汇总数据
        summary = df_country.groupby(['install_day', 'mediasource']).agg({
            'cost': 'sum',
            'revenue': 'sum'
        }).reset_index()
        
        # 计算 ROI
        summary['ROI'] = summary['revenue'] / summary['cost']
        
        # 计算每个 install_day 中 applovin 的花费占比
        total_cost_per_day = summary.groupby('install_day')['cost'].sum().reset_index()
        total_cost_per_day.rename(columns={'cost': 'total_cost'}, inplace=True)
        summary = summary.merge(total_cost_per_day, on='install_day')
        summary['applovin_cost_ratio'] = summary.apply(
            lambda row: row['cost'] / row['total_cost'] if row['mediasource'] == 'applovin_int' else 0, axis=1
        )

        # 重塑数据框架
        pivot = summary.pivot(index='install_day', columns='mediasource', values=['cost', 'revenue', 'ROI']).reset_index()
        pivot.columns = ['install_day'] + [f'{col[1]}_{col[0]}' for col in pivot.columns[1:]]
        
        # 添加 applovin_cost_ratio 列
        applovin_cost_ratio = summary[summary['mediasource'] == 'applovin_int'][['install_day', 'applovin_cost_ratio']]
        pivot = pivot.merge(applovin_cost_ratio, on='install_day', how='left')

        # 添加国家列
        pivot['country'] = country
        results.append(pivot)
    
    # 合并所有国家的结果
    final_df = pd.concat(results, ignore_index=True)
    
    # 遍历每个国家，计算相关系数
    for country in final_df['country'].unique():
        country_df = final_df[final_df['country'] == country].drop(columns=['install_day', 'country'])
        correlation_matrix = country_df.corr()
        
        # 打印国家及其相关系数矩阵
        print('\n', country)
        print('>>applovin_cost_ratio corr:')
        print(correlation_matrix['applovin_cost_ratio'])
        print('>>applovin_int_ROI corr:')
        print(correlation_matrix['applovin_int_ROI'])

def debug2():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    
    # 重命名 r7usd 列为 revenue
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 只保留特定的国家
    countries = ['US', 'KR', 'JP', 'TW', 'UK']
    # countries = ['US']
    df = df[df['country'].isin(countries)]
    
    # 按 install_day 和 country 计算 total_revenue
    total_revenue_per_day_country = df.groupby(['install_day', 'country'])['revenue'].sum().reset_index()
    total_revenue_per_day_country.rename(columns={'revenue': 'total_revenue'}, inplace=True)
    
    # 将 total_revenue 合并回原始数据框
    df = df.merge(total_revenue_per_day_country, on=['install_day', 'country'])
    
    # 按 install_day 和 country 计算 total_cost
    total_cost_per_day_country = df.groupby(['install_day', 'country'])['cost'].sum().reset_index()
    total_cost_per_day_country.rename(columns={'cost': 'total_cost'}, inplace=True)
    
    # 将 total_cost 合并回原始数据框
    df = df.merge(total_cost_per_day_country, on=['install_day', 'country'])
    
    # 计算每一行的 media_cost_proportion
    df['media_cost_proportion'] = df['cost'] / df['total_cost']
    
    # 定义类型列表
    typeList = [
        {'type': '0~10%', 'min': 0, 'max': 0.1},
        {'type': '10%~20%', 'min': 0.1, 'max': 0.2},
        {'type': '20%~30%', 'min': 0.2, 'max': 0.3},
        {'type': '30%~40%', 'min': 0.3, 'max': 0.4},
        {'type': '40%~50%', 'min': 0.4, 'max': 0.5},
        {'type': '50%~100%', 'min': 0.5, 'max': 1}
    ]
    
    # 根据 media_cost_proportion 添加 type 列
    def get_type(proportion):
        for t in typeList:
            if t['min'] <= proportion < t['max']:
                return t['type']
        return None
    
    df['type'] = df['media_cost_proportion'].apply(get_type)

    # 初始化结果DataFrame
    results = pd.DataFrame(columns=['type', 'media', 'country', 'cost_revenue_corr', 'cost_total_revenue_corr'])
    
    # 遍历每种类型
    for t in typeList:
        type_df = df[df['type'] == t['type']]
        
        if type_df.empty:
            continue
        
        # 遍历每个媒体和每个国家
        for media in ['applovin_int', 'Facebook Ads', 'googleadwords_int']:
            for country in countries:
                media_country_df = type_df[(type_df['mediasource'] == media) & (type_df['country'] == country)][['cost', 'revenue', 'total_revenue']]
                
                # 判断行数是否足够
                if len(media_country_df) < 3:
                    continue
                
                # 计算相关性
                correlation_matrix = media_country_df.corr()
                
                # 获取相关性结果
                cost_revenue_corr = correlation_matrix.loc['cost', 'revenue']
                cost_total_revenue_corr = correlation_matrix.loc['cost', 'total_revenue']
                
                # 将结果添加到DataFrame中
                results = results.append({
                    'type': t['type'],
                    'media': media,
                    'country': country,
                    'cost_revenue_corr': cost_revenue_corr,
                    'cost_total_revenue_corr': cost_total_revenue_corr
                }, ignore_index=True)
    
    # 保存结果到CSV文件
    results.to_csv('/src/data/correlation_results_by_country.csv', index=False)


def debug2_all():
    df = pd.read_csv('/src/data/20240826_weekly_df.csv')
    df = df[df['app_package'] == 'com.fun.lastwar.gp']
    
    # 重命名 r7usd 列为 revenue
    df.rename(columns={'r7usd': 'revenue'}, inplace=True)
    
    # 去除国家列
    df.drop(columns=['country'], inplace=True)
    
    # 按 install_day 和 mediasource 进行数据汇总
    df = df.groupby(['install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    # 定义类型列表
    typeList = [
        {'type': '0~10%', 'min': 0, 'max': 0.1},
        {'type': '10%~20%', 'min': 0.1, 'max': 0.2},
        {'type': '20%~30%', 'min': 0.2, 'max': 0.3},
        {'type': '30%~40%', 'min': 0.3, 'max': 0.4},
        {'type': '40%~50%', 'min': 0.4, 'max': 0.5},
        {'type': '50%~100%', 'min': 0.5, 'max': 1}
    ]

    # 按 install_day 计算 total_revenue
    total_revenue_per_day = df.groupby('install_day')['revenue'].sum().reset_index()
    total_revenue_per_day.rename(columns={'revenue': 'total_revenue'}, inplace=True)
    
    # 将 total_revenue 合并回原始数据框
    df = df.merge(total_revenue_per_day, on='install_day')
    
    # 按 install_day 分组
    grouped = df.groupby('install_day')
    
    # 初始化分类结果的列表
    classified_data = []

    for install_day, group in grouped:
        # 计算每个 install_day 内的总花费
        total_cost = group['cost'].sum()
        
        # 计算每个媒体的 cost_proportion
        for media in ['applovin_int', 'Facebook Ads', 'googleadwords_int']:
            media_cost = group[group['mediasource'] == media]['cost'].sum()
            media_cost_proportion = media_cost / total_cost if total_cost > 0 else 0
            
            # 确定分类类型
            for t in typeList:
                if t['min'] <= media_cost_proportion < t['max']:
                    media_group = group[group['mediasource'] == media].copy()
                    media_group['type'] = t['type']
                    classified_data.append(media_group)
                    break
    
    # 合并分类结果
    if classified_data:
        classified_df = pd.concat(classified_data)
        
        # 初始化结果DataFrame
        results = pd.DataFrame(columns=['type', 'media', 'cost_revenue_corr', 'cost_total_revenue_corr'])
        
        # 遍历每种类型
        for t in typeList:
            type_df = classified_df[classified_df['type'] == t['type']]
            
            if type_df.empty:
                continue
            
            # 遍历每个媒体
            for media in ['applovin_int', 'Facebook Ads', 'googleadwords_int']:
                media_df = type_df[type_df['mediasource'] == media][['cost', 'revenue', 'total_revenue']]
                
                # 判断行数是否足够
                if len(media_df) < 3:
                    continue
                
                # 计算相关性
                correlation_matrix = media_df.corr()
                
                # 获取相关性结果
                cost_revenue_corr = correlation_matrix.loc['cost', 'revenue']
                cost_total_revenue_corr = correlation_matrix.loc['cost', 'total_revenue']
                
                # 将结果添加到DataFrame中
                results = results.append({
                    'type': t['type'],
                    'media': media,
                    'cost_revenue_corr': cost_revenue_corr,
                    'cost_total_revenue_corr': cost_total_revenue_corr
                }, ignore_index=True)
        
        # 保存结果到CSV文件
        results.to_csv('/src/data/correlation_results_all.csv', index=False)


if __name__ == '__main__':
    # step1()
    # debug()
    # debug_more()
    # debug_more_all()

    debug_more2()
    # debug_more2_all()

    # debug2()
    # debug2_all()