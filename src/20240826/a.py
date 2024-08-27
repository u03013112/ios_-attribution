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
having
    sum(cost_value_usd) > 0;
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
    interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int']
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
        # 保存数据为CSV文件
        output_csv_path = f'/src/data/20240826_{idx}.csv'
        data[['install_day', 'mediasource', 'revenue_proportion', 'cost_proportion']].to_csv(output_csv_path, index=False)
        
        plt.figure(figsize=(14, 7))
        
        ax1 = plt.gca()
        ax2 = ax1.twinx()
        
        colors = {
            'applovin_int': 'red',
            'Facebook Ads': 'blue',
            'googleadwords_int': 'yellow'
        }
        
        for mediasource in interested_medias:
            media_data = data[data['mediasource'] == mediasource]
            if not media_data.empty:
                ax1.plot(media_data['install_day'], media_data['cost_proportion'], marker='o', color=colors[mediasource], label=f'{mediasource} Cost')
                ax2.plot(media_data['install_day'], media_data['revenue_proportion'], marker='x', color=colors[mediasource], label=f'{mediasource} Revenue', linestyle='dashed')
        
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Cost Proportion')
        ax2.set_ylabel('Revenue Proportion')
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.title(f'Cost and Revenue Proportions for {country} - android')
        
        output_img_path = f'/src/data/20240826_{idx}.png'
        plt.savefig(output_img_path)
        plt.close()
        print(f'Saved plot to {output_img_path}')
        print(f'Saved data to {output_csv_path}')


def step1():
    df = getMediaCostDataFromMC(installTimeStart='20240101', installTimeEnd='20240630')

    # Convert install_day to datetime
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    
    # Add week column
    df['week'] = df['install_day'].dt.isocalendar().week
    
    # Aggregate data by week, country, app_package, and mediasource
    weekly_df = df.groupby(['country', 'app_package', 'week', 'mediasource']).agg({
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
    
    print(weekly_df)

    print(weekly_df[weekly_df['mediasource'] == 'applovin_int'])
    
    weekly_df.to_csv('/src/data/20240826_weekly_df.csv', index=False)

    # Detect changes and plot
    detect_and_plot_changes(weekly_df)


if __name__ == '__main__':
    step1()