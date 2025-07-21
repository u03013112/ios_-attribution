# 获取applovin goal roas数据，与applovin对应的花费、回收、roi等数据
# 尝试分析是否有相关性或者其他关联
# 争取找到对应关系

import os
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import seaborn as sns

import sys

from torch import dtype

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO


def getData():
    filename = '/src/data/20250718_data.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename,dtype={'install_day': str})
        print(f"Loaded data from {filename}")
    else:

        sql = """
SELECT 
    t1.install_day,
    t1.country,
    t1.campaign_id,
    t1.campaign_name,
    t1.campaign_roas_goal,
    t2.cost_value_usd,
    t2.af_revenue_d7_cohort,
    t3.gpir_revenue_d7_cohort,
    t4.onlyprofit_cost,
    t4.onlyprofit_revenue_d7_cohort
FROM 
    (
        SELECT
            stat_date as install_day,
            campaign_id,
            campaign_name,
            country,
            campaign_roas_goal
        FROM
            ods_holo_applovin_campaign_data
        WHERE
            campaign_package_name = 'com.fun.lastwar.gp'
            AND report_type = 'report'
    ) t1
INNER JOIN 
    (
        SELECT
            install_day,
            country,
            campaign_id,
            SUM(cost_value_usd) as cost_value_usd,
            SUM(revenue_h168) as af_revenue_d7_cohort
        FROM
            dws_overseas_public_roi
        WHERE
            app = '502'
            AND app_package = 'com.fun.lastwar.gp'
            AND facebook_segment IN ('country', 'N/A')
            AND mediasource = 'applovin_int'
        GROUP BY
            install_day,
            country,
            campaign_id
    ) t2 ON t1.install_day = t2.install_day 
           AND t1.country = t2.country 
           AND t1.campaign_id = t2.campaign_id
INNER JOIN 
    (
        SELECT
            install_day,
            country,
            campaign_id,
            SUM(revenue_h168) as gpir_revenue_d7_cohort
        FROM
            ads_lastwar_mediasource_reattribution
        WHERE
            facebook_segment IN ('country', 'N/A')
            AND mediasource = 'applovin_int'
        GROUP BY
            install_day,
            country,
            campaign_id
    ) t3 ON COALESCE(t1.install_day, t2.install_day) = t3.install_day 
           AND COALESCE(t1.country, t2.country) = t3.country 
           AND COALESCE(t1.campaign_id, t2.campaign_id) = t3.campaign_id
INNER JOIN 
    (
        SELECT
            install_day,
            country,
            campaign_id,
            SUM(cost_value_usd) AS onlyprofit_cost,
            SUM(revenue_h168) as onlyprofit_revenue_d7_cohort
        FROM 
            dws_overseas_lastwar_roi_onlyprofit
        WHERE
            app = '502'
            AND mediasource = 'applovin_int'
        GROUP BY
            install_day,
            country,
            campaign_id
    ) t4 ON COALESCE(t1.install_day, t2.install_day, t3.install_day) = t4.install_day 
           AND COALESCE(t1.country, t2.country, t3.country) = t4.country 
           AND COALESCE(t1.campaign_id, t2.campaign_id, t3.campaign_id) = t4.campaign_id
ORDER BY 
    install_day, country, campaign_id
;
        """
        print(f"SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)
        print(f"Executed SQL and saved data to {filename}")
    
    return df

def tryToAnalyze(df):
    # 数据太多，先过滤一下只看US
    us_df = df[
        (df['country'] == 'US')
        & (df['install_day'] >= '20240101')
    ].copy()
    print(f"US data shape: {us_df.shape}")
    
    if us_df.empty:
        print("No US data found")
        return
    
    # 转换日期格式
    us_df['install_day'] = pd.to_datetime(us_df['install_day'], format='%Y%m%d')
    
    # 计算ROI指标
    us_df['af_roi'] = us_df['af_revenue_d7_cohort'] / us_df['cost_value_usd']
    us_df['gpir_roi'] = us_df['gpir_revenue_d7_cohort'] / us_df['cost_value_usd']
    us_df['onlyprofit_roi'] = us_df['onlyprofit_revenue_d7_cohort'] / us_df['onlyprofit_cost']
    
    # 削弱异常值，将roi大于0.2的值设为0.2
    us_df['af_roi'] = us_df['af_roi'].clip(upper=0.2)
    us_df['gpir_roi'] = us_df['gpir_roi'].clip(upper=0.2)
    us_df['onlyprofit_roi'] = us_df['onlyprofit_roi'].clip(upper=0.2)

    # 处理无穷大和NaN值
    us_df = us_df.replace([np.inf, -np.inf], np.nan)
    
    # 按照campaign_id分组
    campaign_ids = us_df['campaign_id'].unique()
    print(f"Found {len(campaign_ids)} campaigns: {campaign_ids}")
    
    # 设置图表样式
    plt.style.use('default')
    
    for campaign_id in campaign_ids:
        campaign_data = us_df[us_df['campaign_id'] == campaign_id].copy()
        campaign_data = campaign_data.sort_values('install_day')
        
        # 获取campaign名称
        campaign_name = campaign_data['campaign_name'].iloc[0] if not campaign_data.empty else str(campaign_id)
        print(f"\nAnalyzing Campaign: {campaign_name} (ID: {campaign_id})")
        # 如果数据少于30天，则跳过
        if len(campaign_data) < 30:
            print(f"Skipping campaign {campaign_id} due to insufficient data (only {len(campaign_data)} days)")
            continue

        # 创建子图
        fig, axes = plt.subplots(3, 1, figsize=(15, 12), sharex=True)
        fig.suptitle(f'Campaign Analysis: {campaign_name} (ID: {campaign_id})', fontsize=16, fontweight='bold')
        
        # 第一张小图：campaign_roas_goal
        ax1 = axes[0]
        ax1.plot(campaign_data['install_day'], campaign_data['campaign_roas_goal'], 
                # marker='o', 
                linewidth=1, markersize=6, color='blue')
        ax1.set_ylabel('Campaign ROAS Goal', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        ax1.set_title('ROAS Goal Over Time')
        
        # 第二张小图：cost_value_usd 和 onlyprofit_cost
        ax2 = axes[1]
        ax2.plot(campaign_data['install_day'], campaign_data['cost_value_usd'], 
                # marker='s', 
                linewidth=1, markersize=6, color='red', label='Cost Value USD')
        ax2.plot(campaign_data['install_day'], campaign_data['onlyprofit_cost'], 
                # marker='^',
                linewidth=1, markersize=6, color='orange', label='OnlyProfit Cost')
        ax2.set_ylabel('Cost ($)', fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_title('Cost Comparison')
        
        # 第三张小图：各种ROI
        ax3 = axes[2]
        ax3.plot(campaign_data['install_day'], campaign_data['af_roi'], 
                # marker='o', 
                linewidth=1, markersize=6, color='green', label='AF ROI')
        ax3.plot(campaign_data['install_day'], campaign_data['gpir_roi'], 
                # marker='s', 
                linewidth=1, markersize=6, color='purple', label='GPIR ROI')
        ax3.plot(campaign_data['install_day'], campaign_data['onlyprofit_roi'], 
                # marker='^', 
                linewidth=1, markersize=6, color='brown', label='OnlyProfit ROI')
        
        # # 添加ROAS Goal参考线
        # if not campaign_data['campaign_roas_goal'].isna().all():
        #     avg_goal = campaign_data['campaign_roas_goal'].mean()
        #     ax3.axhline(y=avg_goal, color='blue', linestyle='--', alpha=0.7, 
        #                label=f'Avg ROAS Goal ({avg_goal:.2f})')
        
        ax3.set_ylabel('ROI', fontweight='bold')
        ax3.set_xlabel('Install Day', fontweight='bold')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        ax3.set_title('ROI Comparison')
        
        # 格式化x轴日期
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(campaign_data)//10)))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 保存图片
        plt.savefig(f'/src/data/20250718_campaign_{campaign_id}_analysis.png', dpi=300, bbox_inches='tight')
        
        # 打印一些统计信息
        print(f"\n=== Campaign {campaign_id} Statistics ===")
        print(f"Campaign Name: {campaign_name}")
        print(f"Date Range: {campaign_data['install_day'].min()} to {campaign_data['install_day'].max()}")
        print(f"Total Cost (USD): ${campaign_data['cost_value_usd'].sum():.2f}")
        print(f"Average ROAS Goal: {campaign_data['campaign_roas_goal'].mean():.2f}")
        print(f"Average AF ROI: {campaign_data['af_roi'].mean():.2f}")
        print(f"Average GPIR ROI: {campaign_data['gpir_roi'].mean():.2f}")
        print(f"Average OnlyProfit ROI: {campaign_data['onlyprofit_roi'].mean():.2f}")
        
        # 分析ROAS Goal与实际ROI的关系
        correlation_af = campaign_data['campaign_roas_goal'].corr(campaign_data['af_roi'])
        correlation_gpir = campaign_data['campaign_roas_goal'].corr(campaign_data['gpir_roi'])
        correlation_onlyprofit = campaign_data['campaign_roas_goal'].corr(campaign_data['onlyprofit_roi'])
        
        print(f"Correlation between ROAS Goal and AF ROI: {correlation_af:.3f}")
        print(f"Correlation between ROAS Goal and GPIR ROI: {correlation_gpir:.3f}")
        print(f"Correlation between ROAS Goal and OnlyProfit ROI: {correlation_onlyprofit:.3f}")
    
    # 创建一个总体相关性分析
    print("\n=== Overall Correlation Analysis (US Only) ===")
    correlation_matrix = us_df[['campaign_roas_goal', 'af_roi', 'gpir_roi', 'onlyprofit_roi']].corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='cool', center=0, 
                square=True, fmt='.3f', cbar_kws={'label': 'Correlation Coefficient'})
    plt.title('Correlation Matrix: ROAS Goal vs ROI Metrics', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('/src/data/20250718_correlation_matrix.png', dpi=300, bbox_inches='tight')

    
    # 打印相关性矩阵
    print(correlation_matrix)


# 尝试按周进行汇总，自然周，周一至周日
# 其中将goal在周中发生变化的周排除掉，只保留整周goal不变的，用goal平均（即周一的goal）
# 并将cost和revenue按周汇总
# 计算汇总后的cost和ROI
def groupbyWeek(df):
    # 数据太多，先过滤一下只看US
    us_df = df[
        (df['country'] == 'US')
        & (df['install_day'] >= '20240101')
    ].copy()
    print(f"US data shape: {us_df.shape}")
    
    if us_df.empty:
        print("No US data found")
        return
    
    # 转换日期格式
    us_df['install_day'] = pd.to_datetime(us_df['install_day'], format='%Y%m%d')
    
    # 添加周信息，周一为一周的开始
    us_df['week_start'] = us_df['install_day'] - pd.to_timedelta(us_df['install_day'].dt.dayofweek, unit='d')
    us_df['year_week'] = us_df['install_day'].dt.strftime('%Y-W%U')
    
    # 按照campaign_id分组处理
    campaign_ids = us_df['campaign_id'].unique()
    print(f"Found {len(campaign_ids)} campaigns: {campaign_ids}")
    
    all_weekly_data = []
    
    for campaign_id in campaign_ids:
        campaign_data = us_df[us_df['campaign_id'] == campaign_id].copy()
        campaign_name = campaign_data['campaign_name'].iloc[0] if not campaign_data.empty else str(campaign_id)
        
        # 按周分组，检查每周的goal是否一致
        weekly_groups = campaign_data.groupby('week_start')
        
        for week_start, week_data in weekly_groups:
            # 检查这一周是否有完整的7天数据（可选，根据需要调整）
            if len(week_data) < 5:  # 至少5天数据
                continue
                
            # 检查这一周的goal是否保持一致（允许小幅波动）
            goal_std = week_data['campaign_roas_goal'].std()
            if pd.isna(goal_std) or goal_std > 0.01:  # goal变化超过0.01则跳过
                continue
            
            # 汇总这一周的数据
            weekly_summary = {
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'week_start': week_start,
                'week_end': week_start + pd.Timedelta(days=6),
                'days_count': len(week_data),
                
                # Goal使用周一的值（或平均值）
                'campaign_roas_goal': week_data['campaign_roas_goal'].iloc[0],
                
                # Cost和Revenue按周汇总
                'total_cost_value_usd': week_data['cost_value_usd'].sum(),
                'total_onlyprofit_cost': week_data['onlyprofit_cost'].sum(),
                'total_af_revenue_d7': week_data['af_revenue_d7_cohort'].sum(),
                'total_gpir_revenue_d7': week_data['gpir_revenue_d7_cohort'].sum(),
                'total_onlyprofit_revenue_d7': week_data['onlyprofit_revenue_d7_cohort'].sum(),
            }
            
            all_weekly_data.append(weekly_summary)
    
    if not all_weekly_data:
        print("No valid weekly data found")
        return
    
    # 转换为DataFrame
    weekly_df = pd.DataFrame(all_weekly_data)
    
    # 计算周汇总后的ROI
    weekly_df['af_roi'] = weekly_df['total_af_revenue_d7'] / weekly_df['total_cost_value_usd']
    weekly_df['gpir_roi'] = weekly_df['total_gpir_revenue_d7'] / weekly_df['total_cost_value_usd']
    weekly_df['onlyprofit_roi'] = weekly_df['total_onlyprofit_revenue_d7'] / weekly_df['total_onlyprofit_cost']
    
    # 削弱异常值
    weekly_df['af_roi'] = weekly_df['af_roi'].clip(upper=0.2)
    weekly_df['gpir_roi'] = weekly_df['gpir_roi'].clip(upper=0.2)
    weekly_df['onlyprofit_roi'] = weekly_df['onlyprofit_roi'].clip(upper=0.2)
    
    # 处理无穷大和NaN值
    weekly_df = weekly_df.replace([np.inf, -np.inf], np.nan)
    
    print(f"Weekly aggregated data shape: {weekly_df.shape}")
    
    # 设置图表样式
    plt.style.use('default')
    
    # 为每个campaign画图
    for campaign_id in weekly_df['campaign_id'].unique():
        campaign_weekly_data = weekly_df[weekly_df['campaign_id'] == campaign_id].copy()
        campaign_weekly_data = campaign_weekly_data.sort_values('week_start')
        
        campaign_name = campaign_weekly_data['campaign_name'].iloc[0]
        print(f"\nAnalyzing Weekly Data for Campaign: {campaign_name} (ID: {campaign_id})")
        
        # 如果周数据少于4周，则跳过
        if len(campaign_weekly_data) < 4:
            print(f"Skipping campaign {campaign_id} due to insufficient weekly data (only {len(campaign_weekly_data)} weeks)")
            continue
        
        # 创建子图
        fig, axes = plt.subplots(3, 1, figsize=(15, 12), sharex=True)
        fig.suptitle(f'Weekly Campaign Analysis: {campaign_name} (ID: {campaign_id})', fontsize=16, fontweight='bold')
        
        # 第一张小图：campaign_roas_goal
        ax1 = axes[0]
        ax1.plot(campaign_weekly_data['week_start'], campaign_weekly_data['campaign_roas_goal'], 
                marker='o', linewidth=2, markersize=8, color='blue')
        ax1.set_ylabel('Campaign ROAS Goal', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        ax1.set_title('Weekly ROAS Goal')
        
        # 第二张小图：weekly cost
        ax2 = axes[1]
        ax2.plot(campaign_weekly_data['week_start'], campaign_weekly_data['total_cost_value_usd'], 
                marker='s', linewidth=2, markersize=8, color='red', label='Total Cost Value USD')
        ax2.plot(campaign_weekly_data['week_start'], campaign_weekly_data['total_onlyprofit_cost'], 
                marker='^', linewidth=2, markersize=8, color='orange', label='Total OnlyProfit Cost')
        ax2.set_ylabel('Weekly Cost ($)', fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_title('Weekly Cost Comparison')
        
        # 第三张小图：weekly ROI
        ax3 = axes[2]
        ax3.plot(campaign_weekly_data['week_start'], campaign_weekly_data['af_roi'], 
                marker='o', linewidth=2, markersize=8, color='green', label='AF ROI')
        ax3.plot(campaign_weekly_data['week_start'], campaign_weekly_data['gpir_roi'], 
                marker='s', linewidth=2, markersize=8, color='purple', label='GPIR ROI')
        ax3.plot(campaign_weekly_data['week_start'], campaign_weekly_data['onlyprofit_roi'], 
                marker='^', linewidth=2, markersize=8, color='brown', label='OnlyProfit ROI')
        
        ax3.set_ylabel('Weekly ROI', fontweight='bold')
        ax3.set_xlabel('Week Start Date', fontweight='bold')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        ax3.set_title('Weekly ROI Comparison')
        
        # 格式化x轴日期
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=max(1, len(campaign_weekly_data)//8)))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 保存图片
        plt.savefig(f'/src/data/20250718_weekly_campaign_{campaign_id}_analysis.png', dpi=300, bbox_inches='tight')
        
        # 打印一些统计信息
        print(f"\n=== Weekly Campaign {campaign_id} Statistics ===")
        print(f"Campaign Name: {campaign_name}")
        print(f"Week Range: {campaign_weekly_data['week_start'].min()} to {campaign_weekly_data['week_start'].max()}")
        print(f"Total Weeks: {len(campaign_weekly_data)}")
        print(f"Total Cost (USD): ${campaign_weekly_data['total_cost_value_usd'].sum():.2f}")
        print(f"Average Weekly ROAS Goal: {campaign_weekly_data['campaign_roas_goal'].mean():.2f}")
        print(f"Average Weekly AF ROI: {campaign_weekly_data['af_roi'].mean():.2f}")
        print(f"Average Weekly GPIR ROI: {campaign_weekly_data['gpir_roi'].mean():.2f}")
        print(f"Average Weekly OnlyProfit ROI: {campaign_weekly_data['onlyprofit_roi'].mean():.2f}")
    
    return weekly_df


# 还是按周进行汇总，但是先计算每天的ROI，然后将ROI异常的天排除掉
# 暂时的思路是：如果某天的ROI大于0.2，则认为异常
# 最总后，按照campaign name进行分组，名字中包含有D7的归类D7，有D28的归类D28，两个都没有的归类为其他
# 最终输出一个DataFrame，列：国家、分类（D7、D28、其他）、goal（枚举所有goal）、平均ROI、平均花费
def tryToAnalyze2(df):
    us_df = df[
        (df['country'] == 'US')
        & (df['install_day'] >= '20240101')
    ].copy()
    print(f"US data shape: {us_df.shape}")
    
    if us_df.empty:
        print("No US data found")
        return

    
    # 转换日期格式
    us_df['install_day'] = pd.to_datetime(us_df['install_day'], format='%Y%m%d')
    
    # 计算每天的ROI指标
    us_df['af_roi'] = us_df['af_revenue_d7_cohort'] / us_df['cost_value_usd']
    us_df['gpir_roi'] = us_df['gpir_revenue_d7_cohort'] / us_df['cost_value_usd']
    us_df['onlyprofit_roi'] = us_df['onlyprofit_revenue_d7_cohort'] / us_df['onlyprofit_cost']
    
    # 处理无穷大和NaN值
    us_df = us_df.replace([np.inf, -np.inf], np.nan)
    
    # 排除ROI异常的天（大于0.2的认为异常）
    print(f"Before filtering abnormal ROI: {len(us_df)} rows")
    us_df_filtered = us_df[
        (us_df['af_roi'] <= 0.2) & 
        (us_df['gpir_roi'] <= 0.2) & 
        (us_df['onlyprofit_roi'] <= 0.2) &
        (~us_df['af_roi'].isna()) &
        (~us_df['gpir_roi'].isna()) &
        (~us_df['onlyprofit_roi'].isna())
    ].copy()
    print(f"After filtering abnormal ROI: {len(us_df_filtered)} rows")
    
    # 添加周信息，周一为一周的开始
    us_df_filtered['week_start'] = us_df_filtered['install_day'] - pd.to_timedelta(us_df_filtered['install_day'].dt.dayofweek, unit='d')
    
    # 按照campaign和周进行分组汇总
    weekly_agg = us_df_filtered.groupby(['campaign_id', 'campaign_name', 'week_start']).agg({
        'campaign_roas_goal': 'first',  # 使用第一个值作为该周的goal
        'cost_value_usd': 'sum',
        'onlyprofit_cost': 'sum',
        'af_revenue_d7_cohort': 'sum',
        'gpir_revenue_d7_cohort': 'sum',
        'onlyprofit_revenue_d7_cohort': 'sum',
        'install_day': 'count'  # 统计该周有多少天的数据
    }).reset_index()
    
    # 重命名列
    weekly_agg = weekly_agg.rename(columns={'install_day': 'days_count'})
    
    # 只保留至少有3天数据的周
    weekly_agg = weekly_agg[weekly_agg['days_count'] >= 3].copy()
    print(f"Weekly aggregated data (>=3 days): {len(weekly_agg)} rows")
    
    # 计算周汇总后的ROI
    weekly_agg['weekly_af_roi'] = weekly_agg['af_revenue_d7_cohort'] / weekly_agg['cost_value_usd']
    weekly_agg['weekly_gpir_roi'] = weekly_agg['gpir_revenue_d7_cohort'] / weekly_agg['cost_value_usd']
    weekly_agg['weekly_onlyprofit_roi'] = weekly_agg['onlyprofit_revenue_d7_cohort'] / weekly_agg['onlyprofit_cost']
    
    # 处理无穷大和NaN值
    weekly_agg = weekly_agg.replace([np.inf, -np.inf], np.nan)
    weekly_agg = weekly_agg.dropna(subset=['weekly_af_roi', 'weekly_gpir_roi', 'weekly_onlyprofit_roi'])
    
    # 根据campaign name进行分类
    def categorize_campaign(campaign_name):
        campaign_name_lower = str(campaign_name).lower()
        if 'd7' in campaign_name_lower:
            return 'D7'
        elif 'd28' in campaign_name_lower:
            return 'D28'
        else:
            return 'Other'
    
    weekly_agg['category'] = weekly_agg['campaign_name'].apply(categorize_campaign)
    
    print(f"Campaign categories distribution:")
    print(weekly_agg['category'].value_counts())
    
    # 按照国家、分类、goal进行最终汇总
    final_result = weekly_agg.groupby(['category', 'campaign_roas_goal']).agg({
        'weekly_af_roi': 'mean',
        'weekly_gpir_roi': 'mean', 
        'weekly_onlyprofit_roi': 'mean',
        'cost_value_usd': 'mean',
        'onlyprofit_cost': 'mean',
        'campaign_id': 'nunique',  # 统计有多少个不同的campaign
        'week_start': 'count'      # 统计总共有多少周的数据
    }).reset_index()
    
    # 重命名列
    final_result = final_result.rename(columns={
        'campaign_roas_goal': 'goal',
        'weekly_af_roi': 'avg_af_roi',
        'weekly_gpir_roi': 'avg_gpir_roi',
        'weekly_onlyprofit_roi': 'avg_onlyprofit_roi',
        'cost_value_usd': 'avg_cost_usd',
        'onlyprofit_cost': 'avg_onlyprofit_cost',
        'campaign_id': 'campaign_count',
        'week_start': 'week_count'
    })
    
    # 添加国家列
    final_result['country'] = 'US'
    
    # 重新排列列的顺序
    final_result = final_result[['country', 'category', 'goal', 'avg_af_roi', 'avg_gpir_roi', 
                                'avg_onlyprofit_roi', 'avg_cost_usd', 'avg_onlyprofit_cost',
                                'campaign_count', 'week_count']]
    
    # 按照分类和goal排序
    final_result = final_result.sort_values(['category', 'goal'])
    
    print(f"\nFinal result shape: {final_result.shape}")
    print("\nFinal aggregated data:")
    print(final_result.to_string(index=False))
    
    # 保存结果到CSV
    final_result.to_csv('/src/data/20250718_campaign_analysis_summary.csv', index=False)
    print(f"\nResults saved to: /src/data/20250718_campaign_analysis_summary.csv")
    
    # 创建可视化图表
    plt.figure(figsize=(32, 10))
    
    # 子图1：不同分类和goal下的平均ROI对比
    plt.subplot(2, 2, 1)
    categories = final_result['category'].unique()
    x_pos = np.arange(len(final_result))
    
    plt.bar(x_pos - 0.2, final_result['avg_af_roi'], 0.2, label='AF ROI', alpha=0.8)
    plt.bar(x_pos, final_result['avg_gpir_roi'], 0.2, label='GPIR ROI', alpha=0.8)
    plt.bar(x_pos + 0.2, final_result['avg_onlyprofit_roi'], 0.2, label='OnlyProfit ROI', alpha=0.8)
    
    plt.xlabel('Category & Goal')
    plt.ylabel('Average ROI')
    plt.title('Average ROI by Category and Goal')
    plt.xticks(x_pos, [f"{row['category']}\n{row['goal']}" for _, row in final_result.iterrows()], rotation=45)
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 子图2：平均花费对比
    plt.subplot(2, 2, 2)
    plt.bar(x_pos - 0.2, final_result['avg_cost_usd'], 0.4, label='Cost USD', alpha=0.8, color='red')
    plt.bar(x_pos + 0.2, final_result['avg_onlyprofit_cost'], 0.4, label='OnlyProfit Cost', alpha=0.8, color='orange')
    
    plt.xlabel('Category & Goal')
    plt.ylabel('Average Cost ($)')
    plt.title('Average Cost by Category and Goal')
    plt.xticks(x_pos, [f"{row['category']}\n{row['goal']}" for _, row in final_result.iterrows()], rotation=45)
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 子图3：Campaign数量分布
    plt.subplot(2, 2, 3)
    plt.bar(x_pos, final_result['campaign_count'], alpha=0.8, color='green')
    plt.xlabel('Category & Goal')
    plt.ylabel('Campaign Count')
    plt.title('Number of Campaigns by Category and Goal')
    plt.xticks(x_pos, [f"{row['category']}\n{row['goal']}" for _, row in final_result.iterrows()], rotation=45)
    plt.grid(True, alpha=0.3)
    
    # 子图4：周数据量分布
    plt.subplot(2, 2, 4)
    plt.bar(x_pos, final_result['week_count'], alpha=0.8, color='purple')
    plt.xlabel('Category & Goal')
    plt.ylabel('Week Count')
    plt.title('Number of Weeks by Category and Goal')
    plt.xticks(x_pos, [f"{row['category']}\n{row['goal']}" for _, row in final_result.iterrows()], rotation=45)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/src/data/20250718_campaign_analysis_summary.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    return final_result    

if __name__ == "__main__":
    df = getData()
    # tryToAnalyze(df)
    # weekly_df = groupbyWeek(df)
    final_result = tryToAnalyze2(df)
    
