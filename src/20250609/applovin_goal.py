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

if __name__ == "__main__":
    df = getData()
    tryToAnalyze(df)
    
