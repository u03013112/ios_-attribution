import os
import datetime
import numpy as np
from odps import DataFrame
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

# lastwar iOS
# 一些数据分析，主要依据大盘、模糊归因（AF)、SKA
# 分国家按照目前的国家分组

def getBiData(startDayStr, endDayStr):
    filename = f'/src/data/iOS20250729_Bi_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, skipping download.")
        return pd.read_csv(filename)
    else:
        sql = f"""
select
	'id6448786147' as app_package,
	install_day,
	mediasource,
	country_group,
	sum(cost) as cost,
	sum(installs) as installs,
	sum(revenue_h24) as revenue_h24,
	sum(revenue_h72) as revenue_h72,
	sum(revenue_h168) as revenue_h168
from
	(
		select
			install_day,
			CASE 
				WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
				WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
				ELSE mediasource
			END as mediasource,
			COALESCE(cc.country_group, 'OTHER') AS country_group,
			cost_value_usd as cost,
			installs,
			revenue_h24 as revenue_h24,
			revenue_h72 as revenue_h72,
			revenue_h168 as revenue_h168
		from
			dws_overseas_public_roi t1
			left join lw_country_group_table_by_j_20250703 cc on t1.country = cc.country
		where
			app = '502'
			and app_package in ('id6448786147', 'id6736925794')
			and facebook_segment in ('country', 'N/A')
			and install_day between '{startDayStr}'
			and '{endDayStr}'
	)
group by
	install_day,
	mediasource,
	country_group;
    """
        df = execSql(sql)
        df.to_csv(filename, index=False)
        return df
    
# 获得大盘数据，按照country_group分组
def getTotalData(df):
    totalDf = df.groupby(['install_day','country_group']).sum().reset_index()
    totalDf =  totalDf.rename(columns={
        'cost':'total_cost',
        'installs':'total_installs',
        'revenue_h24':'total_revenue_h24',
        'revenue_h72':'total_revenue_h72',
        'revenue_h168':'total_revenue_h168'
        })
    return totalDf

# 从BiData中获取花费大于0的媒体列表
# 并不是所有媒体都要加入这波分析，太小的帮助不大
def getMediaList(df):
    df = df.groupby(['mediasource']).sum().reset_index()
    df = df[df['cost'] > 0]
    return df['mediasource'].unique()

# 从BiData中获取AF数据
# 拆分媒体，获得媒体的模糊归因数据
def getAfData(df,mediaList):
    afDf = pd.DataFrame()
    for media in mediaList:
        df0 = df[df['mediasource'] == media]
        df0 = df0.groupby(['install_day','country_group']).sum().reset_index()
        df0 = df0.rename(columns={
            'cost': f'af_{media}_cost',
            'installs': f'af_{media}_installs',
            'revenue_h24': f'af_{media}_revenue_h24',
            'revenue_h72': f'af_{media}_revenue_h72',
            'revenue_h168': f'af_{media}_revenue_h168'
        })
        if afDf.empty:
            afDf = df0
        else:
            afDf = pd.merge(afDf, df0, on=['install_day', 'country_group'], how='outer')
    afDf = afDf.fillna(0)
    return afDf

# 获得SKA收入数据
def getSkaRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/iOS20250729_Ska_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, skipping download.")
        return pd.read_csv(filename)
    else:
        sql = f"""
SELECT
    REPLACE(install_date, '-', '') AS install_day,
    CASE 
        WHEN media_source = 'applovin_int' AND UPPER(ad_network_campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
        WHEN media_source = 'applovin_int' AND UPPER(ad_network_campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
        ELSE media_source
    END as mediasource,
    CASE 
        WHEN UPPER(ad_network_campaign_name) LIKE '%T1%' THEN 'T1'
        WHEN UPPER(ad_network_campaign_name) LIKE '%US%' THEN 'US'
        WHEN UPPER(ad_network_campaign_name) LIKE '%JP%' THEN 'JP'
        WHEN UPPER(ad_network_campaign_name) LIKE '%KR%' THEN 'KR'
        WHEN UPPER(ad_network_campaign_name) LIKE '%GCC%' THEN 'GCC'
        ELSE 'OTHER'
    END AS country_group,
    SUM(CASE WHEN event_name = 'af_purchase_update_skan_on' THEN skad_revenue ELSE 0 END) AS revenue_h24,
    (COUNT(CASE WHEN event_name IN ('af_skad_install', 'af_skad_redownload') THEN 1 END) - 
        COUNT(CASE WHEN event_name = 'af_purchase_update_skan_on' THEN 1 END)) AS installs
FROM
    ods_platform_appsflyer_skad_details
WHERE
    app_id in ('id6448786147', 'id6736925794')
    AND day between '{startDayStr}' and '{endDayStr}'
    AND event_name in ('af_skad_install', 'af_skad_redownload', 'af_purchase_update_skan_on')
GROUP BY
    install_day,
    CASE 
        WHEN media_source = 'applovin_int' AND UPPER(ad_network_campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
        WHEN media_source = 'applovin_int' AND UPPER(ad_network_campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
        ELSE media_source
    END,
    country_group
ORDER BY
    install_day,
    mediasource,
    country_group
;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)
        return df

#  将SKA收入数据格式修改，与AfData保持一致
def getSkaRevenueData2(getSkaRevenueDataReturn,mediaList):
    # 排除部分数据异常日期数据，暂定去除20250331~20250427，共28天
    df = getSkaRevenueDataReturn[
        (getSkaRevenueDataReturn['install_day'] < 20250331)
        | (getSkaRevenueDataReturn['install_day'] > 20250427)
    ].copy()

    skaDf = pd.DataFrame()
    for media in mediaList:
        # print(media)
        df0 = df[df['mediasource'] == media]
        df0 = df0.groupby(['install_day','country_group']).sum().reset_index()
        df0 = df0.rename(columns={
            'revenue_h24': f'ska_{media}_revenue_h24',
            'installs': f'ska_{media}_installs'
        })
        if skaDf.empty:
            skaDf = df0
        else:
            skaDf = pd.merge(skaDf, df0, on=['install_day', 'country_group'], how='outer')
    skaDf = skaDf.fillna(0)
    return skaDf

# 基础数据处理，获得初步数据
def getData(startDayStr, endDayStr):
    df = getBiData(startDayStr, endDayStr)
    
    mediaList = getMediaList(df)

    totalDf = getTotalData(df)
    afDf = getAfData(df,mediaList)

    mergedDf = pd.merge(totalDf, afDf, on=['install_day', 'country_group'], how='outer')

    skaRevenueDf = getSkaRevenueData(startDayStr, endDayStr)
    # 进行修正，媒体改名，将媒体名称与af数据保持一致
    # tiktokglobal_int -> bytedanceglobal_int
    skaRevenueDf['mediasource'] = skaRevenueDf['mediasource'].replace({
        'tiktokglobal_int': 'bytedanceglobal_int'
    })
    skaRevenueDf2 = getSkaRevenueData2(skaRevenueDf,mediaList)
    mergedDf = pd.merge(mergedDf, skaRevenueDf2, on=['install_day', 'country_group'], how='outer')
    
    return mergedDf


# 大盘与af数据进行比较,name是给这个分析起个名字，比如 分天期、分周期、分月期
def compareTotalAndAf(df, name):
    # 获取所有国家组
    country_groups = df['country_group'].unique()
    print(f"发现的国家组: {country_groups}")
    
    # 创建相关性结果汇总列表
    correlation_results = []
    
    # 为每个国家组单独分析
    for country_group in country_groups:
        print(f"\n正在处理国家组: {country_group}")
        
        # 筛选当前国家组的数据
        df_country = df[df['country_group'] == country_group].copy()
        
        if df_country.empty:
            print(f"国家组 {country_group} 没有数据，跳过")
            continue
        
        # 将af开头的列筛选出来
        af_columns = [col for col in df_country.columns if col.startswith('af_')]
        
        # 按照指标类型分组af列
        af_cost_cols = [col for col in af_columns if 'cost' in col]
        af_installs_cols = [col for col in af_columns if 'installs' in col]
        af_revenue_h24_cols = [col for col in af_columns if 'revenue_h24' in col]
        af_revenue_h72_cols = [col for col in af_columns if 'revenue_h72' in col]
        af_revenue_h168_cols = [col for col in af_columns if 'revenue_h168' in col]
        
        # 按日期分组并汇总af数据（当前国家组内）
        agg_dict = {
            'total_cost': 'sum',
            'total_installs': 'sum',
            'total_revenue_h24': 'sum',
            'total_revenue_h72': 'sum',
            'total_revenue_h168': 'sum'
        }
        # 添加af列的聚合
        for col in af_columns:
            agg_dict[col] = 'sum'
        
        df_grouped = df_country.groupby('install_day').agg(agg_dict).reset_index()
        
        # 计算af各指标的总和
        df_grouped['af_total_cost'] = df_grouped[af_cost_cols].sum(axis=1) if af_cost_cols else 0
        df_grouped['af_total_installs'] = df_grouped[af_installs_cols].sum(axis=1) if af_installs_cols else 0
        df_grouped['af_total_revenue_h24'] = df_grouped[af_revenue_h24_cols].sum(axis=1) if af_revenue_h24_cols else 0
        df_grouped['af_total_revenue_h72'] = df_grouped[af_revenue_h72_cols].sum(axis=1) if af_revenue_h72_cols else 0
        df_grouped['af_total_revenue_h168'] = df_grouped[af_revenue_h168_cols].sum(axis=1) if af_revenue_h168_cols else 0
        
        # 将install_day转换为日期
        df_grouped['date'] = pd.to_datetime(df_grouped['install_day'], format='%Y%m%d')
        df_grouped = df_grouped.sort_values('date')
        
        # 计算相关性系数的函数
        def calculate_correlation_values(x, y):
            # 过滤掉NaN值和无穷值
            mask = ~(pd.isna(x) | pd.isna(y) | np.isinf(x) | np.isinf(y))
            x_clean = x[mask]
            y_clean = y[mask]
            
            if len(x_clean) < 2:
                return None, None, None, None
            
            try:
                pearson_corr, pearson_p = pearsonr(x_clean, y_clean)
                spearman_corr, spearman_p = spearmanr(x_clean, y_clean)
                return pearson_corr, pearson_p, spearman_corr, spearman_p
            except Exception as e:
                return None, None, None, None
        
        # 创建图表
        fig, axes = plt.subplots(4, 1, figsize=(16, 20))
        fig.suptitle(f'Total vs AF Comparison - {name} - {country_group}', fontsize=16)
        
        # 指标配置
        metrics = [
            ('installs', 'total_installs', 'af_total_installs', 'Installs'),
            ('revenue_h24', 'total_revenue_h24', 'af_total_revenue_h24', 'Revenue H24'),
            ('revenue_h72', 'total_revenue_h72', 'af_total_revenue_h72', 'Revenue H72'),
            ('revenue_h168', 'total_revenue_h168', 'af_total_revenue_h168', 'Revenue H168')
        ]
        
        for i, (metric_key, total_col, af_col, title) in enumerate(metrics):
            ax = axes[i]
            
            # 绘制线图
            ax.plot(df_grouped['date'], df_grouped[total_col], 
                    label=f'Total {title}', marker='o', linewidth=2, markersize=4)
            ax.plot(df_grouped['date'], df_grouped[af_col], 
                    label=f'AF {title}', marker='s', linewidth=2, markersize=4)
            
            # 设置标题和标签
            ax.set_title(f'{title} - {country_group}', fontsize=12, fontweight='bold')
            ax.set_ylabel('Value')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化x轴日期
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            if name == 'daily':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            elif name == 'weekly':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            else:  # monthly
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # 计算相关性并添加到结果列表
            pearson_corr, pearson_p, spearman_corr, spearman_p = calculate_correlation_values(
                df_grouped[total_col], df_grouped[af_col])
            
            correlation_results.append({
                'time_period': name,
                'country_group': country_group,
                'metric': title,
                'data_points': len(df_grouped),
                'pearson_correlation': pearson_corr,
                'pearson_p_value': pearson_p,
                'spearman_correlation': spearman_corr,
                'spearman_p_value': spearman_p
            })
            
            # 在图上添加相关性信息
            if pearson_corr is not None:
                corr_text = f"{title}:\nPearson: {pearson_corr:.3f} (p={pearson_p:.3f})\nSpearman: {spearman_corr:.3f} (p={spearman_p:.3f})"
            else:
                corr_text = f"{title}:\n数据不足或计算错误"
                
            ax.text(0.02, 0.98, corr_text, transform=ax.transAxes, 
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                    fontsize=8)
        
        # 设置最后一个子图的x轴标签
        axes[-1].set_xlabel('Date')
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片，文件名包含国家组信息
        filename = f'/src/data/iOS20250729_{name}_{country_group}_totalAndAf.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        
        # 清理内存
        plt.close(fig)
    
    # 将相关性结果转换为DataFrame并保存
    correlation_df = pd.DataFrame(correlation_results)
    correlation_csv_filename = f'/src/data/iOS20250729_{name}_totalAndAf_correlations.csv'
    correlation_df.to_csv(correlation_csv_filename, index=False)
    
    print(f"\n所有国家组的分析完成！")
    print(f"相关性结果已保存到: {correlation_csv_filename}")
    
    # 打印相关性汇总
    print(f"\n=== {name} 相关性分析汇总 ===")
    print(correlation_df.to_string(index=False))
    
    return correlation_df

def aggregateByWeek(df):
    """
    按周汇总数据，使用周一作为代表日期
    """
    # 创建df的副本，避免修改原数据
    df_copy = df.copy()
    
    # 将install_day转换为日期
    df_copy['date'] = pd.to_datetime(df_copy['install_day'], format='%Y%m%d')
    
    # 计算每个日期对应的周一日期
    df_copy['week_start'] = df_copy['date'] - pd.to_timedelta(df_copy['date'].dt.dayofweek, unit='d')
    
    # 将周一日期转换回install_day格式
    df_copy['week_install_day'] = df_copy['week_start'].dt.strftime('%Y%m%d')
    
    # 获取所有数值列（只保留原始数据中的数值列）
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    # 构建聚合字典
    agg_dict = {col: 'sum' for col in numeric_columns}
    
    # 按country_group和周进行分组汇总
    weekly_df = df_copy.groupby(['week_install_day', 'country_group']).agg(agg_dict).reset_index()
    
    # 删除原有的install_day列（如果存在），然后重命名
    if 'install_day' in weekly_df.columns:
        weekly_df = weekly_df.drop('install_day', axis=1)
    
    weekly_df = weekly_df.rename(columns={'week_install_day': 'install_day'})
    
    return weekly_df

def aggregateByMonth(df):
    """
    按月汇总数据，使用月初1号作为代表日期
    """
    # 创建df的副本，避免修改原数据
    df_copy = df.copy()
    
    # 将install_day转换为日期
    df_copy['date'] = pd.to_datetime(df_copy['install_day'], format='%Y%m%d')
    
    # 计算每个日期对应的月初日期
    df_copy['month_start'] = df_copy['date'].dt.to_period('M').dt.start_time
    
    # 将月初日期转换回install_day格式
    df_copy['month_install_day'] = df_copy['month_start'].dt.strftime('%Y%m%d')
    
    # 获取所有数值列（只保留原始数据中的数值列）
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    # 构建聚合字典
    agg_dict = {col: 'sum' for col in numeric_columns}
    
    # 按country_group和月进行分组汇总
    monthly_df = df_copy.groupby(['month_install_day', 'country_group']).agg(agg_dict).reset_index()
    
    # 删除原有的install_day列（如果存在），然后重命名
    if 'install_day' in monthly_df.columns:
        monthly_df = monthly_df.drop('install_day', axis=1)
        
    monthly_df = monthly_df.rename(columns={'month_install_day': 'install_day'})
    
    return monthly_df

# 对比总数据和SKA数据
def compareTotalAndSka(df, name):

    # 获取所有国家组
    country_groups = df['country_group'].unique()
    print(f"发现的国家组: {country_groups}")
    
    # 创建相关性结果汇总列表
    correlation_results = []
    
    # 为每个国家组单独分析
    for country_group in country_groups:
        print(f"\n正在处理国家组: {country_group}")
        
        # 筛选当前国家组的数据
        df_country = df[df['country_group'] == country_group].copy()
        
        if df_country.empty:
            print(f"国家组 {country_group} 没有数据，跳过")
            continue
        
        # 将ska开头的列筛选出来
        ska_columns = [col for col in df_country.columns if col.startswith('ska_')]
        
        # 按照指标类型分组ska列
        ska_installs_cols = [col for col in ska_columns if 'installs' in col]
        ska_revenue_h24_cols = [col for col in ska_columns if 'revenue_h24' in col]
        
        # 按日期分组并汇总ska数据（当前国家组内）
        agg_dict = {
            'total_installs': 'sum',
            'total_revenue_h24': 'sum'
        }
        # 添加ska列的聚合
        for col in ska_columns:
            agg_dict[col] = 'sum'
        
        df_grouped = df_country.groupby('install_day').agg(agg_dict).reset_index()
        
        # 计算ska各指标的总和
        df_grouped['ska_total_installs'] = df_grouped[ska_installs_cols].sum(axis=1) if ska_installs_cols else 0
        df_grouped['ska_total_revenue_h24'] = df_grouped[ska_revenue_h24_cols].sum(axis=1) if ska_revenue_h24_cols else 0
        
        # 将install_day转换为日期
        df_grouped['date'] = pd.to_datetime(df_grouped['install_day'], format='%Y%m%d')
        df_grouped = df_grouped.sort_values('date')
        
        # 过滤，将之前异常处理过的日期排除掉，避免影响分析
        df_grouped = df_grouped[df_grouped['ska_total_installs']> 0].copy()

        # 计算相关性系数的函数
        def calculate_correlation_values(x, y):
            # 过滤掉NaN值和无穷值
            mask = ~(pd.isna(x) | pd.isna(y) | np.isinf(x) | np.isinf(y))
            x_clean = x[mask]
            y_clean = y[mask]
            
            if len(x_clean) < 2:
                return None, None, None, None
            
            try:
                pearson_corr, pearson_p = pearsonr(x_clean, y_clean)
                spearman_corr, spearman_p = spearmanr(x_clean, y_clean)
                return pearson_corr, pearson_p, spearman_corr, spearman_p
            except Exception as e:
                return None, None, None, None
        
        # 创建图表 - 只需要2个子图
        fig, axes = plt.subplots(2, 1, figsize=(16, 12))
        fig.suptitle(f'Total vs SKA Comparison - {name} - {country_group}', fontsize=16)
        
        # 指标配置 - 只有installs和revenue_h24
        metrics = [
            ('installs', 'total_installs', 'ska_total_installs', 'Installs'),
            ('revenue_h24', 'total_revenue_h24', 'ska_total_revenue_h24', 'Revenue H24')
        ]
        
        for i, (metric_key, total_col, ska_col, title) in enumerate(metrics):
            ax = axes[i]
            
            # 绘制线图
            ax.plot(df_grouped['date'], df_grouped[total_col], 
                    label=f'Total {title}', marker='o', linewidth=2, markersize=4)
            ax.plot(df_grouped['date'], df_grouped[ska_col], 
                    label=f'SKA {title}', marker='s', linewidth=2, markersize=4)
            
            # 设置标题和标签
            ax.set_title(f'{title} - {country_group}', fontsize=12, fontweight='bold')
            ax.set_ylabel('Value')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化x轴日期
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            if name == 'daily':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            elif name == 'weekly':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            else:  # monthly
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # 计算相关性并添加到结果列表
            pearson_corr, pearson_p, spearman_corr, spearman_p = calculate_correlation_values(
                df_grouped[total_col], df_grouped[ska_col])
            
            correlation_results.append({
                'time_period': name,
                'country_group': country_group,
                'metric': title,
                'data_points': len(df_grouped),
                'pearson_correlation': pearson_corr,
                'pearson_p_value': pearson_p,
                'spearman_correlation': spearman_corr,
                'spearman_p_value': spearman_p
            })
            
            # 在图上添加相关性信息
            if pearson_corr is not None:
                corr_text = f"{title}:\nPearson: {pearson_corr:.3f} (p={pearson_p:.3f})\nSpearman: {spearman_corr:.3f} (p={spearman_p:.3f})"
            else:
                corr_text = f"{title}:\n数据不足或计算错误"
                
            ax.text(0.02, 0.98, corr_text, transform=ax.transAxes, 
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                    fontsize=8)
        
        # 设置最后一个子图的x轴标签
        axes[-1].set_xlabel('Date')
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片，文件名包含国家组信息
        filename = f'/src/data/iOS20250729_{name}_{country_group}_totalAndSka.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        
        # 清理内存
        plt.close(fig)
    
    # 将相关性结果转换为DataFrame并保存
    correlation_df = pd.DataFrame(correlation_results)
    correlation_csv_filename = f'/src/data/iOS20250729_{name}_totalAndSka_correlations.csv'
    correlation_df.to_csv(correlation_csv_filename, index=False)
    
    print(f"\n所有国家组的分析完成！")
    print(f"相关性结果已保存到: {correlation_csv_filename}")
    
    # 打印相关性汇总
    print(f"\n=== {name} SKA相关性分析汇总 ===")
    print(correlation_df.to_string(index=False))
    
    return correlation_df

def compareAfAndSka(df, name):
    # 抑制常数输入警告
    import warnings
    from scipy.stats import SpearmanRConstantInputWarning
    warnings.filterwarnings("ignore", category=SpearmanRConstantInputWarning)
    
    # 获取所有国家组和媒体
    country_groups = ['T1', 'US', 'JP', 'KR', 'GCC', 'OTHER']  # 固定的6个国家组
    
    # 获取所有媒体列表（只从af_*_installs列中提取）
    af_installs_columns = [col for col in df.columns if col.startswith('af_') and col.endswith('_installs')]
    media_list = set()
    for col in af_installs_columns:
        # 从列名中提取媒体名，格式如 af_applovin_int_d7_installs -> applovin_int_d7
        media = col.replace('af_', '').replace('_installs', '')
        media_list.add(media)
    
    media_list = sorted(list(media_list))
    print(f"发现的媒体: {media_list}")
    
    # 为每个媒体单独分析
    for media in media_list:
        print(f"\n正在处理媒体: {media}")
        
        # 创建相关性结果汇总列表（当前媒体）
        correlation_results = []
        
        # 检查当前媒体是否有对应的af和ska数据
        af_media_installs_col = f'af_{media}_installs'
        af_media_revenue_col = f'af_{media}_revenue_h24'
        ska_media_installs_col = f'ska_{media}_installs'
        ska_media_revenue_col = f'ska_{media}_revenue_h24'
        
        # 检查必要的列是否存在
        required_cols = [af_media_installs_col, af_media_revenue_col, ska_media_installs_col, ska_media_revenue_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"媒体 {media} 缺少必要的列: {missing_cols}，跳过")
            continue
        
        # 创建图表 - 6列2行
        fig, axes = plt.subplots(2, 6, figsize=(24, 8))
        fig.suptitle(f'AF vs SKA Comparison - {name} - {media}', fontsize=16)
        
        # 指标配置
        metrics = [
            ('installs', af_media_installs_col, ska_media_installs_col, 'Installs'),
            ('revenue_h24', af_media_revenue_col, ska_media_revenue_col, 'Revenue H24')
        ]
        
        # 计算相关性系数的函数
        def calculate_correlation_values(x, y):
            # 过滤掉NaN值和无穷值
            mask = ~(pd.isna(x) | pd.isna(y) | np.isinf(x) | np.isinf(y))
            x_clean = x[mask]
            y_clean = y[mask]
            
            if len(x_clean) < 2:
                return None, None, None, None
            
            # 检查是否为常数数组
            if len(set(x_clean)) <= 1 or len(set(y_clean)) <= 1:
                return None, None, None, None
            
            try:
                pearson_corr, pearson_p = pearsonr(x_clean, y_clean)
                spearman_corr, spearman_p = spearmanr(x_clean, y_clean)
                return pearson_corr, pearson_p, spearman_corr, spearman_p
            except Exception as e:
                return None, None, None, None
        
        # 遍历每个国家组和指标
        for country_idx, country_group in enumerate(country_groups):
            # 筛选当前国家组的数据
            df_country = df[df['country_group'] == country_group].copy()
            
            if df_country.empty:
                print(f"国家组 {country_group} 没有数据，跳过")
                # 为空数据创建空白图
                for metric_idx in range(2):
                    ax = axes[metric_idx, country_idx]
                    ax.text(0.5, 0.5, f'No data\n{country_group}', 
                           ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f'{metrics[metric_idx][3]} - {country_group}', fontsize=10)
                continue
            
            # 按日期分组并汇总数据（当前国家组内）
            agg_dict = {
                af_media_installs_col: 'sum',
                af_media_revenue_col: 'sum',
                ska_media_installs_col: 'sum',
                ska_media_revenue_col: 'sum'
            }
            
            df_grouped = df_country.groupby('install_day').agg(agg_dict).reset_index()
            
            # 将install_day转换为日期
            df_grouped['date'] = pd.to_datetime(df_grouped['install_day'], format='%Y%m%d')
            df_grouped = df_grouped.sort_values('date')
            
            # 过滤异常数据 - 排除SKA数据为0的异常情况
            df_grouped = df_grouped[
                (df_grouped[ska_media_installs_col] > 0) | 
                (df_grouped[ska_media_revenue_col] > 0)
            ].copy()
            
            # 为每个指标绘图
            for metric_idx, (metric_key, af_col, ska_col, title) in enumerate(metrics):
                ax = axes[metric_idx, country_idx]
                
                if len(df_grouped) == 0:
                    ax.text(0.5, 0.5, f'No valid data\n{country_group}', 
                           ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f'{title} - {country_group}', fontsize=10)
                    
                    # 添加空的相关性记录
                    correlation_results.append({
                        'time_period': name,
                        'media': media,
                        'country_group': country_group,
                        'metric': title,
                        'data_points': 0,
                        'pearson_correlation': None,
                        'pearson_p_value': None,
                        'spearman_correlation': None,
                        'spearman_p_value': None
                    })
                    continue
                
                # 绘制线图
                ax.plot(df_grouped['date'], df_grouped[af_col], 
                        label=f'AF {title}', marker='o', linewidth=1.5, markersize=3)
                ax.plot(df_grouped['date'], df_grouped[ska_col], 
                        label=f'SKA {title}', marker='s', linewidth=1.5, markersize=3)
                
                # 设置标题和标签
                ax.set_title(f'{title} - {country_group}', fontsize=10, fontweight='bold')
                ax.set_ylabel('Value', fontsize=8)
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)
                
                # 格式化x轴日期
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                if name == 'daily':
                    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
                elif name == 'weekly':
                    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
                else:  # monthly
                    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
                
                # 计算相关性并添加到结果列表
                pearson_corr, pearson_p, spearman_corr, spearman_p = calculate_correlation_values(
                    df_grouped[af_col], df_grouped[ska_col])
                
                correlation_results.append({
                    'time_period': name,
                    'media': media,
                    'country_group': country_group,
                    'metric': title,
                    'data_points': len(df_grouped),
                    'pearson_correlation': pearson_corr,
                    'pearson_p_value': pearson_p,
                    'spearman_correlation': spearman_corr,
                    'spearman_p_value': spearman_p
                })
                
                # 在图上添加相关性信息
                if pearson_corr is not None:
                    corr_text = f"P: {pearson_corr:.2f}\nS: {spearman_corr:.2f}"
                else:
                    corr_text = "No corr"
                    
                ax.text(0.02, 0.98, corr_text, transform=ax.transAxes, 
                        verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                        fontsize=7)
        
        # 设置底部子图的x轴标签
        for country_idx in range(6):
            axes[1, country_idx].set_xlabel('Date', fontsize=8)
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片，文件名包含媒体信息
        filename = f'/src/data/iOS20250729_{name}_{media}_afAndSka.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        
        # 清理内存
        plt.close(fig)
        
        # 将当前媒体的相关性结果转换为DataFrame并保存
        if correlation_results:
            correlation_df = pd.DataFrame(correlation_results)
            correlation_csv_filename = f'/src/data/iOS20250729_{name}_{media}_afAndSka_correlations.csv'
            correlation_df.to_csv(correlation_csv_filename, index=False)
            print(f"媒体 {media} 相关性结果已保存到: {correlation_csv_filename}")
        
    print(f"\n所有媒体的AF vs SKA分析完成！")

def compareAfAndSka2(df, name):
    # 抑制常数输入警告
    import warnings
    from scipy.stats import SpearmanRConstantInputWarning
    warnings.filterwarnings("ignore", category=SpearmanRConstantInputWarning)
    
    # 获取所有媒体列表（只从af_*_installs列中提取）
    af_installs_columns = [col for col in df.columns if col.startswith('af_') and col.endswith('_installs')]
    media_list = set()
    for col in af_installs_columns:
        # 从列名中提取媒体名，格式如 af_applovin_int_d7_installs -> applovin_int_d7
        media = col.replace('af_', '').replace('_installs', '')
        media_list.add(media)
    
    media_list = sorted(list(media_list))
    print(f"发现的媒体: {media_list}")
    
    # 创建相关性结果汇总列表（所有媒体）
    correlation_results = []
    
    # 为每个媒体单独分析
    for media in media_list:
        print(f"\n正在处理媒体: {media}")
        
        # 检查当前媒体是否有对应的af和ska数据
        af_media_installs_col = f'af_{media}_installs'
        af_media_revenue_col = f'af_{media}_revenue_h24'
        ska_media_installs_col = f'ska_{media}_installs'
        ska_media_revenue_col = f'ska_{media}_revenue_h24'
        
        # 检查必要的列是否存在
        required_cols = [af_media_installs_col, af_media_revenue_col, ska_media_installs_col, ska_media_revenue_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"媒体 {media} 缺少必要的列: {missing_cols}，跳过")
            continue
        
        # 按日期分组并汇总数据（不区分国家，直接汇总所有国家）
        agg_dict = {
            af_media_installs_col: 'sum',
            af_media_revenue_col: 'sum',
            ska_media_installs_col: 'sum',
            ska_media_revenue_col: 'sum'
        }
        
        df_grouped = df.groupby('install_day').agg(agg_dict).reset_index()
        
        # 将install_day转换为日期
        df_grouped['date'] = pd.to_datetime(df_grouped['install_day'], format='%Y%m%d')
        df_grouped = df_grouped.sort_values('date')
        
        # 过滤异常数据 - 排除SKA数据为0的异常情况
        df_grouped = df_grouped[
            (df_grouped[ska_media_installs_col] > 0) | 
            (df_grouped[ska_media_revenue_col] > 0)
        ].copy()
        
        if len(df_grouped) == 0:
            print(f"媒体 {media} 没有有效数据，跳过")
            continue
        
        # 创建图表 - 1列2行（只有2个指标）
        fig, axes = plt.subplots(2, 1, figsize=(16, 12))
        fig.suptitle(f'AF vs SKA Comparison (All Countries) - {name} - {media}', fontsize=16)
        
        # 指标配置
        metrics = [
            ('installs', af_media_installs_col, ska_media_installs_col, 'Installs'),
            ('revenue_h24', af_media_revenue_col, ska_media_revenue_col, 'Revenue H24')
        ]
        
        # 计算相关性系数的函数
        def calculate_correlation_values(x, y):
            # 过滤掉NaN值和无穷值
            mask = ~(pd.isna(x) | pd.isna(y) | np.isinf(x) | np.isinf(y))
            x_clean = x[mask]
            y_clean = y[mask]
            
            if len(x_clean) < 2:
                return None, None, None, None
            
            # 检查是否为常数数组
            if len(set(x_clean)) <= 1 or len(set(y_clean)) <= 1:
                return None, None, None, None
            
            try:
                pearson_corr, pearson_p = pearsonr(x_clean, y_clean)
                spearman_corr, spearman_p = spearmanr(x_clean, y_clean)
                return pearson_corr, pearson_p, spearman_corr, spearman_p
            except Exception as e:
                return None, None, None, None
        
        # 为每个指标绘图
        for metric_idx, (metric_key, af_col, ska_col, title) in enumerate(metrics):
            ax = axes[metric_idx]
            
            # 绘制线图
            ax.plot(df_grouped['date'], df_grouped[af_col], 
                    label=f'AF {title}', marker='o', linewidth=2, markersize=4)
            ax.plot(df_grouped['date'], df_grouped[ska_col], 
                    label=f'SKA {title}', marker='s', linewidth=2, markersize=4)
            
            # 设置标题和标签
            ax.set_title(f'{title} - {media} (All Countries)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Value')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化x轴日期
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            if name == 'daily':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            elif name == 'weekly':
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            else:  # monthly
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # 计算相关性并添加到结果列表
            pearson_corr, pearson_p, spearman_corr, spearman_p = calculate_correlation_values(
                df_grouped[af_col], df_grouped[ska_col])
            
            correlation_results.append({
                'time_period': name,
                'media': media,
                'country_group': 'ALL',  # 标记为所有国家
                'metric': title,
                'data_points': len(df_grouped),
                'pearson_correlation': pearson_corr,
                'pearson_p_value': pearson_p,
                'spearman_correlation': spearman_corr,
                'spearman_p_value': spearman_p
            })
            
            # 在图上添加相关性信息
            if pearson_corr is not None:
                corr_text = f"{title}:\nPearson: {pearson_corr:.3f} (p={pearson_p:.3f})\nSpearman: {spearman_corr:.3f} (p={spearman_p:.3f})"
            else:
                corr_text = f"{title}:\n数据不足或计算错误"
                
            ax.text(0.02, 0.98, corr_text, transform=ax.transAxes, 
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                    fontsize=8)
        
        # 设置最后一个子图的x轴标签
        axes[-1].set_xlabel('Date')
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片，文件名包含媒体信息
        filename = f'/src/data/iOS20250729_{name}_{media}_afAndSka_allCountries.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        
        # 清理内存
        plt.close(fig)
        
        print(f"媒体 {media} 分析完成")
    
    # 将所有媒体的相关性结果转换为DataFrame并保存
    if correlation_results:
        correlation_df = pd.DataFrame(correlation_results)
        correlation_csv_filename = f'/src/data/iOS20250729_{name}_afAndSka_allCountries_correlations.csv'
        correlation_df.to_csv(correlation_csv_filename, index=False)
        print(f"\n所有媒体相关性结果已保存到: {correlation_csv_filename}")
        
        # 打印相关性汇总
        print(f"\n=== {name} AF vs SKA (All Countries) 相关性分析汇总 ===")
        print(correlation_df.to_string(index=False))
    
    print(f"\n所有媒体的AF vs SKA (All Countries) 分析完成！")
    
    return correlation_df if correlation_results else None


def main():
    startDayStr = '20240729'
    endDayStr = '20250729'
    df = getData(startDayStr, endDayStr)
    df.to_csv(f'/src/data/iOS20250729_merged_{startDayStr}_{endDayStr}.csv', index=False)

    # compareTotalAndAf(df, 'daily')
    # compareTotalAndSka(df, 'daily')
    # compareAfAndSka(df, 'daily')
    compareAfAndSka2(df, 'daily')
    
    # 周度汇总和分析
    weekly_df = aggregateByWeek(df)
    weekly_df.to_csv(f'/src/data/iOS20250729_weekly_{startDayStr}_{endDayStr}.csv', index=False)
    # compareTotalAndAf(weekly_df, 'weekly')
    # compareTotalAndSka(weekly_df, 'weekly')
    # compareAfAndSka(weekly_df, 'weekly')
    compareAfAndSka2(weekly_df, 'weekly')
    
    # 月度汇总和分析
    monthly_df = aggregateByMonth(df)
    monthly_df.to_csv(f'/src/data/iOS20250729_monthly_{startDayStr}_{endDayStr}.csv', index=False)
    # compareTotalAndAf(monthly_df, 'monthly')
    # compareTotalAndSka(monthly_df, 'monthly')
    # compareAfAndSka(monthly_df, 'monthly')
    compareAfAndSka2(monthly_df, 'monthly')

if __name__ == '__main__':
    main()