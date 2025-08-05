import os
import datetime
import numpy as np
from odps import DataFrame
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymc as pm
import arviz as az

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

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
    
def getTotalData(df):
    totalDf = df.groupby(['install_day','country_group']).agg({
        'cost': 'sum',
        'installs': 'sum',
        'revenue_h24': 'sum',
        'revenue_h72': 'sum',
        'revenue_h168': 'sum'
    }).reset_index()
    totalDf =  totalDf.rename(columns={
        'cost':'total_cost',
        'installs':'total_installs',
        'revenue_h24':'total_revenue_h24',
        'revenue_h72':'total_revenue_h72',
        'revenue_h168':'total_revenue_h168'
        })
    return totalDf


def getMediaList(df):
    df = df.groupby(['mediasource']).sum().reset_index()
    df = df[df['cost'] > 0]
    return df['mediasource'].unique()

# 从BiData中获取AF数据
# 拆分媒体，获得媒体的模糊归因数据
def getAfData(df, mediaList):
    afDf = pd.DataFrame()
    # 定义要聚合的数值列
    numeric_cols = ['cost', 'installs', 'revenue_h24', 'revenue_h72', 'revenue_h168']
    
    for media in mediaList:
        df0 = df[df['mediasource'] == media]
        # 只对数值列进行聚合
        df0 = df0.groupby(['install_day','country_group'])[numeric_cols].sum().reset_index()
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

# 由于google的花费比预想的要高（202501~202507，花费超过10%），google又没有模糊归因，google的收入很大幅度的影响大盘收入。所以需要对AF数据进行修正
# 将afDf中的media == 'googleadwords_int' 的数据进行修正
# 简单估计google的收入 = 所有有花费媒体收入 * google的花费 / 所有有花费媒体的花费
# 其中有花费媒体可以通过getMediaList(df)获取
# 最后将修正后的数值，直接赋值给afDf['af_googleadwords_int_revenue_h168']
def fixAfDataForGoogle(afDf, mediaList):
    """
    修正AF数据中Google的收入数据
    由于Google没有模糊归因，但花费很高，需要估算其收入
    估算方法：Google收入 = 付费媒体收入(排除Google) * Google花费 / 付费媒体花费(排除Google)
    """
    # 检查是否存在Google相关列
    google_cost_col = 'af_googleadwords_int_cost'
    google_revenue_col = 'af_googleadwords_int_revenue_h168'
    
    if google_cost_col not in afDf.columns:
        print("警告: 未找到Google花费数据列，跳过修正")
        return afDf
    
    # 从mediaList中获取付费媒体列表，排除Google
    if len(mediaList) == 0:
        print("警告: mediaList为空，跳过修正")
        return afDf
    
    # 构建付费媒体的花费和收入列名（都排除Google）
    cost_columns = []
    revenue_columns = []
    
    for media in mediaList:
        # 排除Google媒体
        if media != 'googleadwords_int':
            cost_col = f'af_{media}_cost'
            revenue_col = f'af_{media}_revenue_h168'
            
            # 花费列（排除Google）
            if cost_col in afDf.columns:
                cost_columns.append(cost_col)
            
            # 收入列（排除Google）
            if revenue_col in afDf.columns:
                revenue_columns.append(revenue_col)
    
    if not cost_columns or not revenue_columns:
        print("警告: 未找到付费媒体（排除Google）的花费或收入数据，跳过修正")
        return afDf
    
    # 使用向量化操作计算Google的估算收入
    # 计算付费媒体总花费（排除Google）
    afDf['temp_total_cost'] = afDf[cost_columns].sum(axis=1)
    
    # 计算付费媒体总收入（排除Google）
    afDf['temp_total_revenue'] = afDf[revenue_columns].sum(axis=1)
    
    # 获取Google的花费
    google_cost = afDf[google_cost_col].fillna(0)
    
    # 计算Google估算收入：只有当其他媒体有花费且有收入，且Google有花费时才计算
    mask = (afDf['temp_total_cost'] > 0) & (afDf['temp_total_revenue'] > 0) & (google_cost > 0)
    
    # 创建Google收入列（如果不存在）
    if google_revenue_col not in afDf.columns:
        afDf[google_revenue_col] = 0
    
    # 使用向量化操作计算估算收入
    afDf.loc[mask, google_revenue_col] = (
        afDf.loc[mask, 'temp_total_revenue'] * 
        afDf.loc[mask, google_cost_col] / 
        afDf.loc[mask, 'temp_total_cost']
    )
    
    # 对于无法估算的情况，设置为0
    afDf.loc[~mask, google_revenue_col] = 0
    
    # 删除临时列
    afDf.drop(['temp_total_cost', 'temp_total_revenue'], axis=1, inplace=True)
    
    print(f"已完成Google收入数据修正，修正列: {google_revenue_col}")
    print(f"使用的付费媒体列表（排除Google）: {[media for media in mediaList if media != 'googleadwords_int']}")
    print(f"收入计算列（排除Google）: {revenue_columns}")
    print(f"花费计算列（排除Google）: {cost_columns}")
    
    # 输出修正后的统计信息
    total_google_cost = afDf[google_cost_col].sum()
    total_google_revenue = afDf[google_revenue_col].sum()
    if total_google_cost > 0:
        estimated_roi = total_google_revenue / total_google_cost
        print(f"Google修正后统计: 总花费=${total_google_cost:,.2f}, 估算总收入=${total_google_revenue:,.2f}, 估算ROI={estimated_roi:.2%}")
    
    return afDf

def getData(startDayStr, endDayStr):
    df = getBiData(startDayStr, endDayStr)
    
    mediaList = getMediaList(df)
    print(f"获取到的媒体列表: {mediaList}")
    totalDf = getTotalData(df)
    afDf = getAfData(df,mediaList)
    
    # 修正Google的收入数据
    afDf = fixAfDataForGoogle(afDf, mediaList)

    mergedDf = pd.merge(totalDf, afDf, on=['install_day', 'country_group'], how='outer')
    mergedDf['install_day'] = pd.to_datetime(mergedDf['install_day'].astype(str), format='%Y%m%d')

    return mergedDf

def calculate_media_attribution(df, country):
    """计算指定国家各媒体的收入占比"""
    countryDf = df[df['country_group'] == country]
    
    if countryDf.empty:
        print(f"警告: 国家 {country} 没有数据")
        return
    
    # 获取总收入
    totalRevenue = countryDf['total_revenue_h168'].sum()
    
    if totalRevenue == 0:
        print(f"警告: 国家 {country} 总收入为0")
        return
    
    # 动态获取所有媒体收入列
    mediaColumns = [col for col in countryDf.columns if col.startswith('af_') and col.endswith('_revenue_h168')]
    
    # 计算各媒体收入占比
    mediaAttributions = []
    totalMediaRevenue = 0
    
    for col in mediaColumns:
        mediaRevenue = countryDf[col].sum()
        if mediaRevenue > 0:  # 只显示有收入的媒体
            mediaName = col.replace('af_', '').replace('_revenue_h168', '')
            percentage = mediaRevenue / totalRevenue
            mediaAttributions.append({
                'media': mediaName,
                'revenue': mediaRevenue,
                'percentage': percentage
            })
            totalMediaRevenue += mediaRevenue
    
    # 计算自然量占比
    organicRevenue = totalRevenue - totalMediaRevenue
    organicPercentage = organicRevenue / totalRevenue
    
    # 按占比排序
    mediaAttributions.sort(key=lambda x: x['percentage'], reverse=True)
    
    # 输出结果
    print(f"\n=== 国家: {country} ===")
    print(f"总收入: ${totalRevenue:,.2f}")
    print(f"自然量收入: ${organicRevenue:,.2f} ({organicPercentage:.2%})")
    print("\n媒体归因收入占比:")
    
    for attribution in mediaAttributions:
        print(f"  {attribution['media']}: ${attribution['revenue']:,.2f} ({attribution['percentage']:.2%})")
    
    print("-" * 50)


def bayesian_fit_media_coefficients(df, country, mediaList,mediaOtherList):
    """使用贝叶斯方法拟合媒体系数"""
    countryDf = df[df['country_group'] == country].copy()
    
    if countryDf.empty:
        print(f"警告: 国家 {country} 没有数据")
        return None
    
    # 计算总收入均值，用于设置自然量先验
    totalRevenueMean = countryDf['total_revenue_h168'].mean()
    
    organicRevenueConfigList = [
        {'ratio': 0.1, 'mu': totalRevenueMean * 0.1, 'sigma': totalRevenueMean * 0.008},
        {'ratio': 0.2, 'mu': totalRevenueMean * 0.2, 'sigma': totalRevenueMean * 0.008},
        {'ratio': 0.3, 'mu': totalRevenueMean * 0.3, 'sigma': totalRevenueMean * 0.008}
    ]
    
    resultDf = pd.DataFrame()
    
    for organicConfig in organicRevenueConfigList:
        print(f"\n=== 拟合 {country} - 自然量占比 {organicConfig['ratio']:.0%} ===")
        
        with pm.Model() as model:
            # 自然量先验
            organicRevenue = pm.Normal('organicRevenue', 
                                     mu=organicConfig['mu'], 
                                     sigma=organicConfig['sigma'])
            
            # 为每个媒体设置系数先验 (0.8~1.2, 均值1.0)
            mediaCoeffs = {}
            mediaTrueRevenues = {}
            
            for media in mediaList:
                # 媒体系数
                coeffName = f'{media}_coeff'
                mediaCoeffs[media] = pm.Normal(coeffName, mu=1.0, sigma=0.1)
                
                # 媒体真实收入 = 模糊归因收入 * 系数
                afRevenueCol = f'af_{media}_revenue_h168'
                if afRevenueCol in countryDf.columns:
                    mediaTrueRevenues[media] = mediaCoeffs[media] * countryDf[afRevenueCol]
                else:
                    print(f"警告: 找不到列 {afRevenueCol}")
                    mediaTrueRevenues[media] = 0
            
            otherMediaRevenues = {}
            # 其他媒体的真实收入（如果有）
            for media in mediaOtherList:
                afRevenueCol = f'af_{media}_revenue_h168'
                if afRevenueCol in countryDf.columns:
                    otherMediaRevenues[media] = countryDf[afRevenueCol]
                else:
                    print(f"警告: 找不到列 {afRevenueCol}")
                    otherMediaRevenues[media] = 0

            # 计算预测的总收入
            predictedTotalRevenue = organicRevenue + sum(mediaTrueRevenues.values()) + sum(otherMediaRevenues.values())
            
            # 观测节点：总收入
            total_revenue_obs = pm.Normal(
                'total_revenue_obs',
                mu=predictedTotalRevenue,
                sigma=totalRevenueMean * 0.1,  # 设置为均值的10%作为噪声
                observed=countryDf['total_revenue_h168']
            )
            
            # 采样
            trace = pm.sample(1000, tune=1000, target_accept=0.95, random_seed=42)
            
            # 输出结果摘要
            summary = pm.summary(trace, hdi_prob=0.95)
            print(summary)
            
            # 计算拟合质量
            posterior_pred = pm.sample_posterior_predictive(trace, model=model)
            predicted_values = posterior_pred.posterior_predictive['total_revenue_obs'].mean(dim=['chain', 'draw'])
            actual_values = countryDf['total_revenue_h168']
            
            # 计算MAPE
            mape = np.mean(np.abs((actual_values - predicted_values) / actual_values)) * 100
            
            # 提取系数均值
            coeffResults = {}
            for media in mediaList:
                coeffName = f'{media}_coeff'
                if coeffName in summary.index:
                    coeffResults[media] = {
                        'coeff_mean': summary.loc[coeffName, 'mean'],
                        'coeff_hdi_lower': summary.loc[coeffName, 'hdi_2.5%'],
                        'coeff_hdi_upper': summary.loc[coeffName, 'hdi_97.5%']
                    }
            
            # 计算修正后的ROI
            modifiedROIs = calculate_modified_rois(countryDf, mediaList, coeffResults)
            
            # 计算真实平均ROI
            trueROIs = calculate_true_average_rois(countryDf, mediaList)
            
            # 计算预估自然量的实际占比
            organicRevenueMean = summary.loc['organicRevenue', 'mean']
            actual_organic_ratio = organicRevenueMean / totalRevenueMean if totalRevenueMean > 0 else 0
            
            # 保存结果
            result = {
                'country': country,
                'organic_ratio': organicConfig['ratio'],
                'organic_revenue_mean': organicRevenueMean,
                'organic_revenue_hdi_lower': summary.loc['organicRevenue', 'hdi_2.5%'],
                'organic_revenue_hdi_upper': summary.loc['organicRevenue', 'hdi_97.5%'],
                'actual_organic_ratio': actual_organic_ratio,
                'mape': mape,
                **{f'{media}_coeff': coeffResults.get(media, {}).get('coeff_mean', 1.0) for media in mediaList},
                **{f'{media}_modified_roi': modifiedROIs.get(media, 0) for media in mediaList},
                **{f'{media}_true_roi': trueROIs.get(media, 0) for media in mediaList}
            }
            
            resultDf = pd.concat([resultDf, pd.DataFrame([result])], ignore_index=True)
    
    return resultDf

def calculate_modified_rois(countryDf, mediaList, coeffResults):
    """计算修正后的ROI"""
    modifiedROIs = {}
    
    for media in mediaList:
        afRevenueCol = f'af_{media}_revenue_h168'
        afCostCol = f'af_{media}_cost'
        
        if afRevenueCol in countryDf.columns and afCostCol in countryDf.columns:
            totalRevenue = countryDf[afRevenueCol].sum()
            totalCost = countryDf[afCostCol].sum()
            
            if totalCost > 0:
                coeff = coeffResults.get(media, {}).get('coeff_mean', 1.0)
                modifiedROI = (totalRevenue * coeff) / totalCost
                modifiedROIs[media] = modifiedROI
            else:
                modifiedROIs[media] = 0
        else:
            modifiedROIs[media] = 0
    
    return modifiedROIs

def calculate_true_average_rois(countryDf, mediaList):
    """计算真实平均ROI（未修正的原始ROI）"""
    trueROIs = {}
    
    for media in mediaList:
        afRevenueCol = f'af_{media}_revenue_h168'
        afCostCol = f'af_{media}_cost'
        
        if afRevenueCol in countryDf.columns and afCostCol in countryDf.columns:
            totalRevenue = countryDf[afRevenueCol].sum()
            totalCost = countryDf[afCostCol].sum()
            
            if totalCost > 0:
                trueROI = totalRevenue / totalCost
                trueROIs[media] = trueROI
            else:
                trueROIs[media] = 0
        else:
            trueROIs[media] = 0
    
    return trueROIs

def analyze_results(resultDf, country):
    """分析拟合结果"""
    print(f"\n=== {country} 拟合结果分析 ===")
    
    # 按MAPE排序
    resultDf_sorted = resultDf.sort_values('mape')
    
    print("各方案MAPE对比:")
    for _, row in resultDf_sorted.iterrows():
        print(f"自然量占比 {row['organic_ratio']:.0%}: MAPE = {row['mape']:.2f}%, 实际自然量占比 = {row['actual_organic_ratio']:.2%}")
    
    # 显示最佳方案的详细结果
    bestResult = resultDf_sorted.iloc[0]
    print(f"\n最佳方案 (预设自然量占比 {bestResult['organic_ratio']:.0%}):")
    print(f"自然量收入: ${bestResult['organic_revenue_mean']:,.2f}")
    print(f"实际自然量占比: {bestResult['actual_organic_ratio']:.2%}")
    
    mediaList = ['applovin_int_d7','Facebook Ads','moloco_int','applovin_int_d28','bytedanceglobal_int']
    print("\n媒体系数和ROI:")
    for media in mediaList:
        coeff = bestResult.get(f'{media}_coeff', 1.0)
        modified_roi = bestResult.get(f'{media}_modified_roi', 0)
        true_roi = bestResult.get(f'{media}_true_roi', 0)
        print(f"  {media}: 系数={coeff:.3f}, 真实ROI={true_roi:.2%}, 修正ROI={modified_roi:.2%}")
    
    return bestResult

def validate_model_by_period(df, country, bestResult, mediaList, mediaOtherList):
    """使用最佳拟合结果对不同时间周期进行验算"""
    countryDf = df[df['country_group'] == country].copy()
    
    if countryDf.empty:
        print(f"警告: 国家 {country} 没有数据")
        return
    
    # 提取最佳结果的参数
    organicRevenueMean = bestResult['organic_revenue_mean']
    mediaCoeffs = {}
    for media in mediaList:
        mediaCoeffs[media] = bestResult.get(f'{media}_coeff', 1.0)
    
    # 添加周和月的时间列
    countryDf['year_week'] = countryDf['install_day'].dt.strftime('%Y-W%U')
    countryDf['year_month'] = countryDf['install_day'].dt.strftime('%Y-%m')
    
    # 按周汇总验算
    print(f"\n=== {country} 按周汇总验算 ===")
    weekly_mape = calculate_period_mape(countryDf, 'year_week', organicRevenueMean, mediaCoeffs, mediaList, mediaOtherList)
    print(f"按周汇总的MAPE: {weekly_mape:.2f}%")
    
    # 按月汇总验算
    print(f"\n=== {country} 按月汇总验算 ===")
    monthly_mape = calculate_period_mape(countryDf, 'year_month', organicRevenueMean, mediaCoeffs, mediaList, mediaOtherList)
    print(f"按月汇总的MAPE: {monthly_mape:.2f}%")
    
    return weekly_mape, monthly_mape

def calculate_period_mape(countryDf, period_col, organicRevenueMean, mediaCoeffs, mediaList, mediaOtherList):
    """计算指定时间周期的MAPE"""
    # 构建所有需要汇总的媒体列（主要媒体 + 其他媒体）
    all_media_columns = {}
    
    # 主要媒体列
    for media in mediaList:
        all_media_columns[f'af_{media}_revenue_h168'] = 'sum'
    
    # 其他媒体列
    for media in mediaOtherList:
        all_media_columns[f'af_{media}_revenue_h168'] = 'sum'
    
    # 按时间周期汇总数据
    period_df = countryDf.groupby(period_col).agg({
        'total_revenue_h168': 'sum',
        **all_media_columns
    }).reset_index()
    
    # 计算每个周期的天数（用于计算自然量）
    period_days = countryDf.groupby(period_col).size().reset_index(name='days')
    period_df = pd.merge(period_df, period_days, on=period_col)
    
    # 计算预测值
    predicted_revenues = []
    actual_revenues = []
    
    for _, row in period_df.iterrows():
        # 预测的自然量收入 = 每日自然量均值 * 该周期的天数
        predicted_organic = organicRevenueMean * row['days']
        
        # 预测的主要媒体收入 = 模糊归因收入 * 系数
        predicted_main_media_total = 0
        for media in mediaList:
            af_revenue_col = f'af_{media}_revenue_h168'
            if af_revenue_col in row:
                coeff = mediaCoeffs.get(media, 1.0)
                predicted_main_media_total += row[af_revenue_col] * coeff
        
        # 预测的其他媒体收入 = 直接使用原始收入
        predicted_other_media_total = 0
        for media in mediaOtherList:
            af_revenue_col = f'af_{media}_revenue_h168'
            if af_revenue_col in row:
                predicted_other_media_total += row[af_revenue_col]
        
        # 预测的总收入 = 自然量 + 主要媒体修正收入 + 其他媒体原始收入
        predicted_total = predicted_organic + predicted_main_media_total + predicted_other_media_total
        predicted_revenues.append(predicted_total)
        
        # 实际总收入
        actual_revenues.append(row['total_revenue_h168'])
    
    # 计算MAPE
    predicted_revenues = np.array(predicted_revenues)
    actual_revenues = np.array(actual_revenues)
    
    # 过滤掉实际收入为0的情况
    valid_mask = actual_revenues > 0
    if valid_mask.sum() == 0:
        return 0
    
    mape = np.mean(np.abs((actual_revenues[valid_mask] - predicted_revenues[valid_mask]) / actual_revenues[valid_mask])) * 100
    
    # 输出详细信息
    print(f"时间周期数: {len(period_df)}")
    print(f"平均实际收入: ${np.mean(actual_revenues):,.2f}")
    print(f"平均预测收入: ${np.mean(predicted_revenues):,.2f}")
    print(f"预测准确度: {100 - mape:.2f}%")
    print(f"主要媒体数量: {len(mediaList)}, 其他媒体数量: {len(mediaOtherList)}")
    
    return mape


def create_result_table():
    """创建结果表（分区表）"""
    from odps.models import Schema, Column, Partition
    
    o = getO()
    
    # 定义表结构（不包含tag，因为tag将作为分区）
    columns = [
        Column(name='country_group', type='string', comment='国家组'),
        Column(name='organic_revenue', type='double', comment='自然量收入'),
        Column(name='applovin_int_d7_coeff', type='double', comment='applovin_int_d7系数'),
        Column(name='applovin_int_d28_coeff', type='double', comment='applovin_int_d28系数'),
        Column(name='facebook_ads_coeff', type='double', comment='Facebook Ads系数'),
        Column(name='moloco_int_coeff', type='double', comment='moloco_int系数'),
        Column(name='bytedanceglobal_int_coeff', type='double', comment='bytedanceglobal_int系数')
    ]
    
    # 定义分区（tag作为分区字段）
    partitions = [
        Partition(name='tag', type='string', comment='标签分区，格式：20250805_{organic_ratio}')
    ]
    
    schema = Schema(columns=columns, partitions=partitions)
    
    # 创建表
    table_name = 'lw_20250703_ios_bayesian_result_by_j'
    try:
        table = o.create_table(table_name, schema, if_not_exists=True)
        print(f"分区表 {table_name} 创建成功或已存在")
        return table
    except Exception as e:
        print(f"创建表失败: {e}")
        return None


def write_results_to_odps(allResultsDf):
    """将所有结果写入ODPS数据库（分区表）"""
    if allResultsDf.empty:
        print("没有数据需要写入")
        return
    
    # 创建表（如果不存在）
    table = create_result_table()
    if table is None:
        print("无法创建表，写入失败")
        return
    
    try:
        # 获取ODPS连接
        o = getO()
        table = o.get_table('lw_20250703_ios_bayesian_result_by_j')
        
        # 按tag分组，为每个tag创建分区并写入数据
        tag_groups = allResultsDf.groupby('organic_ratio')
        
        for organic_ratio, group_df in tag_groups:
            # 生成tag：20250805_{organic_ratio}
            organic_ratio_str = f"{organic_ratio:.0%}".replace('%', '')  # 20% -> 20
            tag = f"20250805_{organic_ratio_str}"
            
            print(f"处理分区: {tag}")
            
            # 准备写入的数据（不包含tag列，因为tag是分区字段）
            write_data = []
            
            for _, row in group_df.iterrows():
                data_row = {
                    'country_group': row['country'],
                    'organic_revenue': row['organic_revenue_mean'],
                    'applovin_int_d7_coeff': row.get('applovin_int_d7_coeff', 1.0),
                    'applovin_int_d28_coeff': row.get('applovin_int_d28_coeff', 1.0),
                    'facebook_ads_coeff': row.get('Facebook Ads_coeff', 1.0),
                    'moloco_int_coeff': row.get('moloco_int_coeff', 1.0),
                    'bytedanceglobal_int_coeff': row.get('bytedanceglobal_int_coeff', 1.0)
                }
                write_data.append(data_row)
            
            # 转换为DataFrame
            write_df = pd.DataFrame(write_data)
            
            # 删除已存在的分区（如果存在）
            try:
                table.delete_partition(f"tag='{tag}'", if_exists=True)
                print(f"已删除分区: tag='{tag}'")
            except Exception as e:
                print(f"删除分区失败（可能不存在）: {e}")
            
            # 创建新分区
            try:
                table.create_partition(f"tag='{tag}'", if_not_exists=True)
                print(f"已创建分区: tag='{tag}'")
            except Exception as e:
                print(f"创建分区失败: {e}")
                continue
            
            # 写入数据到分区
            try:
                with table.open_writer(partition=f"tag='{tag}'", arrow=True) as writer:
                    writer.write(write_df)
                
                print(f"成功写入 {len(write_df)} 条记录到分区 tag='{tag}'")
                print(f"分区 {tag} 数据预览:")
                print(write_df.head())
                print("-" * 50)
                
            except Exception as e:
                print(f"写入分区 {tag} 失败: {e}")
                # 保存到本地作为备份
                backup_filename = f'/src/data/odps_backup_{tag}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                write_df.to_csv(backup_filename, index=False)
                print(f"分区 {tag} 数据已备份到: {backup_filename}")
        
        print(f"\n所有分区写入完成！")
        
    except Exception as e:
        print(f"写入ODPS失败: {e}")
        # 保存到本地作为备份
        backup_filename = f'/src/data/odps_backup_all_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        allResultsDf.to_csv(backup_filename, index=False)
        print(f"所有数据已备份到: {backup_filename}")


def main():
    startDayStr = '20250101'
    endDayStr = '20250729'
    df = getData(startDayStr, endDayStr)
    
    # 进行适度过滤，install_day > '20250101'
    df = df[df['install_day'] >= '20250101']
    df.to_csv(f'/src/data/20250805_data_{startDayStr}_{endDayStr}.csv', index=False)
    # return

    # 只保留必要的列：install_day, country_group, total_revenue_h168 和所有媒体收入列
    keepColumns = ['install_day', 'country_group', 'total_revenue_h168']
    mediaColumns = [col for col in df.columns if col.startswith('af_') and col.endswith('_revenue_h168')]
    mediaCostColumns = [col for col in df.columns if col.startswith('af_') and col.endswith('_cost')]
    keepColumns.extend(mediaColumns)
    keepColumns.extend(mediaCostColumns)
    df = df[keepColumns]

    countryList = df['country_group'].unique()
    # for quick test，测试完成后，注释下面一行
    # countryList = ['US']

    # 所有有过花费的媒体
    mediaAllList = [
        'Apple Search Ads','Facebook Ads','Twitter','applovin_int'
        ,'applovin_int_d28','applovin_int_d7','bytedanceglobal_int'
        ,'googleadwords_int','liftoff_int','mintegral_int','moloco_int'
        ,'smartnewsads_int','snapchat_int','unityads_int'
    ]
    # 主要分析并拟合的媒体
    mediaList = ['applovin_int_d7','Facebook Ads','moloco_int','applovin_int_d28','bytedanceglobal_int']
    # 剩余媒体
    mediaOtherList = [media for media in mediaAllList if media not in mediaList]

    # 用于存储所有国家的所有结果（包括20%, 30%, 40%三种情况）
    allResults = []
    
    for country in countryList:
        print(f"\n{'='*60}")
        print(f"开始处理国家: {country}")
        print(f"{'='*60}")
        
        calculate_media_attribution(df, country)

        # 贝叶斯拟合
        resultDf = bayesian_fit_media_coefficients(df, country, mediaList,mediaOtherList)
        
        if resultDf is not None:
            # 分析结果（获取最佳结果用于验算）
            bestResult = analyze_results(resultDf, country)
            
            # 使用最佳结果进行按周和按月的验算
            weekly_mape, monthly_mape = validate_model_by_period(df, country, bestResult, mediaList, mediaOtherList)
            
            # 将所有结果（20%, 30%, 40%）添加到列表中
            for _, row in resultDf.iterrows():
                # 为每个结果添加验算信息（使用最佳结果的验算结果）
                row_dict = row.to_dict()
                row_dict['weekly_mape'] = weekly_mape
                row_dict['monthly_mape'] = monthly_mape
                allResults.append(row_dict)
            
            # 保存单个国家的所有结果
            resultDf.to_csv(f'/src/data/bayesian_fit_result_{country}_{startDayStr}_{endDayStr}.csv', index=False)
            
            # 保存包含验算结果的最佳方案
            best_result_with_validation = pd.DataFrame([bestResult])
            best_result_with_validation.to_csv(f'/src/data/best_result_with_validation_{country}_{startDayStr}_{endDayStr}.csv', index=False)
    
    # 合并所有国家的所有结果
    if allResults:
        allResultsDf = pd.DataFrame(allResults)
        
        # 保存合并后的所有结果
        combined_filename = f'/src/data/combined_all_results_{startDayStr}_{endDayStr}.csv'
        allResultsDf.to_csv(combined_filename, index=False)
        print(f"\n所有国家的结果已合并保存到: {combined_filename}")
        print(f"总共处理了 {len(allResultsDf)} 条记录")
        
        # 调用write_results_to_odps将合并后的结果写入数据库
        print(f"\n开始将合并后的结果写入ODPS数据库...")
        write_results_to_odps(allResultsDf)
        
        print(f"\n=== 处理完成 ===")
        print(f"处理的国家数量: {len(countryList)}")
        print(f"总记录数: {len(allResultsDf)}")
        print(f"数据已保存到本地文件: {combined_filename}")
        print(f"数据已写入ODPS表: lw_20250703_ios_bayesian_result_by_j")
        
    else:
        print("警告: 没有生成任何结果数据")


if __name__ == "__main__":
    main()
