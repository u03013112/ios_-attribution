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
    totalDf = df.groupby(['install_day','country_group']).sum().reset_index()
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
        print(df0)
        if afDf.empty:
            afDf = df0
        else:
            afDf = pd.merge(afDf, df0, on=['install_day', 'country_group'], how='outer')
    afDf = afDf.fillna(0)
    return afDf

def getData(startDayStr, endDayStr):
    df = getBiData(startDayStr, endDayStr)
    
    mediaList = getMediaList(df)

    totalDf = getTotalData(df)
    afDf = getAfData(df,mediaList)

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


def bayesian_fit_media_coefficients(df, country, mediaList):
    """使用贝叶斯方法拟合媒体系数"""
    countryDf = df[df['country_group'] == country].copy()
    
    if countryDf.empty:
        print(f"警告: 国家 {country} 没有数据")
        return None
    
    # 计算总收入均值，用于设置自然量先验
    totalRevenueMean = countryDf['total_revenue_h168'].mean()
    
    # 自然量先验配置：20%, 30%, 40%
    organicRevenueConfigList = [
        {'ratio': 0.2, 'mu': totalRevenueMean * 0.2, 'sigma': totalRevenueMean * 0.05},
        {'ratio': 0.3, 'mu': totalRevenueMean * 0.3, 'sigma': totalRevenueMean * 0.05},
        {'ratio': 0.4, 'mu': totalRevenueMean * 0.4, 'sigma': totalRevenueMean * 0.05}
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
            
            # 计算预测的总收入
            predictedTotalRevenue = organicRevenue + sum(mediaTrueRevenues.values())
            
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
            
            # 保存结果
            result = {
                'country': country,
                'organic_ratio': organicConfig['ratio'],
                'organic_revenue_mean': summary.loc['organicRevenue', 'mean'],
                'organic_revenue_hdi_lower': summary.loc['organicRevenue', 'hdi_2.5%'],
                'organic_revenue_hdi_upper': summary.loc['organicRevenue', 'hdi_97.5%'],
                'mape': mape,
                **{f'{media}_coeff': coeffResults.get(media, {}).get('coeff_mean', 1.0) for media in mediaList},
                **{f'{media}_modified_roi': modifiedROIs.get(media, 0) for media in mediaList}
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

def analyze_results(resultDf, country):
    """分析拟合结果"""
    print(f"\n=== {country} 拟合结果分析 ===")
    
    # 按MAPE排序
    resultDf_sorted = resultDf.sort_values('mape')
    
    print("各方案MAPE对比:")
    for _, row in resultDf_sorted.iterrows():
        print(f"自然量占比 {row['organic_ratio']:.0%}: MAPE = {row['mape']:.2f}%")
    
    # 显示最佳方案的详细结果
    bestResult = resultDf_sorted.iloc[0]
    print(f"\n最佳方案 (自然量占比 {bestResult['organic_ratio']:.0%}):")
    print(f"自然量收入: ${bestResult['organic_revenue_mean']:,.2f}")
    
    mediaList = ['applovin_int_d7','Facebook Ads','moloco_int','applovin_int_d28','bytedanceglobal_int']
    print("\n媒体系数:")
    for media in mediaList:
        coeff = bestResult.get(f'{media}_coeff', 1.0)
        roi = bestResult.get(f'{media}_modified_roi', 0)
        print(f"  {media}: 系数={coeff:.3f}, 修正ROI={roi:.3f}")
    
    return bestResult

def main():
    startDayStr = '20240729'
    endDayStr = '20250729'
    df = getData(startDayStr, endDayStr)
    
    # 进行适度过滤，install_day > '20250101'
    df = df[df['install_day'] >= '20250101']
    
    # df.tail(100).to_csv(f'/src/data/iOS20250729_Debug_{startDayStr}_{endDayStr}.csv', index=False)

    # 只保留必要的列：install_day, country_group, total_revenue_h168 和所有媒体收入列
    keepColumns = ['install_day', 'country_group', 'total_revenue_h168']
    mediaColumns = [col for col in df.columns if col.startswith('af_') and col.endswith('_revenue_h168')]
    keepColumns.extend(mediaColumns)
    df = df[keepColumns]

    countryList = df['country_group'].unique()
    # for quick test，测试完成后，注释下面一行
    countryList = ['US']

    mediaList = ['applovin_int_d7','Facebook Ads','moloco_int','applovin_int_d28','bytedanceglobal_int']

    for country in countryList:
        calculate_media_attribution(df, country)

        # 贝叶斯拟合
        resultDf = bayesian_fit_media_coefficients(df, country, mediaList)
        
        if resultDf is not None:
            # 分析结果
            bestResult = analyze_results(resultDf, country)
            
            # 保存结果
            resultDf.to_csv(f'/src/data/bayesian_fit_result_{country}_{startDayStr}_{endDayStr}.csv', index=False)



if __name__ == '__main__':
    main()
