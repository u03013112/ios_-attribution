# 模糊归因版本
import os
import datetime
import arviz as az
import pandas as pd
import pymc as pm
import numpy as np
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO

def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/mohu_revenue_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    mediasource,
    country,
    sum(cost) as cost,
    sum(revenue_h24) as revenue_h24,
    sum(revenue_d7) as revenue_d7
from
(
    select
    install_day,
    mediasource,
    COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country,
    cost_value_usd as cost,
    revenue_h24 as revenue_h24,
    revenue_d7 as revenue_d7
from
    dws_overseas_public_roi t1
    left join cdm_laswwar_country_map on t1.country = cdm_laswwar_country_map.country
where
    app = '502'
    and app_package = 'id6448786147'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
)
group by
    install_day,
    mediasource,
    country
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

# 大盘，不分国家
def bayesianTotalDataPrepare(startDayStr, endDayStr):
    mediaList = [
        'Facebook Ads','applovin_int','moloco_int','bytedanceglobal_int','snapchat_int'
    ]

    revenueDf = getRevenueData(startDayStr, endDayStr)
    revenueTotalDf = revenueDf.groupby(['install_day']).agg({
        'revenue_h24': 'sum',
        'revenue_d7': 'sum',

    }).reset_index()
    revenueTotalDf.rename(columns={
        'revenue_h24': 'total 24h revenue',
        'revenue_d7': 'total 7d revenue'
    }, inplace=True)
    revenueTotalDf = revenueTotalDf.sort_values(by=['install_day'], ascending=[False])

    revenueMediaDf = revenueDf[revenueDf['mediasource'].isin(mediaList)]
    revenueMediaDf = revenueMediaDf.groupby(['install_day', 'mediasource']).agg({
        'cost': 'sum',
        'revenue_h24': 'sum',
        'revenue_d7': 'sum',
    }).reset_index()

    revenue24hMediaDf = revenueMediaDf.pivot(index='install_day', columns='mediasource', values='revenue_h24').reset_index()
    revenue24hMediaDf.rename(columns={
        'applovin_int': 'applovin 24h revenue',
        'Facebook Ads': 'facebook 24h revenue',
        'moloco_int': 'moloco 24h revenue',
        'bytedanceglobal_int': 'tiktok 24h revenue',
        'snapchat_int': 'snapchat 24h revenue'
    }, inplace=True)
    
    revenue7dMediaDf = revenueMediaDf.pivot(index='install_day', columns='mediasource', values='revenue_d7').reset_index()
    revenue7dMediaDf.rename(columns={
        'applovin_int': 'applovin 7d revenue',
        'Facebook Ads': 'facebook 7d revenue',
        'moloco_int': 'moloco 7d revenue',
        'bytedanceglobal_int': 'tiktok 7d revenue',
        'snapchat_int': 'snapchat 7d revenue'
    }, inplace=True)

    costMediaDf = revenueMediaDf.pivot(index='install_day', columns='mediasource', values='cost').reset_index()
    costMediaDf.rename(columns={
        'applovin_int': 'applovin cost',
        'Facebook Ads': 'facebook cost',
        'moloco_int': 'moloco cost',
        'bytedanceglobal_int': 'tiktok cost',
        'snapchat_int': 'snapchat cost'
    }, inplace=True)

    prepareDf = pd.merge(revenueTotalDf, revenue24hMediaDf, on='install_day', how='left')
    prepareDf = pd.merge(prepareDf, revenue7dMediaDf, on='install_day', how='left')
    prepareDf = pd.merge(prepareDf, costMediaDf, on='install_day', how='left')
    prepareDf = prepareDf.sort_values(by=['install_day'], ascending=[False])

    prepareDf.to_csv('/src/data/lw_mohu_bayesian_total.csv', index=False)
    return prepareDf

def cc(summary, prepareDf):
    # 提取参数的均值作为估计值
    organicRevenue_mean = summary.loc['organicRevenue', 'mean']
    facebookX_mean = summary.loc['facebookX', 'mean']
    applovinX_mean = summary.loc['applovinX', 'mean']
    # googleX_mean = summary.loc['googleX', 'mean']
    tiktokX_mean = summary.loc['tiktokX', 'mean']
    molocoX_mean = summary.loc['molocoX', 'mean']
    snapchatX_mean = summary.loc['snapchatX', 'mean']

    # 使用参数估计值计算预测值
    predicted_revenue = organicRevenue_mean + \
                        facebookX_mean * prepareDf['facebook 7d revenue'] + \
                        applovinX_mean * prepareDf['applovin 7d revenue'] + \
                        tiktokX_mean * prepareDf['tiktok 7d revenue'] + \
                        molocoX_mean * prepareDf['moloco 7d revenue'] + \
                        snapchatX_mean * prepareDf['snapchat 7d revenue']
    
    detailDf = pd.DataFrame({
        'install_week': prepareDf['install_week'],
        'Facebook_actual_revenue': prepareDf['facebook 7d revenue'],
        'applovin_actual_revenue': prepareDf['applovin 7d revenue'],
        # 'google_actual_revenue': prepareDf['google revenue'],
        'tiktok_actual_revenue': prepareDf['tiktok 7d revenue'],
        'moloco_actual_revenue': prepareDf['moloco 7d revenue'],
        'snapchat_actual_revenue': prepareDf['snapchat 7d revenue'],
        'Facebook_predicted_revenue': facebookX_mean * prepareDf['facebook 7d revenue'],
        'applovin_predicted_revenue': applovinX_mean * prepareDf['applovin 7d revenue'],
        'tiktok_predicted_revenue': tiktokX_mean * prepareDf['tiktok 7d revenue'],
        'moloco_predicted_revenue': molocoX_mean * prepareDf['moloco 7d revenue'],
        'snapchat_predicted_revenue': snapchatX_mean * prepareDf['snapchat 7d revenue'],
        'actual_revenue': prepareDf['total 7d revenue'],
        'predicted_revenue': predicted_revenue,
        'organicRevenue_predicted': organicRevenue_mean,
    })

    # 计算绝对百分比误差
    absolute_percentage_error = np.abs((detailDf['actual_revenue'] - detailDf['predicted_revenue']) / detailDf['actual_revenue'])
    
    # 计算 MAPE
    mape = np.mean(absolute_percentage_error)
    # print(f'MAPE: {mape:.2f}%')

    # 计算自然量占比
    organicRatio = detailDf['organicRevenue_predicted'].sum() / detailDf['predicted_revenue'].sum()
    # print(f'Organic Ratio: {organicRatio:.2f}%')

    data = {
        'organicRevenue_predicted': [organicRevenue_mean],
        'facebook X': [facebookX_mean],
        'applovin X': [applovinX_mean],
        'tiktok X': [tiktokX_mean],
        'moloco X': [molocoX_mean],
        'snapchat X': [snapchatX_mean],
        'mape': [mape],
        'organicRatio': [organicRatio],
    }
    
    retDf = pd.DataFrame(data)
    return retDf,detailDf

# 数据是by day的，但是按照周来汇总，并计算MAPE
def ccByDay(summary, prepareDf):
    # 提取参数的均值作为估计值
    organicRevenue_mean = summary.loc['organicRevenue', 'mean']
    facebookX_mean = summary.loc['facebookX', 'mean']
    applovinX_mean = summary.loc['applovinX', 'mean']
    tiktokX_mean = summary.loc['tiktokX', 'mean']
    molocoX_mean = summary.loc['molocoX', 'mean']
    snapchatX_mean = summary.loc['snapchatX', 'mean']

    # 使用参数估计值计算预测值
    predicted_revenue = organicRevenue_mean + \
                        facebookX_mean * prepareDf['facebook 7d revenue'] + \
                        applovinX_mean * prepareDf['applovin 7d revenue'] + \
                        tiktokX_mean * prepareDf['tiktok 7d revenue'] + \
                        molocoX_mean * prepareDf['moloco 7d revenue'] + \
                        snapchatX_mean * prepareDf['snapchat 7d revenue']
    
    detailDf = pd.DataFrame({
        'install_day': prepareDf['install_day'],
        'Facebook_actual_revenue': prepareDf['facebook 7d revenue'],
        'applovin_actual_revenue': prepareDf['applovin 7d revenue'],
        'tiktok_actual_revenue': prepareDf['tiktok 7d revenue'],
        'moloco_actual_revenue': prepareDf['moloco 7d revenue'],
        'snapchat_actual_revenue': prepareDf['snapchat 7d revenue'],
        'Facebook_predicted_revenue': facebookX_mean * prepareDf['facebook 7d revenue'],
        'applovin_predicted_revenue': applovinX_mean * prepareDf['applovin 7d revenue'],
        'tiktok_predicted_revenue': tiktokX_mean * prepareDf['tiktok 7d revenue'],
        'moloco_predicted_revenue': molocoX_mean * prepareDf['moloco 7d revenue'],
        'snapchat_predicted_revenue': snapchatX_mean * prepareDf['snapchat 7d revenue'],
        'actual_revenue': prepareDf['total 7d revenue'],
        'predicted_revenue': predicted_revenue,
        'organicRevenue_predicted': organicRevenue_mean,
    })

    detailDf['install_week'] = detailDf['install_day'].dt.strftime('%Y-%W')
    # 将install_week中的'2024-53'转换为'2025-00'，因为53周是跨年的
    detailDf['install_week'] = detailDf['install_week'].replace({'2024-53': '2025-00'})
    detailDf = detailDf.drop(columns=['install_day'])
    detailDf = detailDf.groupby(['install_week']).sum().reset_index()

    # 计算绝对百分比误差
    absolute_percentage_error = np.abs((detailDf['actual_revenue'] - detailDf['predicted_revenue']) / detailDf['actual_revenue'])
    
    # 计算 MAPE
    mape = np.mean(absolute_percentage_error)
    # print(f'MAPE: {mape:.2f}%')

    # 计算自然量占比
    organicRatio = detailDf['organicRevenue_predicted'].sum() / detailDf['predicted_revenue'].sum()
    # print(f'Organic Ratio: {organicRatio:.2f}%')

    data = {
        'organicRevenue_predicted': [organicRevenue_mean],
        'facebook X': [facebookX_mean],
        'applovin X': [applovinX_mean],
        'tiktok X': [tiktokX_mean],
        'moloco X': [molocoX_mean],
        'snapchat X': [snapchatX_mean],
        'mape': [mape],
        'organicRatio': [organicRatio],
    }
    
    retDf = pd.DataFrame(data)
    return retDf,detailDf

def totalMain():
    # startDayStr = '20250106'
    # startDayStr = '20240728'
    startDayStr = '20241216'
    endDayStr = '20250518'
    
    prepareDf = bayesianTotalDataPrepare(startDayStr, endDayStr)

    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf['install_week'] = prepareDf['install_day'].dt.strftime('%Y-%W')
    prepareDf = prepareDf.drop(columns=['install_day'])
    prepareWeekDf = prepareDf.groupby(['install_week']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/totalMain_{startDayStr}_{endDayStr}.csv', index=False)

    # 计算目前的自然量占比
    organicRevenue = prepareWeekDf['total 7d revenue'] - \
                    (prepareWeekDf['facebook 7d revenue'] + 
                    prepareWeekDf['applovin 7d revenue'] + 
                    prepareWeekDf['moloco 7d revenue'] + 
                    prepareWeekDf['tiktok 7d revenue'] + 
                    prepareWeekDf['snapchat 7d revenue'])
    prepareWeekDf['organic_revenue'] = organicRevenue
    organic_ratio = prepareWeekDf['organic_revenue'].sum() / prepareWeekDf['total 7d revenue'].sum()
    print(f'Organic Revenue Ratio: {organic_ratio:.2%}')


    facebookRoi = prepareWeekDf['facebook 7d revenue'].sum() / prepareWeekDf['facebook cost'].sum()
    applovinRoi = prepareWeekDf['applovin 7d revenue'].sum() / prepareWeekDf['applovin cost'].sum()
    tiktokRoi = prepareWeekDf['tiktok 7d revenue'].sum() / prepareWeekDf['tiktok cost'].sum()
    molocoRoi = prepareWeekDf['moloco 7d revenue'].sum() / prepareWeekDf['moloco cost'].sum()
    snapchatRoi = prepareWeekDf['snapchat 7d revenue'].sum() / prepareWeekDf['snapchat cost'].sum()
    print(f'Facebook ROI: {facebookRoi:.2%}')
    print(f'Applovin ROI: {applovinRoi:.2%}')
    print(f'TikTok ROI: {tiktokRoi:.2%}')
    print(f'Moloco ROI: {molocoRoi:.2%}')
    print(f'Snapchat ROI: {snapchatRoi:.2%}')


    facebookRevenueRatio = prepareWeekDf['facebook 7d revenue'] / prepareWeekDf['total 7d revenue']
    applovinRevenueRatio = prepareWeekDf['applovin 7d revenue'] / prepareWeekDf['total 7d revenue']
    tiktokRevenueRatio = prepareWeekDf['tiktok 7d revenue'] / prepareWeekDf['total 7d revenue']
    molocoRevenueRatio = prepareWeekDf['moloco 7d revenue'] / prepareWeekDf['total 7d revenue']
    snapchatRevenueRatio = prepareWeekDf['snapchat 7d revenue'] / prepareWeekDf['total 7d revenue']
    ratioDf = pd.DataFrame({
        'facebook_revenue_ratio': facebookRevenueRatio,
        'applovin_revenue_ratio': applovinRevenueRatio,
        'tiktok_revenue_ratio': tiktokRevenueRatio,
        'moloco_revenue_ratio': molocoRevenueRatio,
        'snapchat_revenue_ratio': snapchatRevenueRatio,
    })
    
    # 计算每个媒体的收入占比标准差
    std_devs = ratioDf.std()
    print("每个媒体的收入占比标准差:")
    print(std_devs)

    organicRevenueConfigList = [
        {'mu':60000, 'sigma':2000},
        {'mu':110000, 'sigma':2000},
        {'mu':160000, 'sigma':2000},
        {'mu':210000, 'sigma':2000},
        {'mu':250000, 'sigma':2000},
    ]


    resultDf = pd.DataFrame()

    for organicConfigRevenue in organicRevenueConfigList:
        basic_model = pm.Model()

        # 贝叶斯模型
        with basic_model as model:
            # 先验分布
            organicRevenue = pm.Normal('organicRevenue', mu=organicConfigRevenue['mu'], sigma=organicConfigRevenue['sigma'])
            facebookX = pm.Normal('facebookX', mu=1, sigma=0.05)
            applovinX = pm.Normal('applovinX', mu=1, sigma=0.05)
            # googleX = pm.Normal('googleX', mu=1, sigma=0.05)
            tiktokX = pm.Normal('tiktokX', mu=1, sigma=0.05)
            molocoX = pm.Normal('molocoX', mu=1, sigma=0.05)
            snapchatX = pm.Normal('snapchatX', mu=1, sigma=0.05)
    
            mu = organicRevenue + \
                facebookX * prepareWeekDf['facebook 7d revenue'] + \
                applovinX * prepareWeekDf['applovin 7d revenue'] + \
                molocoX * prepareWeekDf['moloco 7d revenue'] + \
                tiktokX * prepareWeekDf['tiktok 7d revenue'] + \
                snapchatX * prepareWeekDf['snapchat 7d revenue']
            
            revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=3000, observed=prepareWeekDf['total 7d revenue'])
            trace = pm.sample(1000)

        # 输出结果
        summary = pm.summary(trace, hdi_prob=0.95)

        retDf,detailDf = cc(summary, prepareWeekDf)
        retDf['organicRevenueMu'] = organicConfigRevenue['mu']

        detailDf.to_csv(f'/src/data/detail_{organicConfigRevenue["mu"]}_{startDayStr}_{endDayStr}.csv', index=False)
        
        facebookActualRevenueRatio = detailDf['Facebook_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        applovinActualRevenueRatio = detailDf['applovin_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        tiktokActualRevenueRatio = detailDf['tiktok_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        molocoActualRevenueRatio = detailDf['moloco_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        snapchatActualRevenueRatio = detailDf['snapchat_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        print(f'Organic Revenue Mu: {organicConfigRevenue["mu"]}')
        print(f'Facebook Actual Revenue Ratio: {facebookActualRevenueRatio:.2%}')
        print(f'Applovin Actual Revenue Ratio: {applovinActualRevenueRatio:.2%}')
        print(f'TikTok Actual Revenue Ratio: {tiktokActualRevenueRatio:.2%}')
        print(f'Moloco Actual Revenue Ratio: {molocoActualRevenueRatio:.2%}')
        print(f'Snapchat Actual Revenue Ratio: {snapchatActualRevenueRatio:.2%}')
        facebookPredictedRevenueRatio = detailDf['Facebook_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        applovinPredictedRevenueRatio = detailDf['applovin_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        tiktokPredictedRevenueRatio = detailDf['tiktok_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        molocoPredictedRevenueRatio = detailDf['moloco_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        snapchatPredictedRevenueRatio = detailDf['snapchat_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        print(f'Facebook Predicted Revenue Ratio: {facebookPredictedRevenueRatio:.2%}')
        print(f'Applovin Predicted Revenue Ratio: {applovinPredictedRevenueRatio:.2%}')
        print(f'TikTok Predicted Revenue Ratio: {tiktokPredictedRevenueRatio:.2%}')
        print(f'Moloco Predicted Revenue Ratio: {molocoPredictedRevenueRatio:.2%}')
        print(f'Snapchat Predicted Revenue Ratio: {snapchatPredictedRevenueRatio:.2%}')

        resultDf = pd.concat([resultDf, retDf], ignore_index=True)

    print(resultDf)
    resultDf.to_csv(f'/src/data/mohu_result_{startDayStr}_{endDayStr}.csv', index=False)


    kpi = 0.0808
    resultDf['kpi'] = kpi

    resultDf['facebook kpi'] = (kpi/resultDf['facebook X'])
    resultDf['applovin kpi'] = (kpi/resultDf['applovin X'])
    resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
    resultDf['moloco kpi'] = (kpi/resultDf['moloco X'])
    resultDf['snapchat kpi'] = (kpi/resultDf['snapchat X'])

    resultDf = resultDf.rename(
        columns={
            'organicRevenueMu': 'organic_revenue_mu',
            'facebook X': 'facebook_x',
            'applovin X': 'applovin_x',
            'tiktok X': 'tiktok_x',
            'moloco X': 'moloco_x',
            'snapchat X': 'snapchat_x',
            'mape': 'mape',
            'organicRatio': 'organic_ratio',
            'kpi': '7d kpi',
            'facebook kpi': 'facebook_kpi',
            'applovin kpi': 'applovin_kpi',
            'tiktok kpi': 'tiktok_kpi',
            'moloco kpi': 'moloco_kpi',
            'snapchat kpi': 'snapchat_kpi',
        }
    )
    print(resultDf)
    resultDf.to_csv(f'/src/data/result_{startDayStr}_{endDayStr}_kpi.csv', index=False)

# 不再按周汇总
def totalMainByDay():
    print("Starting totalMainByDay...")
    startDayStr = '20241216'
    endDayStr = '20250518'
    
    prepareDf = bayesianTotalDataPrepare(startDayStr, endDayStr)

    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf.rename(columns={'install_day': 'install_week'}, inplace=True)
    prepareWeekDf = prepareDf.groupby(['install_week']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/totalMainByDay_{startDayStr}_{endDayStr}.csv', index=False)

    # 计算目前的自然量占比
    organicRevenue = prepareWeekDf['total 7d revenue'] - \
                    (prepareWeekDf['facebook 7d revenue'] + 
                    prepareWeekDf['applovin 7d revenue'] + 
                    prepareWeekDf['moloco 7d revenue'] + 
                    prepareWeekDf['tiktok 7d revenue'] + 
                    prepareWeekDf['snapchat 7d revenue'])
    prepareWeekDf['organic_revenue'] = organicRevenue
    organic_ratio = prepareWeekDf['organic_revenue'].sum() / prepareWeekDf['total 7d revenue'].sum()
    organic_mean = prepareWeekDf['organic_revenue'].mean()
    print(f'Organic Revenue Ratio: {organic_ratio:.2%}')
    print(f'Organic Revenue Mean: {organic_mean:.2f}')


    facebookRoi = prepareWeekDf['facebook 7d revenue'].sum() / prepareWeekDf['facebook cost'].sum()
    applovinRoi = prepareWeekDf['applovin 7d revenue'].sum() / prepareWeekDf['applovin cost'].sum()
    tiktokRoi = prepareWeekDf['tiktok 7d revenue'].sum() / prepareWeekDf['tiktok cost'].sum()
    molocoRoi = prepareWeekDf['moloco 7d revenue'].sum() / prepareWeekDf['moloco cost'].sum()
    snapchatRoi = prepareWeekDf['snapchat 7d revenue'].sum() / prepareWeekDf['snapchat cost'].sum()
    print(f'Facebook ROI: {facebookRoi:.2%}')
    print(f'Applovin ROI: {applovinRoi:.2%}')
    print(f'TikTok ROI: {tiktokRoi:.2%}')
    print(f'Moloco ROI: {molocoRoi:.2%}')
    print(f'Snapchat ROI: {snapchatRoi:.2%}')


    facebookRevenueRatio = prepareWeekDf['facebook 7d revenue'] / prepareWeekDf['total 7d revenue']
    applovinRevenueRatio = prepareWeekDf['applovin 7d revenue'] / prepareWeekDf['total 7d revenue']
    tiktokRevenueRatio = prepareWeekDf['tiktok 7d revenue'] / prepareWeekDf['total 7d revenue']
    molocoRevenueRatio = prepareWeekDf['moloco 7d revenue'] / prepareWeekDf['total 7d revenue']
    snapchatRevenueRatio = prepareWeekDf['snapchat 7d revenue'] / prepareWeekDf['total 7d revenue']
    ratioDf = pd.DataFrame({
        'facebook_revenue_ratio': facebookRevenueRatio,
        'applovin_revenue_ratio': applovinRevenueRatio,
        'tiktok_revenue_ratio': tiktokRevenueRatio,
        'moloco_revenue_ratio': molocoRevenueRatio,
        'snapchat_revenue_ratio': snapchatRevenueRatio,
    })
    
    # 计算每个媒体的收入占比标准差
    std_devs = ratioDf.std()
    print("每个媒体的收入占比标准差:")
    print(std_devs)

    organicRevenueConfigList = [
        {'mu':10000, 'sigma':100},
        {'mu':15000, 'sigma':100},
        {'mu':20000, 'sigma':100},
        {'mu':25000, 'sigma':100},
        {'mu':30000, 'sigma':100},
    ]

    resultDf = pd.DataFrame()

    for organicConfigRevenue in organicRevenueConfigList:
        basic_model = pm.Model()

        # 贝叶斯模型
        with basic_model as model:
            # 先验分布
            organicRevenue = pm.Normal('organicRevenue', mu=organicConfigRevenue['mu'], sigma=organicConfigRevenue['sigma'])
            facebookX = pm.Normal('facebookX', mu=1, sigma=0.05)
            applovinX = pm.Normal('applovinX', mu=1, sigma=0.05)
            tiktokX = pm.Normal('tiktokX', mu=1, sigma=0.05)
            molocoX = pm.Normal('molocoX', mu=1, sigma=0.05)
            snapchatX = pm.Normal('snapchatX', mu=1, sigma=0.05)
    
            mu = organicRevenue + \
                facebookX * prepareWeekDf['facebook 7d revenue'] + \
                applovinX * prepareWeekDf['applovin 7d revenue'] + \
                molocoX * prepareWeekDf['moloco 7d revenue'] + \
                tiktokX * prepareWeekDf['tiktok 7d revenue'] + \
                snapchatX * prepareWeekDf['snapchat 7d revenue']
            
            revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=1000, observed=prepareWeekDf['total 7d revenue'])
            trace = pm.sample(1000)

        # 输出结果
        summary = pm.summary(trace, hdi_prob=0.95)

        prepareWeekDf.rename(columns={'install_week': 'install_day'}, inplace=True)

        retDf,detailDf = ccByDay(summary, prepareWeekDf)
        retDf['organicRevenueMu'] = organicConfigRevenue['mu']

        detailDf.to_csv(f'/src/data/detailByDay_{organicConfigRevenue["mu"]}_{startDayStr}_{endDayStr}.csv', index=False)
        
        facebookActualRevenueRatio = detailDf['Facebook_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        applovinActualRevenueRatio = detailDf['applovin_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        tiktokActualRevenueRatio = detailDf['tiktok_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        molocoActualRevenueRatio = detailDf['moloco_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        snapchatActualRevenueRatio = detailDf['snapchat_actual_revenue'].sum() / detailDf['actual_revenue'].sum()
        print(f'Organic Revenue Mu: {organicConfigRevenue["mu"]}')
        print(f'Facebook Actual Revenue Ratio: {facebookActualRevenueRatio:.2%}')
        print(f'Applovin Actual Revenue Ratio: {applovinActualRevenueRatio:.2%}')
        print(f'TikTok Actual Revenue Ratio: {tiktokActualRevenueRatio:.2%}')
        print(f'Moloco Actual Revenue Ratio: {molocoActualRevenueRatio:.2%}')
        print(f'Snapchat Actual Revenue Ratio: {snapchatActualRevenueRatio:.2%}')
        facebookPredictedRevenueRatio = detailDf['Facebook_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        applovinPredictedRevenueRatio = detailDf['applovin_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        tiktokPredictedRevenueRatio = detailDf['tiktok_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        molocoPredictedRevenueRatio = detailDf['moloco_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        snapchatPredictedRevenueRatio = detailDf['snapchat_predicted_revenue'].sum() / detailDf['actual_revenue'].sum()
        print(f'Facebook Predicted Revenue Ratio: {facebookPredictedRevenueRatio:.2%}')
        print(f'Applovin Predicted Revenue Ratio: {applovinPredictedRevenueRatio:.2%}')
        print(f'TikTok Predicted Revenue Ratio: {tiktokPredictedRevenueRatio:.2%}')
        print(f'Moloco Predicted Revenue Ratio: {molocoPredictedRevenueRatio:.2%}')
        print(f'Snapchat Predicted Revenue Ratio: {snapchatPredictedRevenueRatio:.2%}')

        resultDf = pd.concat([resultDf, retDf], ignore_index=True)

    print(resultDf)
    resultDf.to_csv(f'/src/data/mohuByDay_result_{startDayStr}_{endDayStr}.csv', index=False)


    kpi = 0.0808
    resultDf['kpi'] = kpi

    resultDf['facebook kpi'] = (kpi/resultDf['facebook X'])
    resultDf['applovin kpi'] = (kpi/resultDf['applovin X'])
    resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
    resultDf['moloco kpi'] = (kpi/resultDf['moloco X'])
    resultDf['snapchat kpi'] = (kpi/resultDf['snapchat X'])

    resultDf = resultDf.rename(
        columns={
            'organicRevenueMu': 'organic_revenue_mu',
            'facebook X': 'facebook_x',
            'applovin X': 'applovin_x',
            'tiktok X': 'tiktok_x',
            'moloco X': 'moloco_x',
            'snapchat X': 'snapchat_x',
            'mape': 'mape',
            'organicRatio': 'organic_ratio',
            'kpi': '7d kpi',
            'facebook kpi': 'facebook_kpi',
            'applovin kpi': 'applovin_kpi',
            'tiktok kpi': 'tiktok_kpi',
            'moloco kpi': 'moloco_kpi',
            'snapchat kpi': 'snapchat_kpi',
        }
    )
    print(resultDf)
    resultDf.to_csv(f'/src/data/resultByDay_{startDayStr}_{endDayStr}_kpi.csv', index=False)


def bayesianCountryDataPrepare(startDayStr, endDayStr):
    mediaList = [
        'Facebook Ads','applovin_int','moloco_int','bytedanceglobal_int','snapchat_int'
    ]

    revenueDf = getRevenueData(startDayStr, endDayStr)
    revenueTotalDf = revenueDf.groupby(['install_day','country']).agg({
        'revenue_d7': 'sum',

    }).reset_index()
    revenueTotalDf.rename(columns={
        'revenue_d7': 'total 7d revenue'
    }, inplace=True)
    revenueTotalDf = revenueTotalDf.sort_values(by=['install_day','country'], ascending=[False, True])

    revenueMediaDf = revenueDf[revenueDf['mediasource'].isin(mediaList)]
    revenueMediaDf = revenueMediaDf.groupby(['install_day', 'mediasource','country']).agg({
        'cost': 'sum',
        'revenue_d7': 'sum',
    }).reset_index()
    
    revenue7dMediaDf = revenueMediaDf.pivot(index=['install_day','country'], columns='mediasource', values='revenue_d7').reset_index()
    revenue7dMediaDf.rename(columns={
        'applovin_int': 'applovin 7d revenue',
        'Facebook Ads': 'facebook 7d revenue',
        'moloco_int': 'moloco 7d revenue',
        'bytedanceglobal_int': 'tiktok 7d revenue',
        'snapchat_int': 'snapchat 7d revenue'
    }, inplace=True)

    costMediaDf = revenueMediaDf.pivot(index=['install_day','country'], columns='mediasource', values='cost').reset_index()
    costMediaDf.rename(columns={
        'applovin_int': 'applovin cost',
        'Facebook Ads': 'facebook cost',
        'moloco_int': 'moloco cost',
        'bytedanceglobal_int': 'tiktok cost',
        'snapchat_int': 'snapchat cost'
    }, inplace=True)

    prepareDf = pd.merge(revenueTotalDf, revenue7dMediaDf, on=['install_day','country'], how='left')
    # prepareDf = pd.merge(prepareDf, costMediaDf, on=['install_day','country'], how='left')
    prepareDf = prepareDf.sort_values(by=['install_day','country'], ascending=[False, True])
    prepareDf.fillna(0, inplace=True)

    prepareDf.to_csv(f'/src/data/lw_mohu_bayesian_country_{startDayStr}_{endDayStr}.csv', index=False)
    return prepareDf

def countryMain():
    startDayStr = '20241216'
    endDayStr = '20250518'
    
    prepareDf = bayesianCountryDataPrepare(startDayStr, endDayStr)
    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf['install_week'] = prepareDf['install_day'].dt.strftime('%Y-%W')
    prepareDf = prepareDf.drop(columns=['install_day'])
    prepareWeekDf = prepareDf.groupby(['install_week', 'country']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/countryMain_{startDayStr}_{endDayStr}.csv', index=False)

    countryList = prepareDf['country'].unique()

    # countryList = ['US']

    for country in countryList:
        countryDf = prepareWeekDf[prepareWeekDf['country'] == country].copy()
        facebookRevenueRatio = countryDf['facebook 7d revenue'] / countryDf['total 7d revenue']
        applovinRevenueRatio = countryDf['applovin 7d revenue'] / countryDf['total 7d revenue']
        tiktokRevenueRatio = countryDf['tiktok 7d revenue'] / countryDf['total 7d revenue']
        molocoRevenueRatio = countryDf['moloco 7d revenue'] / countryDf['total 7d revenue']
        snapchatRevenueRatio = countryDf['snapchat 7d revenue'] / countryDf['total 7d revenue']
        ratioDf = pd.DataFrame({
            'facebook_revenue_ratio': facebookRevenueRatio,
            'applovin_revenue_ratio': applovinRevenueRatio,
            'tiktok_revenue_ratio': tiktokRevenueRatio,
            'moloco_revenue_ratio': molocoRevenueRatio,
            'snapchat_revenue_ratio': snapchatRevenueRatio,
        })

        std_devs = ratioDf.std()
        print(f"国家: {country}")
        print("每个媒体的收入占比标准差:")
        print(std_devs)
        print('每个媒体的收入占比均值:')
        print(ratioDf.mean())



        # 计算目前的自然量占比
        organicRevenue = countryDf['total 7d revenue'] - \
                        (countryDf['facebook 7d revenue'] + 
                        countryDf['applovin 7d revenue'] + 
                        countryDf['moloco 7d revenue'] + 
                        countryDf['tiktok 7d revenue'] + 
                        countryDf['snapchat 7d revenue'])
        countryDf['organic_revenue'] = organicRevenue
        organic_ratio = countryDf['organic_revenue'].sum() / countryDf['total 7d revenue'].sum()
        organic_revenue_mean = countryDf['organic_revenue'].mean()
        print(f'国家: {country}, Organic Revenue Ratio: {organic_ratio:.2%}, Organic Revenue Mean: {organic_revenue_mean:.2f}')

        # 制作organicRevenueConfigList
        # 从目前的自然量收入占比 -》 15% 中间分成5个档位
        # 比如目前是 55%，那么就是 55%、45%、35%、25%、15% 5个占比档位
        # 然后再根据收入*自然量收入占比 获得5个档位的mu，sigma保持是mu的10%
        N = 5
        organicRevenueConfigList = []
        step = (organic_ratio - 0.15) / (N - 1)
        target_ratios = [organic_ratio - i * step for i in range(N)]
        print('Target Ratios:', target_ratios)

        for ratio in target_ratios:
            mu = countryDf['total 7d revenue'].mean() * ratio
            sigma = mu * 0.01
            sigma = 100
            organicRevenueConfigList.append({'mu': mu, 'sigma': sigma})

        # organicRevenueConfigList 排序，从小到大
        organicRevenueConfigList = sorted(organicRevenueConfigList, key=lambda x: x['mu'])
            

        print(f"国家: {country}, Organic Revenue Config List: {organicRevenueConfigList}")

        resultDf = pd.DataFrame()

        for organicConfigRevenue in organicRevenueConfigList:
            basic_model = pm.Model()
            with basic_model as model:
                organicRevenue = pm.Normal('organicRevenue', mu=organicConfigRevenue['mu'], sigma=organicConfigRevenue['sigma'])
                facebookX = pm.Normal('facebookX', mu=1, sigma=0.05)
                applovinX = pm.Normal('applovinX', mu=1, sigma=0.05)
                tiktokX = pm.Normal('tiktokX', mu=1, sigma=0.05)
                molocoX = pm.Normal('molocoX', mu=1, sigma=0.05)
                snapchatX = pm.Normal('snapchatX', mu=1, sigma=0.05)
        
                mu = organicRevenue + \
                facebookX * countryDf['facebook 7d revenue'] + \
                applovinX * countryDf['applovin 7d revenue'] + \
                molocoX * countryDf['moloco 7d revenue'] + \
                tiktokX * countryDf['tiktok 7d revenue'] + \
                snapchatX * countryDf['snapchat 7d revenue']
            
                revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=500, observed=countryDf['total 7d revenue'])
                trace = pm.sample(1000)

                # 输出结果
                summary = pm.summary(trace, hdi_prob=0.95)

                retDf,detailDf = cc(summary, countryDf)
                retDf['organicRevenueMu'] = organicConfigRevenue['mu']

                detailDf.to_csv(f'/src/data/detail_{country}_{organicConfigRevenue["mu"]}_{startDayStr}_{endDayStr}.csv', index=False)
                
                resultDf = pd.concat([resultDf, retDf], ignore_index=True)

        print(resultDf)
        resultDf.to_csv(f'/src/data/mohu_result_{country}_{startDayStr}_{endDayStr}.csv', index=False)

def countryMainByDay():
    startDayStr = '20241216'
    endDayStr = '20250518'
    
    prepareDf = bayesianCountryDataPrepare(startDayStr, endDayStr)
    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf.rename(columns={'install_day': 'install_week'}, inplace=True)
    prepareWeekDf = prepareDf.groupby(['install_week', 'country']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/countryMain_{startDayStr}_{endDayStr}.csv', index=False)

    # countryList = prepareDf['country'].unique()
    countryList = ['US', 'JP', 'GCC', 'KR', 'OTHER']
    # prepareWeekDf 中 country 不属于 countryList 的数据 统一为 OTHER
    prepareWeekDf['country'] = prepareWeekDf['country'].apply(lambda x: x if x in countryList else 'OTHER')
    prepareWeekDf = prepareWeekDf.groupby(['install_week', 'country']).sum().reset_index()

    kpiMap = {
        'US': 0.075,
        'JP': 0.085,
        'GCC': 0.055,
        'KR': 0.085,
        'OTHER': 0.1
    }

    for country in countryList:
        countryDf = prepareWeekDf[prepareWeekDf['country'] == country].copy()
        facebookRevenueRatio = countryDf['facebook 7d revenue'] / countryDf['total 7d revenue']
        applovinRevenueRatio = countryDf['applovin 7d revenue'] / countryDf['total 7d revenue']
        tiktokRevenueRatio = countryDf['tiktok 7d revenue'] / countryDf['total 7d revenue']
        molocoRevenueRatio = countryDf['moloco 7d revenue'] / countryDf['total 7d revenue']
        snapchatRevenueRatio = countryDf['snapchat 7d revenue'] / countryDf['total 7d revenue']
        ratioDf = pd.DataFrame({
            'facebook_revenue_ratio': facebookRevenueRatio,
            'applovin_revenue_ratio': applovinRevenueRatio,
            'tiktok_revenue_ratio': tiktokRevenueRatio,
            'moloco_revenue_ratio': molocoRevenueRatio,
            'snapchat_revenue_ratio': snapchatRevenueRatio,
        })

        std_devs = ratioDf.std()
        print(f"国家: {country}")
        print("每个媒体的收入占比标准差:")
        print(std_devs)
        print('每个媒体的收入占比均值:')
        print(ratioDf.mean())
        # continue


        # 计算目前的自然量占比
        organicRevenue = countryDf['total 7d revenue'] - \
                        (countryDf['facebook 7d revenue'] + 
                        countryDf['applovin 7d revenue'] + 
                        countryDf['moloco 7d revenue'] + 
                        countryDf['tiktok 7d revenue'] + 
                        countryDf['snapchat 7d revenue'])
        countryDf['organic_revenue'] = organicRevenue
        organic_ratio = countryDf['organic_revenue'].sum() / countryDf['total 7d revenue'].sum()
        organic_revenue_mean = countryDf['organic_revenue'].mean()
        print(f'国家: {country}, Organic Revenue Ratio: {organic_ratio:.2%}, Organic Revenue Mean: {organic_revenue_mean:.2f}')

        # 制作organicRevenueConfigList
        # 从目前的自然量收入占比 -》 15% 中间分成5个档位
        # 比如目前是 55%，那么就是 55%、45%、35%、25%、15% 5个占比档位
        # 然后再根据收入*自然量收入占比 获得5个档位的mu，sigma保持是mu的10%
        N = 5
        organicRevenueConfigList = []
        step = (organic_ratio - 0.15) / (N - 1)
        target_ratios = [organic_ratio - i * step for i in range(N)]
        print('Target Ratios:', target_ratios)

        for ratio in target_ratios:
            mu = countryDf['total 7d revenue'].mean() * ratio
            sigma = mu * 0.01
            sigma = 10
            organicRevenueConfigList.append({'mu': mu, 'sigma': sigma})

        # organicRevenueConfigList 排序，从小到大
        organicRevenueConfigList = sorted(organicRevenueConfigList, key=lambda x: x['mu'])
            

        print(f"国家: {country}, Organic Revenue Config List: {organicRevenueConfigList}")

        resultDf = pd.DataFrame()

        for organicConfigRevenue in organicRevenueConfigList:
            basic_model = pm.Model()
            with basic_model as model:
                organicRevenue = pm.Normal('organicRevenue', mu=organicConfigRevenue['mu'], sigma=organicConfigRevenue['sigma'])
                facebookX = pm.Normal('facebookX', mu=1, sigma=0.05)
                applovinX = pm.Normal('applovinX', mu=1, sigma=0.05)
                tiktokX = pm.Normal('tiktokX', mu=1, sigma=0.05)
                molocoX = pm.Normal('molocoX', mu=1, sigma=0.05)
                snapchatX = pm.Normal('snapchatX', mu=1, sigma=0.05)
        
                mu = organicRevenue + \
                facebookX * countryDf['facebook 7d revenue'] + \
                applovinX * countryDf['applovin 7d revenue'] + \
                molocoX * countryDf['moloco 7d revenue'] + \
                tiktokX * countryDf['tiktok 7d revenue'] + \
                snapchatX * countryDf['snapchat 7d revenue']
            
                revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=500, observed=countryDf['total 7d revenue'])
                trace = pm.sample(1000)

                # 输出结果
                summary = pm.summary(trace, hdi_prob=0.95)

                countryDf.rename(columns={'install_week': 'install_day'}, inplace=True)

                retDf,detailDf = ccByDay(summary, countryDf)
                retDf['organicRevenueMu'] = organicConfigRevenue['mu']

                detailDf.to_csv(f'/src/data/detailByDay_{country}_{organicConfigRevenue["mu"]}_{startDayStr}_{endDayStr}.csv', index=False)
                
                resultDf = pd.concat([resultDf, retDf], ignore_index=True)

        print(resultDf)
        resultDf.to_csv(f'/src/data/mohuByDay_result_{country}_{startDayStr}_{endDayStr}.csv', index=False)


        kpi = kpiMap[country]
        resultDf['kpi'] = kpi

        resultDf['facebook kpi'] = (kpi/resultDf['facebook X'])
        resultDf['applovin kpi'] = (kpi/resultDf['applovin X'])
        resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
        resultDf['moloco kpi'] = (kpi/resultDf['moloco X'])
        resultDf['snapchat kpi'] = (kpi/resultDf['snapchat X'])

        resultDf = resultDf.rename(
            columns={
                'organicRevenueMu': 'organic_revenue_mu',
                'facebook X': 'facebook_x',
                'applovin X': 'applovin_x',
                'tiktok X': 'tiktok_x',
                'moloco X': 'moloco_x',
                'snapchat X': 'snapchat_x',
                'mape': 'mape',
                'organicRatio': 'organic_ratio',
                'kpi': '7d kpi',
                'facebook kpi': 'facebook_kpi',
                'applovin kpi': 'applovin_kpi',
                'tiktok kpi': 'tiktok_kpi',
                'moloco kpi': 'moloco_kpi',
                'snapchat kpi': 'snapchat_kpi',
            }
        )
        print(resultDf)
        resultDf.to_csv(f'/src/data/resultByDay_{country}_{startDayStr}_{endDayStr}_kpi.csv', index=False)



if __name__ == '__main__':
    # totalMain()
    # totalMainByDay()
    # countryMain()
    countryMainByDay()