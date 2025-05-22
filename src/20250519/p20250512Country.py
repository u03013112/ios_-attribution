# SKA校准版本1

import os
import arviz as az
import pandas as pd
import pymc as pm
import numpy as np
import matplotlib.pyplot as plt

def cc(summary, prepareDf):
    # 提取参数的均值作为估计值
    organicRevenue_mean = summary.loc['organicRevenue', 'mean']
    facebookX_mean = summary.loc['facebookX', 'mean']
    applovinX_mean = summary.loc['applovinX', 'mean']
    googleX_mean = summary.loc['googleX', 'mean']
    tiktokX_mean = summary.loc['tiktokX', 'mean']
    # print(f"organicRevenue_mean: {organicRevenue_mean}")
    # print(f"facebookX_mean: {facebookX_mean}")
    # print(f"applovinX_mean: {applovinX_mean}")
    # print(f"googleX_mean: {googleX_mean}")
    # print(f"tiktokX_mean: {tiktokX_mean}")

    # 使用参数估计值计算预测值
    predicted_revenue = organicRevenue_mean + \
                        facebookX_mean * prepareDf['Facebook Ads'] + \
                        applovinX_mean * prepareDf['applovin_int'] + \
                        googleX_mean * prepareDf['googleadwords_int'] + \
                        tiktokX_mean * prepareDf['tiktokglobal_int']
    
    detailDf = pd.DataFrame({
        'install_week': prepareDf['install_week'],
        'Facebook_actual_revenue': prepareDf['Facebook Ads'],
        'applovin_actual_revenue': prepareDf['applovin_int'],
        'google_actual_revenue': prepareDf['googleadwords_int'],
        'tiktok_actual_revenue': prepareDf['tiktokglobal_int'],
        'Facebook_predicted_revenue': facebookX_mean * prepareDf['Facebook Ads'],
        'applovin_predicted_revenue': applovinX_mean * prepareDf['applovin_int'],
        'google_predicted_revenue': googleX_mean * prepareDf['googleadwords_int'],
        'tiktok_predicted_revenue': tiktokX_mean * prepareDf['tiktokglobal_int'],
        'actual_revenue': prepareDf['r24h_usd'],
        'predicted_revenue': predicted_revenue,
        'organicRevenue_predicted': organicRevenue_mean,
    })

    # 计算绝对百分比误差
    absolute_percentage_error = np.abs((detailDf['actual_revenue'] - detailDf['predicted_revenue']) / detailDf['actual_revenue']) * 100
    
    # 计算 MAPE
    mape = np.mean(absolute_percentage_error)
    # print(f'MAPE: {mape:.2f}%')

    # 计算自然量占比
    organicRatio = detailDf['organicRevenue_predicted'].sum() / detailDf['predicted_revenue'].sum() * 100
    # print(f'Organic Ratio: {organicRatio:.2f}%')


    data = {
        'organicRevenue_predicted': [f'{organicRevenue_mean:.2f}'],
        'facebook X': [f'{facebookX_mean:.2f}'],
        'applovin X': [f'{applovinX_mean:.2f}'],
        'google X': [f'{googleX_mean:.2f}'],
        'tiktok X': [f'{tiktokX_mean:.2f}'],
        'mape': [f'{mape:.2f}%'],
        'organicRatio': [f'{organicRatio:.2f}%'],
    }

    
    retDf = pd.DataFrame(data)
    return retDf,detailDf

def bayesianTotalModel():
    prepareDf = pd.read_csv('lw_20250519_bayesian_total_week.csv')
    

    organicRevenueConfigList = [
        {'mu':10000, 'sigma':2000},
        {'mu':20000, 'sigma':2000},
        {'mu':25000, 'sigma':2000},
        {'mu':30000, 'sigma':2000},
        {'mu':40000, 'sigma':2000}
    ]

    resultDf = pd.DataFrame()

    for organicConfigRevenue in organicRevenueConfigList:
        basic_model = pm.Model()

        # 贝叶斯模型
        with basic_model as model:
            # 先验分布
            organicRevenue = pm.Normal('organicRevenue', mu=organicConfigRevenue['mu'], sigma=organicConfigRevenue['sigma'])
        
            facebookX = pm.Normal('facebookX', mu=1, sigma=0.1)
            applovinX = pm.Normal('applovinX', mu=1, sigma=0.1)
            googleX = pm.Normal('googleX', mu=1, sigma=0.1)
            tiktokX = pm.Normal('tiktokX', mu=1, sigma=0.1)

            mu = organicRevenue + \
                facebookX * prepareDf['Facebook Ads'] + \
                applovinX * prepareDf['applovin_int'] + \
                googleX * prepareDf['googleadwords_int'] + \
                tiktokX * prepareDf['tiktokglobal_int']
            
            # 似然函数
            revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=3000, observed=prepareDf['r24h_usd'])

            # 采样
            trace = pm.sample(1000)
            
        # 输出结果
        summary = pm.summary(trace, hdi_prob=0.95)

        retDf,detailDf = cc(summary, prepareDf)

        detailDf.to_csv(f'detail_{organicConfigRevenue["mu"]}.csv', index=False)
        
        resultDf = pd.concat([resultDf, retDf], ignore_index=True)

    resultDf.to_csv('result.csv', index=False)
    print(resultDf)

if __name__ == '__main__':
    bayesianTotalModel()
