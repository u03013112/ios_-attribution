import os
os.environ['PYTENSOR_FLAGS'] = 'optimizer=None'

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import pymc as pm
import arviz as az

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO
def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/lw_revenue_mediasource_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    mediasource,
    sum(revenue_d7) as revenue_d7
from
    dws_overseas_public_roi
where
    app = '502'
    and app_package = 'com.fun.lastwar.gp'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    mediasource
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

def main():
    df = getRevenueData('20250101', '20250601')
    organicDf = df[df['mediasource'] == 'Organic'].copy()
    organicDf = organicDf.sort_values(by='install_day')

    organicDf['install_day'] = pd.to_datetime(organicDf['install_day'], format='%Y%m%d')
    organicDf = organicDf[['install_day', 'revenue_d7']]

    #
    with pm.Model() as model:
        sigma_obs = pm.Exponential('sigma_obs', 1.0)
        sigma_trend = pm.Exponential('sigma_trend', 0.5)
        nu = pm.Exponential('nu', 1/10)

        n_days = organicDf.shape[0]

        # 手动指定初始分布
        init_dist = pm.Normal.dist(mu=organicDf['revenue_d7'].iloc[0], sigma=10)
        trend = pm.GaussianRandomWalk('trend', sigma=sigma_trend, shape=n_days, init_dist=init_dist)

        obs = pm.StudentT('obs', mu=trend, sigma=sigma_obs, nu=nu, observed=organicDf['revenue_d7'])

        trace = pm.sample(1000, tune=1000, target_accept=0.95, random_seed=42)


    # 3. 结果可视化
    # 提取趋势的后验均值和置信区间
    trend_posterior = az.summary(trace, var_names=['trend'], hdi_prob=0.95)
    trend_mean = trend_posterior['mean'].values
    trend_lower = trend_posterior['hdi_2.5%'].values
    trend_upper = trend_posterior['hdi_97.5%'].values

    # 绘图
    plt.figure(figsize=(12, 6))
    plt.plot(organicDf['install_day'], organicDf['revenue_d7'], label='Observed Income', color='blue', alpha=0.5)
    plt.plot(organicDf['install_day'], trend_mean, label='Estimated Trend', color='red')
    plt.fill_between(organicDf['install_day'], trend_lower, trend_upper, color='red', alpha=0.3, label='95% Credible Interval')
    plt.title('Bayesian Trend Estimation of Server Income')
    plt.xlabel('Date')
    plt.ylabel('Income')
    plt.legend()
    plt.grid(alpha=0.3)

    # 保存图片
    plt.tight_layout()
    plt.savefig('/src/data/organic_revenue_trend.png', dpi=200)


if __name__ == '__main__':
    main()