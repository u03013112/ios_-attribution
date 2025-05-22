import os
import arviz as az
import pandas as pd
import pymc as pm
import numpy as np
import matplotlib.pyplot as plt

def calculate_mape(summary, prepareDf):
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
    
    reslutDf = pd.DataFrame({
        'install_week': prepareDf['install_week'],
        'actual_revenue': prepareDf['r24h_usd'],
        'predicted_revenue': predicted_revenue,
        'organicRevenue_mean': organicRevenue_mean,
    })

    reslutDf.to_csv('result.csv', index=False)

    # 计算绝对百分比误差
    absolute_percentage_error = np.abs((reslutDf['actual_revenue'] - reslutDf['predicted_revenue']) / reslutDf['actual_revenue']) * 100
    
    # 计算 MAPE
    mape = np.mean(absolute_percentage_error)
    print(f'MAPE: {mape:.2f}%')

    # 计算自然量占比
    organicRatio = reslutDf['organicRevenue_mean'].sum() / reslutDf['predicted_revenue'].sum() * 100
    print(f'Organic Ratio: {organicRatio:.2f}%')



    # 绘图
    plt.figure(figsize=(10, 6))
    plt.plot(prepareDf['install_week'], prepareDf['r24h_usd'], label='Actual Revenue', marker='o')
    plt.plot(prepareDf['install_week'], predicted_revenue, label='Predicted Revenue', marker='x')
    plt.xlabel('Install Week')
    plt.ylabel('Revenue')
    plt.title('Actual vs Predicted Revenue')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    # plt.show()
    plt.savefig("actual_vs_predicted_revenue.png")
    plt.close()

    return mape

def bayesianTotalModel():
    prepareDf = pd.read_csv('lw_20250519_bayesian_total_week.csv')
    print(prepareDf.head(10))
    resltStr = '''
  install_week  Facebook Ads  applovin_int  googleadwords_int  tiktokglobal_int       r24h_usd
0      2025-11     19860.115     23666.630           5889.965          9870.370   98913.239587
1      2025-10     22868.230     23166.460           6615.235          9420.755  106385.634296
2      2025-09     21773.865     36626.655           9443.705         12489.645  116772.129025
3      2025-08     20753.165     28980.855           9195.515          7819.345  114862.213507
4      2025-07     21982.545     31503.475           5018.390          6850.420  108558.163723
5      2025-06     17103.140     26708.305           6389.295          6109.690   98971.553856
6      2025-05     21322.295     30330.210           8204.395          7101.980  112861.952936
7      2025-04     28432.245     33020.935           9387.680          9656.175  126651.792936
8      2025-03     22449.105     33647.815           9723.065         11203.460  123749.763272
9      2025-02     26868.735     31113.130           6728.745         12006.395  109998.889714
'''

    shape_natural = 0.2711213446744097
    scale_natural = 9718.87231171613

    basic_model = pm.Model()
    # 贝叶斯模型
    with basic_model as model:
        # 先验分布
        # organicRevenue = pm.Lognormal('organicRevenue', mu=np.log(scale_natural), sd=shape_natural)
        # organicRevenue = pm.Normal('organicRevenue', mu=30000, sigma=3000)
        organicRevenue = pm.Normal('organicRevenue', mu=10000, sigma=2000)
        # organicRevenue = pm.Lognormal(
        #     'organicRevenue', 
        #     mu=np.log(scale_natural), 
        #     sigma = shape_natural
        # )

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
    print(summary)
    
    calculate_mape(summary, prepareDf)
    # print(f'MAPE:{calculate_mape(summary, prepareDf):.2f}%')

    az.plot_trace(trace, combined=True)
    plt.tight_layout()
    # plt.show()
    plt.savefig("trace_plot.png")

if __name__ == '__main__':
    bayesianTotalModel()