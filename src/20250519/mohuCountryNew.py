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
    filename = f'/src/data/mohu_country_revenue_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print(f"Loading data from {filename}")
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

from odps.models import Schema, Column, Partition,TableSchema
def createTable():
    o = getO()
    columns = [
        Column(name='country', type='string', comment=''),
        Column(name='organic_revenue_mu', type='double', comment=''),
        Column(name='applovin_x', type='double', comment=''),
        Column(name='facebook_x', type='double', comment=''),
        Column(name='tiktok_x', type='double', comment=''),
        Column(name='moloco_x', type='double', comment=''),
        Column(name='snapchat_x', type='double', comment=''),
        Column(name='mape', type='double', comment=''),
        Column(name='organic_ratio', type='double', comment=''),
        Column(name='7d_kpi', type='double', comment=''),
        Column(name='applovin_kpi', type='double', comment=''),
        Column(name='facebook_kpi', type='double', comment=''),
        Column(name='tiktok_kpi', type='double', comment=''),
        Column(name='moloco_kpi', type='double', comment=''),
        Column(name='snapchat_kpi', type='double', comment=''),
    ]
    
    partitions = [
        Partition(name='day', type='string', comment='')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('lastwar_ios_mohu_country_kpi_table_20250529', schema, if_not_exists=True)
    return table
    
def deleteTable(dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_mohu_country_kpi_table_20250529')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)

def writeTable(df,dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_mohu_country_kpi_table_20250529')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        writer.write(df)


def main(dayStr = None):
    if dayStr is None:
        today = datetime.datetime.now()
    else:
        today = datetime.datetime.strptime(dayStr, '%Y%m%d')

    # 如果不是周一，什么都不做
    if today.weekday() != 0:
        # print("今天不是周一，不执行数据准备。")
        return
    
    todayStr = today.strftime('%Y%m%d')
    # 由于SKA数据比较慢，在周一时上周数据并不完整，所以改为获取上上周数据。
    lastSunday = today - datetime.timedelta(days=8)
    lastSundayStr = lastSunday.strftime('%Y%m%d')
    print(f"今天是周一，执行数据准备，今天日期：{todayStr}，上周日日期：{lastSundayStr}")

    startDayStr = '20241216'
    endDayStr = lastSundayStr
    
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

    resultDfTotal = pd.DataFrame()

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
                'kpi': '7d_kpi',
                'facebook kpi': 'facebook_kpi',
                'applovin kpi': 'applovin_kpi',
                'tiktok kpi': 'tiktok_kpi',
                'moloco kpi': 'moloco_kpi',
                'snapchat kpi': 'snapchat_kpi',
            }
        )
        print(resultDf)
        resultDf.to_csv(f'/src/data/resultByDay_{country}_{startDayStr}_{endDayStr}_kpi.csv', index=False)

        resultDf['country'] = country

        resultDfTotal = pd.concat([resultDfTotal, resultDf], ignore_index=True)


    resultDfTotal = resultDfTotal[['country','organic_revenue_mu','applovin_x','facebook_x','tiktok_x','moloco_x','snapchat_x','mape','organic_ratio','7d_kpi','applovin_kpi','facebook_kpi','tiktok_kpi','moloco_kpi','snapchat_kpi']]
    resultDfTotal.to_csv(f'/src/data/mohuByDayCountry_result_total_{startDayStr}_{endDayStr}.csv', index=False)
    
    createTable()
    deleteTable(todayStr)
    writeTable(resultDfTotal, todayStr)

# 历史数据补充，如果有需要补充的历史数据，调佣这个函数，并且调整时间范围
def historyData():
    startDayStr = '20250101'
    endDayStr = '20250602'

    startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')

    for i in range((endDay - startDay).days + 1):
        day = startDay + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y%m%d')
        # print(dayStr)
        main(dayStr)



if __name__ == '__main__':
    # historyData()  # 如果需要补充历史数据，取消注释
    # main('20250602')
    main()