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

def getKPI(dayStr):
    sql = f'''
SELECT 
    roi_007_best
FROM rg_bi.ads_predict_base_roi_day1_window_multkey
    WHERE type = 'id6448786147'
    AND country = 'ALL'
    AND end_date = '{dayStr}'
;
    '''
    df = execSql(sql)
    if df.empty:
        print(f"No ROI data found for {dayStr}.")
        return None
    kpi = df['roi_007_best'].values[0]
    print(f"KPI for {dayStr}: {kpi}")
    return kpi

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
    table = o.create_table('lastwar_ios_mohu_kpi_table_20250529', schema, if_not_exists=True)
    return table
    
def deleteTable(dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_mohu_kpi_table_20250529')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)

def writeTable(df,dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_mohu_kpi_table_20250529')
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
    
    prepareDf = bayesianTotalDataPrepare(startDayStr, endDayStr)

    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf.rename(columns={'install_day': 'install_week'}, inplace=True)
    prepareWeekDf = prepareDf.groupby(['install_week']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/prepareDayDf_mohu_{todayStr}.csv', index=False)

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
        
        resultDf = pd.concat([resultDf, retDf], ignore_index=True)

    print(resultDf)
    resultDf.to_csv(f'/src/data/result_{todayStr}.csv', index=False)

    kpi = getKPI(lastSundayStr)
    if kpi is None:
        print(f"No KPI found for {lastSundayStr}.")
        return
    
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

    resultDf.to_csv(f'/src/data/resultByDay_{startDayStr}_{endDayStr}_kpi.csv', index=False)

    resultDf = resultDf[['organic_revenue_mu','applovin_x','facebook_x','tiktok_x','moloco_x','snapchat_x','mape','organic_ratio','7d_kpi','applovin_kpi','facebook_kpi','tiktok_kpi','moloco_kpi','snapchat_kpi']]

    createTable()
    deleteTable(todayStr)
    writeTable(resultDf, todayStr)

# 历史数据补充，如果有需要补充的历史数据，调佣这个函数，并且调整时间范围
def historyData():
    startDayStr = '20250101'
    endDayStr = '20250526'

    startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')

    for i in range((endDay - startDay).days + 1):
        day = startDay + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y%m%d')
        # print(dayStr)
        main(dayStr)

if __name__ == '__main__':
    # historyData()  # 如果需要补充历史数据，取消注释
    # main('20250526')
    main()

