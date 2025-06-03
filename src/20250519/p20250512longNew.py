# SKA校准版本 延长数据时间版本，之前是使用11周，改为更久从2024-07-28开始，避开2024-07-06更改CV Mapping影响
# 改为周期版本，定期的更新数据
# 另外使用新的docker container
# 使用docker exec -it pymc python src/20250519/p20250512longNew.py

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

def getRevenueData():
    sql = """
SELECT
	install_day,
	SUM(r24h_usd) AS r24h_usd,
	country
FROM
	(SELECT
		install_day,
		COALESCE(
			SUM(
				CASE
					WHEN event_timestamp - install_timestamp between 0
					and 24 * 3600 THEN revenue_value_usd
					ELSE 0
				END
			),
			0
		) as r24h_usd,
		COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country
	FROM
		rg_bi.ads_lastwar_ios_purchase_adv
		left join cdm_laswwar_country_map on rg_bi.ads_lastwar_ios_purchase_adv.country = cdm_laswwar_country_map.country
	WHERE
		install_day >= '20240701'
	GROUP BY
		install_day,
		countrygroup
	)
GROUP BY
	install_day,
	country
;
    """
    df = execSql(sql)

    df['install_day'] = df['install_day'].astype(str)
    return df

def getSKARevenue():
    sql = """
SELECT
	install_date,
	country,
	media_source,
	SUM(revenue) AS ska_revenue
FROM
	(
		with t1 as(
			SELECT
				REPLACE(install_date, '-', '') AS install_date,
				media_source,
				SUBSTRING_INDEX(
					SUBSTRING_INDEX(ad_network_campaign_name, '_', 2),
					'_',
					-1
				) AS country,
				SUM(skad_revenue) AS revenue
			FROM
				ods_platform_appsflyer_skad_details
			WHERE
				app_id = 'id6448786147'
				AND day > 20240701
				AND event_name = 'af_purchase_update_skan_on'
			GROUP BY
				install_date,
				media_source,
				country
		)
		SELECT
			install_date,
            CASE
			WHEN t1.country IN ('T1', 'T2', 'T3', 'GCC') THEN t1.country
                ELSE COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER')
            END AS country,
			media_source,
			revenue
		FROM
			t1
			left join cdm_laswwar_country_map on t1.country = cdm_laswwar_country_map.country
	)
GROUP BY
	install_date,
	country,
	media_source	
;
    """
    df = execSql(sql)
    df['install_date'] = df['install_date'].astype(str)

    return df

def bayesianTotalDataPrepare():
    mediaList = [
        'googleadwords_int','Facebook Ads','applovin_int','tiktokglobal_int'
    ]

    revenueDf = getRevenueData()
    revenueDf = revenueDf.groupby(['install_day']).agg({'r24h_usd': 'sum'}).reset_index()
    revenueDf = revenueDf.sort_values(by=['install_day'], ascending=[False])

    skaRevenueDf = getSKARevenue()
    skaRevenueDf.loc[skaRevenueDf['media_source'].isin(mediaList) == False, 'media_source'] = 'other'
    skaRevenueDf = skaRevenueDf.groupby(['install_date', 'media_source']).agg({'ska_revenue': 'sum'}).reset_index()
    skaRevenueDf = skaRevenueDf.sort_values(by=['install_date', 'media_source'], ascending=[False, True])
    skaRevenueDf = skaRevenueDf.rename(columns={'install_date': 'install_day'})
    
    skaRevenueDf = skaRevenueDf.pivot(index='install_day', columns='media_source', values='ska_revenue')
    skaRevenueDf.rename(
        columns={
            'Facebook Ads': 'facebook revenue',
            'googleadwords_int': 'google revenue',
            'applovin_int': 'applovin revenue',
            'tiktokglobal_int': 'tiktok revenue',
            'other': 'other revenue'
        }, inplace=True
    )
    prepareDf = pd.merge(skaRevenueDf, revenueDf, how='left', left_on='install_day', right_on='install_day')

    return prepareDf

def cc(summary, prepareDf):
    # 提取参数的均值作为估计值
    organicRevenue_mean = summary.loc['organicRevenue', 'mean']
    facebookX_mean = summary.loc['facebookX', 'mean']
    applovinX_mean = summary.loc['applovinX', 'mean']
    googleX_mean = summary.loc['googleX', 'mean']
    tiktokX_mean = summary.loc['tiktokX', 'mean']
    otherX_mean = summary.loc['otherX', 'mean']

    # 使用参数估计值计算预测值
    predicted_revenue = organicRevenue_mean + \
                        facebookX_mean * prepareDf['facebook revenue'] + \
                        applovinX_mean * prepareDf['applovin revenue'] + \
                        googleX_mean * prepareDf['google revenue'] + \
                        tiktokX_mean * prepareDf['tiktok revenue'] + \
                        otherX_mean * prepareDf['other revenue']
    
    detailDf = pd.DataFrame({
        'install_week': prepareDf['install_week'],
        'Facebook_actual_revenue': prepareDf['facebook revenue'],
        'applovin_actual_revenue': prepareDf['applovin revenue'],
        'google_actual_revenue': prepareDf['google revenue'],
        'tiktok_actual_revenue': prepareDf['tiktok revenue'],
        'other_actual_revenue': prepareDf['other revenue'],
        'Facebook_predicted_revenue': facebookX_mean * prepareDf['facebook revenue'],
        'applovin_predicted_revenue': applovinX_mean * prepareDf['applovin revenue'],
        'google_predicted_revenue': googleX_mean * prepareDf['google revenue'],
        'tiktok_predicted_revenue': tiktokX_mean * prepareDf['tiktok revenue'],
        'other_predicted_revenue': otherX_mean * prepareDf['other revenue'],
        'actual_revenue': prepareDf['r24h_usd'],
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
        'google X': [googleX_mean],
        'tiktok X': [tiktokX_mean],
        'other X': [otherX_mean],
        'mape': [mape],
        'organicRatio': [organicRatio],
    }
    
    retDf = pd.DataFrame(data)
    return retDf,detailDf

def getKPI(dayStr):
    sql = f'''
SELECT 
    roi_h024_best
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
    kpi = df['roi_h024_best'].values[0]
    print(f"KPI for {dayStr}: {kpi}")
    return kpi

def dd():
    resultDf = pd.read_csv('result.csv')
    kpi = 0.0184
    resultDf['facebook kpi'] = (kpi/resultDf['facebook X'])
    resultDf['applovin kpi'] = (kpi/resultDf['applovin X'])
    resultDf['google kpi'] = (kpi/resultDf['google X'])
    resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
    print(resultDf)
    resultDf.to_csv('result_kpi.csv', index=False)

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

    
    prepareDf = bayesianTotalDataPrepare()
    prepareDf = prepareDf[
        (prepareDf['install_day'] >= '20240729') & 
        (prepareDf['install_day'] <= lastSundayStr)
    ]
    # 排除一些SKAN数据不正常的时间段
    exculdeList = [
        {'start': '20250324', 'end': '20250701'},
    ]
    for exclude in exculdeList:
        prepareDf = prepareDf[
            (prepareDf['install_day'] < exclude['start']) | 
            (prepareDf['install_day'] > exclude['end'])
        ]

    prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
    prepareDf['install_week'] = prepareDf['install_day'].dt.strftime('%Y-%W')
    prepareDf = prepareDf.drop(columns=['install_day'])
    prepareWeekDf = prepareDf.groupby(['install_week']).sum().reset_index()
    prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
    prepareWeekDf = prepareWeekDf.reset_index(drop=True)

    prepareWeekDf.to_csv(f'/src/data/prepareWeekDf_SKAN_{todayStr}.csv', index=False)

    organicRevenueConfigList = [
        {'mu':10000, 'sigma':2000},
        {'mu':20000, 'sigma':2000},
        {'mu':30000, 'sigma':2000},
        {'mu':40000, 'sigma':2000},
        {'mu':50000, 'sigma':2000},
        {'mu':60000, 'sigma':2000},
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
            otherX = pm.Normal('otherX', mu=1, sigma=0.1)

            mu = organicRevenue + \
                facebookX * prepareWeekDf['facebook revenue'] + \
                applovinX * prepareWeekDf['applovin revenue'] + \
                googleX * prepareWeekDf['google revenue'] + \
                tiktokX * prepareWeekDf['tiktok revenue'] + \
                otherX * prepareWeekDf['other revenue']
            
            # 似然函数
            revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=3000, observed=prepareWeekDf['r24h_usd'])

            # 采样
            trace = pm.sample(1000)
            
        # 输出结果
        summary = pm.summary(trace, hdi_prob=0.95)

        retDf,detailDf = cc(summary, prepareWeekDf)
        retDf['organicRevenueMu'] = organicConfigRevenue['mu']

        detailDf.to_csv(f'detail_{organicConfigRevenue["mu"]}.csv', index=False)
        
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
    resultDf['google kpi'] = (kpi/resultDf['google X'])
    resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
    resultDf['other kpi'] = (kpi/resultDf['other X'])
    print(resultDf)
    resultDf.to_csv(f'/src/data/result_{todayStr}_kpi.csv', index=False)


    resultDf = resultDf.rename(
        columns={
            'organicRevenueMu': 'organic_revenue_mu',
            'facebook X': 'facebook_x',
            'applovin X': 'applovin_x',
            'google X': 'google_x',
            'tiktok X': 'tiktok_x',
            'other X': 'other_x',
            'mape': 'mape',
            'organicRatio': 'organic_ratio',
            'kpi': '24_hours_kpi',
            'facebook kpi': 'facebook_kpi',
            'applovin kpi': 'applovin_kpi',
            'google kpi': 'google_kpi',
            'tiktok kpi': 'tiktok_kpi',
            'other kpi': 'other_kpi'
        }
    )
    resultDf = resultDf[['organic_revenue_mu','applovin_x','facebook_x','google_x','tiktok_x','other_x','mape','organic_ratio','24_hours_kpi','applovin_kpi','facebook_kpi','google_kpi','tiktok_kpi','other_kpi']]
    createTable()
    deleteTable(todayStr)
    writeTable(resultDf, todayStr)


# 历史数据补充，如果有需要补充的历史数据，调佣这个函数，并且调整时间范围
def historyData():
    startDayStr = '20250303'
    endDayStr = '20250526'

    startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')

    for i in range((endDay - startDay).days + 1):
        day = startDay + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y%m%d')
        # print(dayStr)
        main(dayStr)


from odps.models import Schema, Column, Partition,TableSchema
def createTable():
    o = getO()
    columns = [
        Column(name='organic_revenue_mu', type='double', comment=''),
        Column(name='applovin_x', type='double', comment=''),
        Column(name='facebook_x', type='double', comment=''),
        Column(name='google_x', type='double', comment=''),
        Column(name='tiktok_x', type='double', comment=''),
        Column(name='other_x', type='double', comment=''),
        Column(name='mape', type='double', comment=''),
        Column(name='organic_ratio', type='double', comment=''),
        Column(name='24_hours_kpi', type='double', comment=''),
        Column(name='applovin_kpi', type='double', comment=''),
        Column(name='facebook_kpi', type='double', comment=''),
        Column(name='google_kpi', type='double', comment=''),
        Column(name='tiktok_kpi', type='double', comment=''),
        Column(name='other_kpi', type='double', comment=''),
    ]
    
    partitions = [
        Partition(name='day', type='string', comment='')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('lastwar_ios_skan_kpi_table_20250526', schema, if_not_exists=True)
    return table
    
def deleteTable(dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_skan_kpi_table_20250526')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)

def writeTable(df,dayStr):
    o = getO()
    t = o.get_table('lastwar_ios_skan_kpi_table_20250526')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        writer.write(df)

if __name__ == '__main__':
    # historyData()  # 如果需要补充历史数据，取消注释
    # main('20250526')
    main()
