# SKA校准版本 延长数据时间版本，之前是使用11周，改为更久从2024-07-28开始，避开2024-07-06更改CV Mapping影响


import os
import arviz as az
import pandas as pd
import pymc as pm
import numpy as np
import matplotlib.pyplot as plt
import sys
sys.path.append('../../')
from src.maxCompute import execSql

def getCostData():
    filename = 'lw_20240729_costdata.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = """
select
	install_day,
	mediasource,
	country,
	sum(usd) as usd
from
	(
		select
			install_day,
			mediasource,
			COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country,
			sum(cost_value_usd) as usd
		from
			dws_overseas_public_roi
			left join cdm_laswwar_country_map on dws_overseas_public_roi.country = cdm_laswwar_country_map.country
		where
			app = '502'
			and facebook_segment in ('country', 'N/A')
			and app_package = 'id6448786147'
            and install_day >= '2024-07-01'
		group by
			install_day,
			mediasource,
			countrygroup
	)
group by
	install_day,
	mediasource,
	country
order by
	install_day desc;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def getRevenueData():
    filename = 'lw_20240729_revenuedata.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
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
        df.to_csv(filename, index=False)

    return df

def getSKARevenue():
    filename = 'lw_20240729_ska_revenue.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
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
        df.to_csv(filename, index=False)
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
    skaRevenueDf = skaRevenueDf[skaRevenueDf['install_day'] >= 20240729]
    # # 只保留指定的media_source，其他媒体暂时不要
    # skaRevenueDf = skaRevenueDf[skaRevenueDf['media_source'].isin(mediaList)]

    # 将skaRevenueDf 转成 install_day，googleadwords_int revenue，Facebook Ads revenue，applovin_int revenue，tiktokglobal_int revenue的格式
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

    costDf = getCostData()
    costDf.replace({
        'mediasource': {
            'bytedanceglobal_int':'tiktokglobal_int',
        }
    }, inplace=True)
    costDf.loc[costDf['mediasource'].isin(mediaList) == False, 'mediasource'] = 'other'
    costDf = costDf.groupby(['install_day','mediasource']).sum().reset_index()
    costDf = costDf.pivot(index='install_day', columns='mediasource', values='usd')
    costDf.rename(
        columns={
            'Facebook Ads': 'facebook cost',
            'googleadwords_int': 'google cost',
            'applovin_int': 'applovin cost',
            'tiktokglobal_int': 'tiktok cost',
            'other': 'other cost'
        }, inplace=True
    )

    prepareDf = pd.merge(prepareDf, costDf, how='left', left_on='install_day', right_on='install_day')

    return prepareDf
def bayesianTotalData():
    filename = 'lw_20240729_bayesian_total_week.csv'
    if os.path.exists(filename):
        prepareDf = pd.read_csv(filename)
    else:
        prepareDf = bayesianTotalDataPrepare()
        prepareDf = prepareDf[(prepareDf['install_day'] >= 20240729) & (prepareDf['install_day'] <= 20250323)]
        prepareDf['install_day'] = pd.to_datetime(prepareDf['install_day'].astype(str), format='%Y%m%d')
        prepareDf['install_week'] = prepareDf['install_day'].dt.strftime('%Y-%W')
        prepareDf = prepareDf.drop(columns=['install_day'])
        prepareWeekDf = prepareDf.groupby(['install_week']).sum().reset_index()
        prepareWeekDf = prepareWeekDf.sort_values(by=['install_week'], ascending=[False])
        prepareWeekDf = prepareWeekDf.reset_index(drop=True)
        # print(prepareWeekDf.head(10))
        
        prepareWeekDf.to_csv(filename, index=False)
    
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
        'other X': [f'{otherX_mean:.2f}'],
        'mape': [f'{mape:.2f}%'],
        'organicRatio': [f'{organicRatio:.2f}%'],
    }
    
    retDf = pd.DataFrame(data)
    return retDf,detailDf

def aa(prepareDf):
    df = prepareDf.copy()
    # install_week,facebook revenue,applovin revenue,google revenue,other revenue,tiktok revenue,r24h_usd,facebook cost,applovin cost,google cost,other cost,tiktok cost
    df['cost'] = df['facebook cost'] + df['applovin cost'] + df['google cost'] + df['other cost'] + df['tiktok cost']
    df['roi24h'] = df['r24h_usd'] / df['cost']
    df['facebook_roi'] = df['facebook revenue'] / df['facebook cost']
    df['applovin_roi'] = df['applovin revenue'] / df['applovin cost']
    df['google_roi'] = df['google revenue'] / df['google cost']
    df['tiktok_roi'] = df['tiktok revenue'] / df['tiktok cost']
    df['other_roi'] = df['other revenue'] / df['other cost']

    kpi = 0.0184
    # 达标周的各媒体roi均值
    kpiDf = df[df['roi24h'] >= kpi].copy()
    print('达标周/所有周：',f'{len(kpiDf)} / {len(df)}')
    
    return kpiDf[['facebook_roi', 'applovin_roi', 'google_roi', 'tiktok_roi', 'other_roi','roi24h']].mean()

def bayesianTotalModel():
    prepareDf = bayesianTotalData()
    kpiDf = aa(prepareDf)
    print('达标周的各媒体roi均值：')
    print(kpiDf)

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
                facebookX * prepareDf['facebook revenue'] + \
                applovinX * prepareDf['applovin revenue'] + \
                googleX * prepareDf['google revenue'] + \
                tiktokX * prepareDf['tiktok revenue'] + \
                otherX * prepareDf['other revenue']
            
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

def dd():
    resultDf = pd.read_csv('result.csv')
    kpi = 0.0184
    resultDf['facebook kpi'] = (kpi/resultDf['facebook X'])
    resultDf['applovin kpi'] = (kpi/resultDf['applovin X'])
    resultDf['google kpi'] = (kpi/resultDf['google X'])
    resultDf['tiktok kpi'] = (kpi/resultDf['tiktok X'])
    print(resultDf)
    resultDf.to_csv('result_kpi.csv', index=False)

if __name__ == '__main__':
    bayesianTotalModel()
    dd()
