# 为了 TH 里程碑自动文档

import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 目前没有里程碑表，直接在这写死
def getLichengbeiData():
    # 如果有改变，手动更改这个表，或者改为从数据库中读取
    lichengbei20250426Df = pd.DataFrame({
        'startday': ['20250426'],
        'target_usd': [35000000],

        'iOS_noJP_7ROI': [0.15],        
        'iOS_noJP_Applovin_7DCampaign_7ROI': [0.14],
        'iOS_noJP_Applovin_28DCampaign_7ROI': [0.1],
        'iOS_JP_7ROI': [0.1],
        'iOS_JP_Applovin_7DCampaign_7ROI': [0.11],
        'iOS_JP_Applovin_28DCampaign_7ROI': [0.08],
        
        'Android_noJP_7ROI': [0.15],        
        'Android_noJP_Applovin_7DCampaign_7ROI': [0.14],
        'Android_noJP_Applovin_28DCampaign_7ROI': [0.1],
        'Android_JP_7ROI': [0.1],
        'Android_JP_Applovin_7DCampaign_7ROI': [0.11],
        'Android_JP_Applovin_28DCampaign_7ROI': [0.08],
    })

    return lichengbei20250426Df

def getData(lichengbeiStartDayStr = '20250426'):
    sql = f'''
SELECT
	install_day,
	platform,
	country_group,
	media,
	campaign_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_d7) AS r7usd
FROM
	(
		SELECT
			install_day,
			CASE
				WHEN app_package = 'com.greenmushroom.boomblitz.gp' THEN 'AOS'
				ELSE 'IOS'
			END AS platform,
			CASE
				WHEN country = 'JP' THEN 'JP'
				ELSE 'OTHER'
			END AS country_group,
			CASE
				WHEN mediasource = 'applovin_int' THEN 'applovin_int'
				ELSE 'OTHER'
			END AS media,
			CASE
				WHEN campaign_name LIKE '%Day28%' THEN '28D'
				ELSE '7D'
			END AS campaign_type,
			cost_value_usd,
			revenue_d7
		FROM
			dws_overseas_public_roi
		WHERE
			app = '116'
			AND zone = '0'
			and facebook_segment in ('country', 'N/A')
			AND install_day >= '{lichengbeiStartDayStr}'
	) AS transformed_data
GROUP BY
	install_day,
	platform,
	country_group,
	media,
	campaign_type
;
    '''
    print(sql)
    df = execSql(sql)

    return df

def total():
    lichengbeiDf = getLichengbeiData()
    df = getData()

    groupByPlatformAndCountryDf = df.groupby(['install_day', 'platform', 'country_group']).agg({'cost': 'sum','r7usd': 'sum'}).reset_index()
    # groupByPlatformAndCountryDf = groupByPlatformAndCountryDf.sort_values(by=['platform', 'country_group','install_day'], ascending=[True, True, True])

    # JP + AOS
    JP_AOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'JP') &
        (groupByPlatformAndCountryDf['platform'] == 'AOS')
    ].copy()
    JP_AOSDf = JP_AOSDf.sort_values(by=['install_day'], ascending=[True])
    JP_AOSDf['sum_cost'] = JP_AOSDf['cost'].cumsum()
    JP_AOSDf['sum_r7usd'] = JP_AOSDf['r7usd'].cumsum()
    JP_AOSDf['sum_7roi'] = JP_AOSDf['sum_r7usd'] / JP_AOSDf['sum_cost']

    JP_AOSDf['KPI'] = lichengbeiDf['Android_JP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_AOSDf['sum_cost_ok'] = JP_AOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    # JP_AOSDf.to_csv('/src/data/th_lichengbei_JP_AOS.csv', index=False)

    # JP + IOS
    JP_IOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'JP') &
        (groupByPlatformAndCountryDf['platform'] == 'IOS')
    ].copy()
    JP_IOSDf = JP_IOSDf.sort_values(by=['install_day'], ascending=[True])
    JP_IOSDf['sum_cost'] = JP_IOSDf['cost'].cumsum()
    JP_IOSDf['sum_r7usd'] = JP_IOSDf['r7usd'].cumsum()
    JP_IOSDf['sum_7roi'] = JP_IOSDf['sum_r7usd'] / JP_IOSDf['sum_cost']
    JP_IOSDf['KPI'] = lichengbeiDf['iOS_JP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_IOSDf['sum_cost_ok'] = JP_IOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    # JP_IOSDf.to_csv('/src/data/th_lichengbei_JP_IOS.csv', index=False)

    # NOJP + AOS
    NOJP_AOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'OTHER') &
        (groupByPlatformAndCountryDf['platform'] == 'AOS')
    ].copy()
    NOJP_AOSDf = NOJP_AOSDf.sort_values(by=['install_day'], ascending=[True])
    NOJP_AOSDf['sum_cost'] = NOJP_AOSDf['cost'].cumsum()
    NOJP_AOSDf['sum_r7usd'] = NOJP_AOSDf['r7usd'].cumsum()
    NOJP_AOSDf['sum_7roi'] = NOJP_AOSDf['sum_r7usd'] / NOJP_AOSDf['sum_cost']
    NOJP_AOSDf['KPI'] = lichengbeiDf['Android_noJP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_AOSDf['sum_cost_ok'] = NOJP_AOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    # NOJP_AOSDf.to_csv('/src/data/th_lichengbei_NOJP_AOS.csv', index=False)

    # NOJP + IOS
    NOJP_IOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'OTHER') &
        (groupByPlatformAndCountryDf['platform'] == 'IOS')
    ].copy()
    NOJP_IOSDf = NOJP_IOSDf.sort_values(by=['install_day'], ascending=[True])
    NOJP_IOSDf['sum_cost'] = NOJP_IOSDf['cost'].cumsum()
    NOJP_IOSDf['sum_r7usd'] = NOJP_IOSDf['r7usd'].cumsum()
    NOJP_IOSDf['sum_7roi'] = NOJP_IOSDf['sum_r7usd'] / NOJP_IOSDf['sum_cost']
    NOJP_IOSDf['KPI'] = lichengbeiDf['iOS_noJP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_IOSDf['sum_cost_ok'] = NOJP_IOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    # NOJP_IOSDf.to_csv('/src/data/th_lichengbei_NOJP_IOS.csv', index=False)

    # 合并
    allDf = pd.concat([JP_AOSDf, JP_IOSDf, NOJP_AOSDf, NOJP_IOSDf], ignore_index=True)
    allDf = allDf.groupby(['install_day']).agg({
        'sum_cost_ok': 'sum',
        'sum_cost': 'sum'
    }).reset_index()

    allDf.to_csv('/src/data/th_lichengbei_all.csv', index=False)

    allDf['install_day'] = pd.to_datetime(allDf['install_day'], format='%Y%m%d')
    lichengbeiCost = lichengbeiDf['target_usd'].values[0]

    today = datetime.datetime.now()
    # 计算满7日数据截止日期
    full7dayEndDate = today - datetime.timedelta(days=8)

    print('today:', today.strftime('%Y%m%d'),' full7dayEndDate:', full7dayEndDate.strftime('%Y%m%d'))

    # 画图
    # install_day 是横坐标，sum_cost 与 sum_cost_ok 是纵坐标
    # lichengbeiCost 画一条横线
    # full7dayEndDate 画一条竖线
    # 保存到 /src/data/th_lichengbei_all.png

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.plot(allDf['install_day'], allDf['sum_cost'], label='sum cost', color='blue')
    ax.plot(allDf['install_day'], allDf['sum_cost_ok'], label='sum cost ok', color='green')
    ax.axhline(y=lichengbeiCost, color='red', label='KPI Cost')
    ax.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')

    ax.set_xlabel('Install Day')
    ax.set_ylabel('Cost')
    ax.set_title('milestones Cost Analysis')

    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig('/src/data/th_lichengbei_all.png')
    


def main():
    lichengbeiDf = getLichengbeiData()


if __name__ == '__main__':
    # main()
    total()
