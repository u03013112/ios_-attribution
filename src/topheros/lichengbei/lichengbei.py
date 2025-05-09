# 为了 TH 里程碑自动文档

import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from PIL import Image

import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 目前没有里程碑表，直接在这写死
def getmilestonesData():
    # 如果有改变，手动更改这个表，或者改为从数据库中读取
    milestones20250426Df = pd.DataFrame({
        # 'startday': ['20250426'],
        # for test
        'startday': ['20250326'],
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

    return milestones20250426Df

def getData(dayStr,milestonesStartDayStr = '20250426'):
    # today = datetime.datetime.now()
    # todayStr = today.strftime('%Y%m%d')
    todayStr = dayStr

    filename = f'/src/data/th_milestones_data_{milestonesStartDayStr}_{todayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
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
                WHEN app_package = 'id6450953550' THEN 'IOS'
				ELSE 'OTHER'
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
			AND install_day BETWEEN '{milestonesStartDayStr}' AND '{todayStr}'
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
        df.to_csv(filename, index=False)

    return df

def totalAndPlatformCountry(dayStr,reportData):
    # today = datetime.datetime.now()
    # today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    today = datetime.datetime.strptime(dayStr, '%Y%m%d')
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 计算满7日数据截止日期
    full7dayEndDate = today - datetime.timedelta(days=8)

    milestonesDf = getmilestonesData()
    startdayStr = milestonesDf['startday'].values[0]
    startDay = datetime.datetime.strptime(startdayStr, '%Y%m%d')
    reportData['startDay'] = startdayStr
    reportData['endDay'] = full7dayEndDate.strftime('%Y%m%d')
    days = (full7dayEndDate - startDay).days
    reportData['days'] = days

    print('today:', today.strftime('%Y%m%d'),' full7dayEndDate:', full7dayEndDate.strftime('%Y%m%d'))

    df = getData(dayStr,startdayStr)
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    # 修正一些错误数据，install_day 大于等于今天的，去掉
    df = df[df['install_day'] < today]

    groupByPlatformAndCountryDf = df.groupby(['install_day', 'platform', 'country_group']).agg({'cost': 'sum','r7usd': 'sum'}).reset_index()
    # groupByPlatformAndCountryDf = groupByPlatformAndCountryDf.sort_values(by=['platform', 'country_group','install_day'], ascending=[True, True, True])

    # JP + AOS
    JP_AOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'JP') &
        (groupByPlatformAndCountryDf['platform'] == 'AOS')
    ].copy()
    JP_AOSDf = JP_AOSDf.sort_values(by=['install_day'], ascending=[True])
    JP_AOSDf['7roi'] = JP_AOSDf['r7usd'] / JP_AOSDf['cost']
    JP_AOSDf['sum_cost'] = JP_AOSDf['cost'].cumsum()
    JP_AOSDf['sum_r7usd'] = JP_AOSDf['r7usd'].cumsum()
    JP_AOSDf['sum_7roi'] = JP_AOSDf['sum_r7usd'] / JP_AOSDf['sum_cost']

    JP_AOSDf['KPI'] = milestonesDf['Android_JP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_AOSDf['sum_cost_ok'] = JP_AOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_AOSDf.to_csv('/src/data/th_milestones_JP_AOS.csv', index=False)
    reportData['JP_AOSDf'] = JP_AOSDf

    JP_AOSDf['install_day'] = pd.to_datetime(JP_AOSDf['install_day'], format='%Y%m%d')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_AOSDf['install_day'], JP_AOSDf['KPI'], label='KPI', color='red')
    ax1.plot(JP_AOSDf['install_day'], JP_AOSDf['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS JP 7ROI')
    ax1.legend()
    
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_AOSDf['install_day'], JP_AOSDf['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_AOSDf['install_day'], JP_AOSDf['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_AOSDf['install_day'], JP_AOSDf['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS JP daily cost and Sum Cost ok')
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_AOS.png')
    plt.close()

    # JP + IOS
    JP_IOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'JP') &
        (groupByPlatformAndCountryDf['platform'] == 'IOS')
    ].copy()
    JP_IOSDf = JP_IOSDf.sort_values(by=['install_day'], ascending=[True])
    JP_IOSDf['7roi'] = JP_IOSDf['r7usd'] / JP_IOSDf['cost']
    JP_IOSDf['sum_cost'] = JP_IOSDf['cost'].cumsum()
    JP_IOSDf['sum_r7usd'] = JP_IOSDf['r7usd'].cumsum()
    JP_IOSDf['sum_7roi'] = JP_IOSDf['sum_r7usd'] / JP_IOSDf['sum_cost']
    JP_IOSDf['KPI'] = milestonesDf['iOS_JP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_IOSDf['sum_cost_ok'] = JP_IOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_IOSDf.to_csv('/src/data/th_milestones_JP_IOS.csv', index=False)
    reportData['JP_IOSDf'] = JP_IOSDf

    JP_IOSDf['install_day'] = pd.to_datetime(JP_IOSDf['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_IOSDf['install_day'], JP_IOSDf['KPI'], label='KPI', color='red')
    ax1.plot(JP_IOSDf['install_day'], JP_IOSDf['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS JP 7ROI')
    ax1.legend()
    
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_IOSDf['install_day'], JP_IOSDf['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_IOSDf['install_day'], JP_IOSDf['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_IOSDf['install_day'], JP_IOSDf['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS JP daily cost and Sum Cost ok')
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_IOS.png')
    plt.close()

    # NOJP + AOS
    NOJP_AOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'OTHER') &
        (groupByPlatformAndCountryDf['platform'] == 'AOS')
    ].copy()
    NOJP_AOSDf = NOJP_AOSDf.sort_values(by=['install_day'], ascending=[True])
    NOJP_AOSDf['7roi'] = NOJP_AOSDf['r7usd'] / NOJP_AOSDf['cost']
    NOJP_AOSDf['sum_cost'] = NOJP_AOSDf['cost'].cumsum()
    NOJP_AOSDf['sum_r7usd'] = NOJP_AOSDf['r7usd'].cumsum()
    NOJP_AOSDf['sum_7roi'] = NOJP_AOSDf['sum_r7usd'] / NOJP_AOSDf['sum_cost']
    NOJP_AOSDf['KPI'] = milestonesDf['Android_noJP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_AOSDf['sum_cost_ok'] = NOJP_AOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_AOSDf.to_csv('/src/data/th_milestones_NOJP_AOS.csv', index=False)
    reportData['NOJP_AOSDf'] = NOJP_AOSDf

    NOJP_AOSDf['install_day'] = pd.to_datetime(NOJP_AOSDf['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_AOSDf['install_day'], NOJP_AOSDf['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_AOSDf['install_day'], NOJP_AOSDf['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS NOJP 7ROI')
    ax1.legend()
    
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_AOSDf['install_day'], NOJP_AOSDf['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_AOSDf['install_day'], NOJP_AOSDf['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_AOSDf['install_day'], NOJP_AOSDf['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS NOJP daily cost and Sum Cost ok')
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_AOS.png')
    plt.close()

    # NOJP + IOS
    NOJP_IOSDf = groupByPlatformAndCountryDf[
        (groupByPlatformAndCountryDf['country_group'] == 'OTHER') &
        (groupByPlatformAndCountryDf['platform'] == 'IOS')
    ].copy()
    NOJP_IOSDf = NOJP_IOSDf.sort_values(by=['install_day'], ascending=[True])
    NOJP_IOSDf['7roi'] = NOJP_IOSDf['r7usd'] / NOJP_IOSDf['cost']
    NOJP_IOSDf['sum_cost'] = NOJP_IOSDf['cost'].cumsum()
    NOJP_IOSDf['sum_r7usd'] = NOJP_IOSDf['r7usd'].cumsum()
    NOJP_IOSDf['sum_7roi'] = NOJP_IOSDf['sum_r7usd'] / NOJP_IOSDf['sum_cost']
    NOJP_IOSDf['KPI'] = milestonesDf['iOS_noJP_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_IOSDf['sum_cost_ok'] = NOJP_IOSDf.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_IOSDf.to_csv('/src/data/th_milestones_NOJP_IOS.csv', index=False)
    reportData['NOJP_IOSDf'] = NOJP_IOSDf

    NOJP_IOSDf['install_day'] = pd.to_datetime(NOJP_IOSDf['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_IOSDf['install_day'], NOJP_IOSDf['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_IOSDf['install_day'], NOJP_IOSDf['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS NOJP 7ROI')
    ax1.legend()
    
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_IOSDf['install_day'], NOJP_IOSDf['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_IOSDf['install_day'], NOJP_IOSDf['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_IOSDf['install_day'], NOJP_IOSDf['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS NOJP daily cost and Sum Cost ok')
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_IOS.png')
    plt.close()

    # 合并
    allDf = pd.concat([JP_AOSDf, JP_IOSDf, NOJP_AOSDf, NOJP_IOSDf], ignore_index=True)
    allDf = allDf.groupby(['install_day']).agg({
        'cost': 'sum',
        'sum_cost_ok': 'sum',
        'sum_cost': 'sum'
    }).reset_index()

    allDf.to_csv('/src/data/th_milestones_all.csv', index=False)
    reportData['allDf'] = allDf

    allDf['install_day'] = pd.to_datetime(allDf['install_day'], format='%Y%m%d')
    milestonesCost = milestonesDf['target_usd'].values[0]
    reportData['milestonesTargetUsd'] = milestonesCost

    # 画图
    # install_day 是横坐标，sum_cost 与 sum_cost_ok 是纵坐标
    # milestonesCost 画一条横线
    # full7dayEndDate 画一条竖线
    # 保存到 /src/data/th_milestones_all.png

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.plot(allDf['install_day'], allDf['sum_cost'], label='Sum Cost', color='blue')
    ax.plot(allDf['install_day'], allDf['sum_cost_ok'], label='Sum Cost ok', color='green')
    ax.axhline(y=milestonesCost, color='red', label='KPI Cost')
    ax.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')

    ax.set_xlabel('Install Day')
    ax.set_ylabel('Cost')
    ax.ticklabel_format(style='plain', axis='y')
    ax.set_title('totalAndPlatformCountry milestones cost')

    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig('/src/data/th_milestones_all.png')
    plt.close()
    
def applovin(dayStr,reportData):
    # today = datetime.datetime.now()
    # today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    today = datetime.datetime.strptime(dayStr, '%Y%m%d')
    # today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 计算满7日数据截止日期
    full7dayEndDate = today - datetime.timedelta(days=8)
    

    print('today:', today.strftime('%Y%m%d'),' full7dayEndDate:', full7dayEndDate.strftime('%Y%m%d'))

    milestonesDf = getmilestonesData()
    df = getData(dayStr,milestonesDf['startday'].values[0])
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    # 修正一些错误数据，install_day 大于等于今天的，去掉
    df = df[df['install_day'] < today]

    applovinDf = df[
        (df['media'] == 'applovin_int')
    ].copy()

    # JP + AOS + 7D
    JP_AOS7Df = applovinDf[
        (applovinDf['country_group'] == 'JP') &
        (applovinDf['platform'] == 'AOS') &
        (applovinDf['campaign_type'] == '7D')
    ].copy()
    JP_AOS7Df = JP_AOS7Df.sort_values(by=['install_day'], ascending=[True])
    JP_AOS7Df['7roi'] = JP_AOS7Df['r7usd'] / JP_AOS7Df['cost']
    JP_AOS7Df['sum_cost'] = JP_AOS7Df['cost'].cumsum()
    JP_AOS7Df['sum_r7usd'] = JP_AOS7Df['r7usd'].cumsum()
    JP_AOS7Df['sum_7roi'] = JP_AOS7Df['sum_r7usd'] / JP_AOS7Df['sum_cost']
    JP_AOS7Df['KPI'] = milestonesDf['Android_JP_Applovin_7DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_AOS7Df['sum_cost_ok'] = JP_AOS7Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_AOS7Df.to_csv('/src/data/th_milestones_JP_AOS_7D.csv', index=False)
    JP_AOS7Df['install_day'] = pd.to_datetime(JP_AOS7Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_AOS7Df['install_day'], JP_AOS7Df['KPI'], label='KPI', color='red')
    ax1.plot(JP_AOS7Df['install_day'], JP_AOS7Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS JP 7ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_AOS7Df['install_day'], JP_AOS7Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_AOS7Df['install_day'], JP_AOS7Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_AOS7Df['install_day'], JP_AOS7Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS JP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_AOS_7D.png')
    plt.close()

    # JP + AOS + 28D
    JP_AOS28Df = applovinDf[
        (applovinDf['country_group'] == 'JP') &
        (applovinDf['platform'] == 'AOS') &
        (applovinDf['campaign_type'] == '28D')
    ].copy()
    JP_AOS28Df = JP_AOS28Df.sort_values(by=['install_day'], ascending=[True])
    JP_AOS28Df['7roi'] = JP_AOS28Df['r7usd'] / JP_AOS28Df['cost']
    JP_AOS28Df['sum_cost'] = JP_AOS28Df['cost'].cumsum()
    JP_AOS28Df['sum_r7usd'] = JP_AOS28Df['r7usd'].cumsum()
    JP_AOS28Df['sum_7roi'] = JP_AOS28Df['sum_r7usd'] / JP_AOS28Df['sum_cost']
    JP_AOS28Df['KPI'] = milestonesDf['Android_JP_Applovin_28DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_AOS28Df['sum_cost_ok'] = JP_AOS28Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_AOS28Df.to_csv('/src/data/th_milestones_JP_AOS_28D.csv', index=False)
    JP_AOS28Df['install_day'] = pd.to_datetime(JP_AOS28Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_AOS28Df['install_day'], JP_AOS28Df['KPI'], label='KPI', color='red')
    ax1.plot(JP_AOS28Df['install_day'], JP_AOS28Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS JP 28D ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_AOS28Df['install_day'], JP_AOS28Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_AOS28Df['install_day'], JP_AOS28Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_AOS28Df['install_day'], JP_AOS28Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS JP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_AOS_28D.png')
    plt.close()

    # JP + IOS + 7D
    JP_IOS7Df = applovinDf[
        (applovinDf['country_group'] == 'JP') &
        (applovinDf['platform'] == 'IOS') &
        (applovinDf['campaign_type'] == '7D')
    ].copy()
    JP_IOS7Df = JP_IOS7Df.sort_values(by=['install_day'], ascending=[True])
    JP_IOS7Df['7roi'] = JP_IOS7Df['r7usd'] / JP_IOS7Df['cost']
    JP_IOS7Df['sum_cost'] = JP_IOS7Df['cost'].cumsum()
    JP_IOS7Df['sum_r7usd'] = JP_IOS7Df['r7usd'].cumsum()
    JP_IOS7Df['sum_7roi'] = JP_IOS7Df['sum_r7usd'] / JP_IOS7Df['sum_cost']
    JP_IOS7Df['KPI'] = milestonesDf['iOS_JP_Applovin_7DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_IOS7Df['sum_cost_ok'] = JP_IOS7Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_IOS7Df.to_csv('/src/data/th_milestones_JP_IOS_7D.csv', index=False)
    JP_IOS7Df['install_day'] = pd.to_datetime(JP_IOS7Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_IOS7Df['install_day'], JP_IOS7Df['KPI'], label='KPI', color='red')
    ax1.plot(JP_IOS7Df['install_day'], JP_IOS7Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS JP 7ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_IOS7Df['install_day'], JP_IOS7Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_IOS7Df['install_day'], JP_IOS7Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_IOS7Df['install_day'], JP_IOS7Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS JP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_IOS_7D.png')
    plt.close()

    # JP + IOS + 28D
    JP_IOS28Df = applovinDf[
        (applovinDf['country_group'] == 'JP') &
        (applovinDf['platform'] == 'IOS') &
        (applovinDf['campaign_type'] == '28D')
    ].copy()
    JP_IOS28Df = JP_IOS28Df.sort_values(by=['install_day'], ascending=[True])
    JP_IOS28Df['7roi'] = JP_IOS28Df['r7usd'] / JP_IOS28Df['cost']
    JP_IOS28Df['sum_cost'] = JP_IOS28Df['cost'].cumsum()
    JP_IOS28Df['sum_r7usd'] = JP_IOS28Df['r7usd'].cumsum()
    JP_IOS28Df['sum_7roi'] = JP_IOS28Df['sum_r7usd'] / JP_IOS28Df['sum_cost']
    JP_IOS28Df['KPI'] = milestonesDf['iOS_JP_Applovin_28DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    JP_IOS28Df['sum_cost_ok'] = JP_IOS28Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    JP_IOS28Df.to_csv('/src/data/th_milestones_JP_IOS_28D.csv', index=False)
    JP_IOS28Df['install_day'] = pd.to_datetime(JP_IOS28Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(JP_IOS28Df['install_day'], JP_IOS28Df['KPI'], label='KPI', color='red')
    ax1.plot(JP_IOS28Df['install_day'], JP_IOS28Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS JP 28D ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(JP_IOS28Df['install_day'], JP_IOS28Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(JP_IOS28Df['install_day'], JP_IOS28Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(JP_IOS28Df['install_day'], JP_IOS28Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS JP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_JP_IOS_28D.png')
    plt.close()

    # NOJP + AOS + 7D
    NOJP_AOS7Df = applovinDf[
        (applovinDf['country_group'] == 'OTHER') &
        (applovinDf['platform'] == 'AOS') &
        (applovinDf['campaign_type'] == '7D')
    ].copy()
    NOJP_AOS7Df = NOJP_AOS7Df.sort_values(by=['install_day'], ascending=[True])
    NOJP_AOS7Df['7roi'] = NOJP_AOS7Df['r7usd'] / NOJP_AOS7Df['cost']
    NOJP_AOS7Df['sum_cost'] = NOJP_AOS7Df['cost'].cumsum()
    NOJP_AOS7Df['sum_r7usd'] = NOJP_AOS7Df['r7usd'].cumsum()
    NOJP_AOS7Df['sum_7roi'] = NOJP_AOS7Df['sum_r7usd'] / NOJP_AOS7Df['sum_cost']
    NOJP_AOS7Df['KPI'] = milestonesDf['Android_noJP_Applovin_7DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_AOS7Df['sum_cost_ok'] = NOJP_AOS7Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_AOS7Df.to_csv('/src/data/th_milestones_NOJP_AOS_7D.csv', index=False)
    NOJP_AOS7Df['install_day'] = pd.to_datetime(NOJP_AOS7Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_AOS7Df['install_day'], NOJP_AOS7Df['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_AOS7Df['install_day'], NOJP_AOS7Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS NOJP 7ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_AOS7Df['install_day'], NOJP_AOS7Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_AOS7Df['install_day'], NOJP_AOS7Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_AOS7Df['install_day'], NOJP_AOS7Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS NOJP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_AOS_7D.png')
    plt.close()

    # NOJP + AOS + 28D
    NOJP_AOS28Df = applovinDf[
        (applovinDf['country_group'] == 'OTHER') &
        (applovinDf['platform'] == 'AOS') &
        (applovinDf['campaign_type'] == '28D')
    ].copy()
    NOJP_AOS28Df = NOJP_AOS28Df.sort_values(by=['install_day'], ascending=[True])
    NOJP_AOS28Df['7roi'] = NOJP_AOS28Df['r7usd'] / NOJP_AOS28Df['cost']
    NOJP_AOS28Df['sum_cost'] = NOJP_AOS28Df['cost'].cumsum()
    NOJP_AOS28Df['sum_r7usd'] = NOJP_AOS28Df['r7usd'].cumsum()
    NOJP_AOS28Df['sum_7roi'] = NOJP_AOS28Df['sum_r7usd'] / NOJP_AOS28Df['sum_cost']
    NOJP_AOS28Df['KPI'] = milestonesDf['Android_noJP_Applovin_28DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_AOS28Df['sum_cost_ok'] = NOJP_AOS28Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_AOS28Df.to_csv('/src/data/th_milestones_NOJP_AOS_28D.csv', index=False)
    NOJP_AOS28Df['install_day'] = pd.to_datetime(NOJP_AOS28Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_AOS28Df['install_day'], NOJP_AOS28Df['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_AOS28Df['install_day'], NOJP_AOS28Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('AOS NOJP 28D ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_AOS28Df['install_day'], NOJP_AOS28Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_AOS28Df['install_day'], NOJP_AOS28Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_AOS28Df['install_day'], NOJP_AOS28Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('AOS NOJP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_AOS_28D.png')
    plt.close()

    # NOJP + IOS + 7D
    NOJP_IOS7Df = applovinDf[
        (applovinDf['country_group'] == 'OTHER') &
        (applovinDf['platform'] == 'IOS') &
        (applovinDf['campaign_type'] == '7D')
    ].copy()
    NOJP_IOS7Df = NOJP_IOS7Df.sort_values(by=['install_day'], ascending=[True])
    NOJP_IOS7Df['7roi'] = NOJP_IOS7Df['r7usd'] / NOJP_IOS7Df['cost']
    NOJP_IOS7Df['sum_cost'] = NOJP_IOS7Df['cost'].cumsum()
    NOJP_IOS7Df['sum_r7usd'] = NOJP_IOS7Df['r7usd'].cumsum()
    NOJP_IOS7Df['sum_7roi'] = NOJP_IOS7Df['sum_r7usd'] / NOJP_IOS7Df['sum_cost']
    NOJP_IOS7Df['KPI'] = milestonesDf['iOS_noJP_Applovin_7DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_IOS7Df['sum_cost_ok'] = NOJP_IOS7Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_IOS7Df.to_csv('/src/data/th_milestones_NOJP_IOS_7D.csv', index=False)
    NOJP_IOS7Df['install_day'] = pd.to_datetime(NOJP_IOS7Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_IOS7Df['install_day'], NOJP_IOS7Df['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_IOS7Df['install_day'], NOJP_IOS7Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS NOJP 7ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_IOS7Df['install_day'], NOJP_IOS7Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_IOS7Df['install_day'], NOJP_IOS7Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_IOS7Df['install_day'], NOJP_IOS7Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS NOJP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_IOS_7D.png')
    plt.close()

    # NOJP + IOS + 28D
    NOJP_IOS28Df = applovinDf[
        (applovinDf['country_group'] == 'OTHER') &
        (applovinDf['platform'] == 'IOS') &
        (applovinDf['campaign_type'] == '28D')
    ].copy()
    NOJP_IOS28Df = NOJP_IOS28Df.sort_values(by=['install_day'], ascending=[True])
    NOJP_IOS28Df['7roi'] = NOJP_IOS28Df['r7usd'] / NOJP_IOS28Df['cost']
    NOJP_IOS28Df['sum_cost'] = NOJP_IOS28Df['cost'].cumsum()
    NOJP_IOS28Df['sum_r7usd'] = NOJP_IOS28Df['r7usd'].cumsum()
    NOJP_IOS28Df['sum_7roi'] = NOJP_IOS28Df['sum_r7usd'] / NOJP_IOS28Df['sum_cost']
    NOJP_IOS28Df['KPI'] = milestonesDf['iOS_noJP_Applovin_28DCampaign_7ROI'].values[0]
    # 如果sum_7roi < KPI, 则sum_cost_ok = 0, 否则sum_cost_ok = sum_cost
    NOJP_IOS28Df['sum_cost_ok'] = NOJP_IOS28Df.apply(
        lambda row: 0 if row['sum_7roi'] < row['KPI'] else row['sum_cost'], axis=1
    )
    NOJP_IOS28Df.to_csv('/src/data/th_milestones_NOJP_IOS_28D.csv', index=False)
    NOJP_IOS28Df['install_day'] = pd.to_datetime(NOJP_IOS28Df['install_day'], format='%Y%m%d')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.plot(NOJP_IOS28Df['install_day'], NOJP_IOS28Df['KPI'], label='KPI', color='red')
    ax1.plot(NOJP_IOS28Df['install_day'], NOJP_IOS28Df['sum_7roi'], label='sum 7roi', color='orange')
    ax1.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax1.set_ylabel('ROI')
    ax1.set_title('IOS NOJP 28D ROI')
    ax1.legend()
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.plot(NOJP_IOS28Df['install_day'], NOJP_IOS28Df['cost'], label='daily cost', color='blue')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('daily cost')
    ax3 = ax2.twinx()
    ax3.fill_between(NOJP_IOS28Df['install_day'], NOJP_IOS28Df['sum_cost_ok'], color='green', alpha=0.5)
    ax3.plot(NOJP_IOS28Df['install_day'], NOJP_IOS28Df['sum_cost_ok'], color='green', label='Sum Cost ok')
    ax3.set_ylim(bottom=0)
    ax3.margins(y=0.1)
    ax3.legend(loc='upper right')
    ax3.set_ylabel('Sum Cost ok')
    ax2.axvline(x=full7dayEndDate, color='orange', linestyle='--', label='full7dayEndDate')
    ax2.set_xlabel('Install Day')
    ax2.set_title('IOS NOJP daily cost and Sum Cost ok')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/src/data/th_milestones_NOJP_IOS_28D.png')
    plt.close()

def merge_images_horizontally(image_path1, image_path2, output_path):
    """
    将两张图片横向并排合并成一张图片并保存。
    :param image_path1: 第一张图片的路径
    :param image_path2: 第二张图片的路径
    :param output_path: 合并后图片的保存路径
    """
    try:
        # 打开两张图片
        img1 = Image.open(image_path1)
        img2 = Image.open(image_path2)
        # 获取图片的尺寸
        width1, height1 = img1.size
        width2, height2 = img2.size
        # 创建一个新的图像，其宽度是两张图片宽度之和，高度为两张图片的最大高度
        new_width = width1 + width2
        new_height = max(height1, height2)
        # 创建一个新的空白图像
        new_img = Image.new('RGB', (new_width, new_height))
        # 将第一张图片粘贴到新图像的左侧
        new_img.paste(img1, (0, 0))
        # 将第二张图片粘贴到新图像的右侧
        new_img.paste(img2, (width1, 0))
        # 保存合并后的图片
        new_img.save(output_path)
        print(f"合并后的图片已保存到: {output_path}")
    except Exception as e:
        print(f"合并图片时发生错误: {e}")


from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addHead3,addText,addFile,sendMessage,addImage,addCode,sendMessageToWebhook,sendMessageToWebhook2
def report(reportData):
    
    # 获取飞书的token
    tenantAccessToken = getTenantAccessToken()

    docId = createDoc(tenantAccessToken, f"TopHeros 里程碑进度速读AI版 {reportData['todayStr']}",'IIwifvDaGl8uACdS5FlcbRDKnHh')
    print('docId:', docId)

    addHead1(tenantAccessToken, docId, '', '文档说明')
    addText(tenantAccessToken, docId, '', '本文档每周一自动生成，获得从最近里程碑开始到上周数据。\n')

    addHead1(tenantAccessToken, docId, '', '里程碑进度')
    allDf = reportData['allDf']
    costOk = allDf[allDf['install_day'] == reportData['endDay']]['sum_cost_ok'].sum()
    costOkRate = costOk / reportData['milestonesTargetUsd'] * 100

    text1 = f"目前里程碑于{reportData['startDay']}开始，截止目前满7日数据（{reportData['endDay']}），共计{reportData['days']}天。\n"
    text1 += f"目前累计里程碑达标花费金额{costOk:.0f}美元，完成{costOkRate:.2f}%\n"
    
    addText(tenantAccessToken, docId, '', text1)

    # 为了计算环比，先计算本周与上周的时间范围
    # 由于需要满7日数据，这里的本周指的是上上周，上周则是3周之前
    today = datetime.datetime.strptime(reportData['todayStr'], '%Y%m%d')
    thisWeekStart = today - datetime.timedelta(days=today.weekday()) - datetime.timedelta(days=14)
    thisWeekEnd = thisWeekStart + datetime.timedelta(days=6)
    lastWeekStart = thisWeekStart - datetime.timedelta(days=7)
    lastWeekEnd = thisWeekStart - datetime.timedelta(days=1)

    if reportData['days'] < 14:
        addText(tenantAccessToken, docId, '', '里程碑进度(满7日数据）不足14天，暂不进行环比与细分 分析。')
    else:
        addHead2(tenantAccessToken, docId,'', '大盘')

        warningText = ''

        # allDf = reportData['allDf']
        thisWeekCost = allDf[allDf['install_day'] == thisWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - allDf[allDf['install_day'] == thisWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        lastWeekCost = allDf[allDf['install_day'] == lastWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - allDf[allDf['install_day'] == lastWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()

        weekOnWeekCostRate = (thisWeekCost - lastWeekCost)/lastWeekCost * 100
        op = '上升' if weekOnWeekCostRate > 0 else '下降'
        weekOnWeekCostRate = abs(weekOnWeekCostRate)
        text2 = f"本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额增长为{thisWeekCost:.0f}，环比上周期（{lastWeekStart.strftime('%Y%m%d')}~{lastWeekEnd.strftime('%Y%m%d')}）{op}{weekOnWeekCostRate:.2f}%。"
        addText(tenantAccessToken, docId, '', text2)

        if thisWeekCost < 0:
            warningText += f"大盘 本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额为负数{thisWeekCost:.0f}。说明最近有部分平台+国家的投放数据不达标，需要警惕。\n"
            
        addHead2(tenantAccessToken, docId, '', '细分')
        addHead3(tenantAccessToken, docId, '', 'AOS JP')
        JP_AOSDf = reportData['JP_AOSDf']
        thisWeekCost = JP_AOSDf[JP_AOSDf['install_day'] == thisWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - JP_AOSDf[JP_AOSDf['install_day'] == thisWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        lastWeekCost = JP_AOSDf[JP_AOSDf['install_day'] == lastWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - JP_AOSDf[JP_AOSDf['install_day'] == lastWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        weekOnWeekCostRate = (thisWeekCost - lastWeekCost)/lastWeekCost * 100
        op = '上升' if weekOnWeekCostRate > 0 else '下降'
        weekOnWeekCostRate = abs(weekOnWeekCostRate)
        text3 = f"本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额增长为{thisWeekCost:.0f}，环比上周期（{lastWeekStart.strftime('%Y%m%d')}~{lastWeekEnd.strftime('%Y%m%d')}）{op}{weekOnWeekCostRate:.2f}%。"
        addText(tenantAccessToken, docId, '', text3)

        if thisWeekCost < 0:
            warningText += f"安卓+JP 本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额为负数{thisWeekCost:.0f}。"
            thisWeekDf = JP_AOSDf[JP_AOSDf['install_day'].between(pd.to_datetime(thisWeekStart.strftime('%Y%m%d')), pd.to_datetime(thisWeekEnd.strftime('%Y%m%d')))]
            thisWeekROI = thisWeekDf['r7usd'].sum() / thisWeekDf['cost'].sum()
            kpi = thisWeekDf['KPI'].values[0]
            if thisWeekROI < kpi:
                warningText += f"且本周期的平均ROI低于KPI，正在加剧里程碑不达标的风险。\n"
            else:
                warningText += f"但本周期的平均ROI高于KPI，正在缓解里程碑不达标的风险。\n"


        addHead3(tenantAccessToken, docId, '', 'AOS NOJP')
        NOJP_AOSDf = reportData['NOJP_AOSDf']
        thisWeekCost = NOJP_AOSDf[NOJP_AOSDf['install_day'] == thisWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - NOJP_AOSDf[NOJP_AOSDf['install_day'] == thisWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        lastWeekCost = NOJP_AOSDf[NOJP_AOSDf['install_day'] == lastWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - NOJP_AOSDf[NOJP_AOSDf['install_day'] == lastWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        weekOnWeekCostRate = (thisWeekCost - lastWeekCost)/lastWeekCost * 100
        op = '上升' if weekOnWeekCostRate > 0 else '下降'
        weekOnWeekCostRate = abs(weekOnWeekCostRate)
        text4 = f"本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额增长为{thisWeekCost:.0f}，环比上周期（{lastWeekStart.strftime('%Y%m%d')}~{lastWeekEnd.strftime('%Y%m%d')}）{op}{weekOnWeekCostRate:.2f}%。"
        addText(tenantAccessToken, docId, '', text4)

        if thisWeekCost < 0:
            warningText += f"安卓+非JP 本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额为负数{thisWeekCost:.0f}。"
            thisWeekDf = NOJP_AOSDf[NOJP_AOSDf['install_day'].between(pd.to_datetime(thisWeekStart.strftime('%Y%m%d')), pd.to_datetime(thisWeekEnd.strftime('%Y%m%d')))]
            thisWeekROI = thisWeekDf['r7usd'].sum() / thisWeekDf['cost'].sum()
            kpi = thisWeekDf['KPI'].values[0]
            if thisWeekROI < kpi:
                warningText += f"且本周期的平均ROI低于KPI，正在加剧里程碑不达标的风险。\n"
            else:
                warningText += f"但本周期的平均ROI高于KPI，正在缓解里程碑不达标的风险。\n"

        addHead3(tenantAccessToken, docId, '', 'IOS JP')
        JP_IOSDf = reportData['JP_IOSDf']
        thisWeekCost = JP_IOSDf[JP_IOSDf['install_day'] == thisWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - JP_IOSDf[JP_IOSDf['install_day'] == thisWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        lastWeekCost = JP_IOSDf[JP_IOSDf['install_day'] == lastWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - JP_IOSDf[JP_IOSDf['install_day'] == lastWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        weekOnWeekCostRate = (thisWeekCost - lastWeekCost)/lastWeekCost * 100
        op = '上升' if weekOnWeekCostRate > 0 else '下降'
        weekOnWeekCostRate = abs(weekOnWeekCostRate)
        text5 = f"本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额增长为{thisWeekCost:.0f}，环比上周期（{lastWeekStart.strftime('%Y%m%d')}~{lastWeekEnd.strftime('%Y%m%d')}）{op}{weekOnWeekCostRate:.2f}%。"
        addText(tenantAccessToken, docId, '', text5)

        if thisWeekCost < 0:
            warningText += f"iOS+JP 本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额为负数{thisWeekCost:.0f}。"
            thisWeekDf = JP_IOSDf[JP_IOSDf['install_day'].between(pd.to_datetime(thisWeekStart.strftime('%Y%m%d')), pd.to_datetime(thisWeekEnd.strftime('%Y%m%d')))]
            thisWeekROI = thisWeekDf['r7usd'].sum() / thisWeekDf['cost'].sum()
            kpi = thisWeekDf['KPI'].values[0]
            if thisWeekROI < kpi:
                warningText += f"且本周期的平均ROI低于KPI，正在加剧里程碑不达标的风险。\n"
            else:
                warningText += f"但本周期的平均ROI高于KPI，正在缓解里程碑不达标的风险。\n"
        
        addHead3(tenantAccessToken, docId, '', 'IOS NOJP')
        NOJP_IOSDf = reportData['NOJP_IOSDf']
        thisWeekCost = NOJP_IOSDf[NOJP_IOSDf['install_day'] == thisWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - NOJP_IOSDf[NOJP_IOSDf['install_day'] == thisWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        lastWeekCost = NOJP_IOSDf[NOJP_IOSDf['install_day'] == lastWeekEnd.strftime('%Y%m%d')]['sum_cost_ok'].sum() - NOJP_IOSDf[NOJP_IOSDf['install_day'] == lastWeekStart.strftime('%Y%m%d')]['sum_cost_ok'].sum()
        weekOnWeekCostRate = (thisWeekCost - lastWeekCost)/lastWeekCost * 100
        op = '上升' if weekOnWeekCostRate > 0 else '下降'
        weekOnWeekCostRate = abs(weekOnWeekCostRate)
        text6 = f"本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额增长为{thisWeekCost:.0f}，环比上周期（{lastWeekStart.strftime('%Y%m%d')}~{lastWeekEnd.strftime('%Y%m%d')}）{op}{weekOnWeekCostRate:.2f}%。"
        addText(tenantAccessToken, docId, '', text6)

        if thisWeekCost < 0:
            warningText += f"iOS+非JP 本周期（{thisWeekStart.strftime('%Y%m%d')}~{thisWeekEnd.strftime('%Y%m%d')}）里程碑达标花费金额为负数{thisWeekCost:.0f}。"
            thisWeekDf = NOJP_IOSDf[NOJP_IOSDf['install_day'].between(pd.to_datetime(thisWeekStart.strftime('%Y%m%d')), pd.to_datetime(thisWeekEnd.strftime('%Y%m%d')))]
            thisWeekROI = thisWeekDf['r7usd'].sum() / thisWeekDf['cost'].sum()
            kpi = thisWeekDf['KPI'].values[0]
            if thisWeekROI < kpi:
                warningText += f"且本周期的平均ROI低于KPI，正在加剧里程碑不达标的风险。\n"
            else:
                warningText += f"但本周期的平均ROI高于KPI，正在缓解里程碑不达标的风险。\n"
        

    #风险提示
    addHead1(tenantAccessToken, docId, '', '风险提示')
    # 如果本周里程碑达标花费金额增长为负数，说明最近有部分平台+国家的投放数据不达标，需要警惕。
    # 如果本周里程碑达标花费金额增长为负数，且本周期的平均ROI低于KPI，说明正在加剧里程碑不达标的风险。
    addText(tenantAccessToken, docId, '', warningText,bold=True)

    # 参考数据
    addHead1(tenantAccessToken, docId, '', '参考数据')
    addHead2(tenantAccessToken, docId, '', '大盘')
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_all.csv',view_type = 1)
    addHead3(tenantAccessToken, docId, '', '图例解释：')
    text = '''
红线：KPI Cost 为里程碑目标达标花费。
蓝线：Sum Cost 为里程碑开始至昨日花费金额。
绿线：Sum Cost ok 为里程碑开始至昨日 达标花费金额。达标计算方式详见 备注 章节。 
黄色竖虚线：full7dayEndDate 为满7日分割线，虚线左侧为满7日数据，虚线右侧为未满7日数据。
    '''
    addText(tenantAccessToken, docId, '', text)
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_all.png')

    addHead2(tenantAccessToken, docId, '', '分平台 + 分国家')
    addHead3(tenantAccessToken, docId, '', '图例解释：')
    text = '''
图例解释：
红线：KPI  为达标7日ROI。
橙线：sum 7roi 为里程碑开始至昨日 累计7日ROI。
蓝线：daily cost 每日花费金额。
绿线：Sum Cost ok 为里程碑开始至昨日 达标花费金额。达标计算方式详见 备注 章节。 
黄色竖虚线：full7dayEndDate 为满7日分割线，虚线左侧为满7日数据，虚线右侧为未满7日数据。
    '''
    addText(tenantAccessToken, docId, '', text)

    # 将JP_AOS NOJP_AOS 两张图片横向并排 合并成一张图片
    
    merge_images_horizontally('/src/data/th_milestones_JP_AOS.png', '/src/data/th_milestones_NOJP_AOS.png', '/src/data/th_milestones_AOS.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_AOS.png')
    merge_images_horizontally('/src/data/th_milestones_JP_IOS.png', '/src/data/th_milestones_NOJP_IOS.png', '/src/data/th_milestones_IOS.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_IOS.png')
    
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_AOS.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_AOS.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_IOS.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_IOS.csv',view_type = 1)

    addHead2(tenantAccessToken, docId, '', 'Applivin：')

    merge_images_horizontally('/src/data/th_milestones_JP_AOS_7D.png', '/src/data/th_milestones_NOJP_AOS_7D.png', '/src/data/th_milestones_AOS_7D.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_AOS_7D.png')
    merge_images_horizontally('/src/data/th_milestones_JP_IOS_7D.png', '/src/data/th_milestones_NOJP_IOS_7D.png', '/src/data/th_milestones_IOS_7D.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_IOS_7D.png')
    merge_images_horizontally('/src/data/th_milestones_JP_AOS_28D.png', '/src/data/th_milestones_NOJP_AOS_28D.png', '/src/data/th_milestones_AOS_28D.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_AOS_28D.png')
    merge_images_horizontally('/src/data/th_milestones_JP_IOS_28D.png', '/src/data/th_milestones_NOJP_IOS_28D.png', '/src/data/th_milestones_IOS_28D.png')
    addImage(tenantAccessToken, docId, '', '/src/data/th_milestones_IOS_28D.png')

    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_AOS_7D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_AOS_7D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_IOS_7D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_IOS_7D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_AOS_28D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_AOS_28D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_JP_IOS_28D.csv',view_type = 1)
    addFile(tenantAccessToken, docId, '', '/src/data/th_milestones_NOJP_IOS_28D.csv',view_type = 1)


    # 添加备注
    addHead1(tenantAccessToken, docId, '', '备注')
    addHead2(tenantAccessToken, docId, '', '里程碑要求')
    addText(tenantAccessToken, docId, '', '里程碑要求没有找到数据库表格，目前写死在代码中，如果需要更改要求，请联系我。')

    milestonesDf = getmilestonesData()
    text = f'''
里程碑开始时间：{milestonesDf['startday'].values[0]}
里程碑目标达标总花费：{milestonesDf['target_usd'].values[0]:.0f}美元

iOS JP 7ROI ：{milestonesDf['iOS_JP_7ROI'].values[0] * 100:.0f}%
iOS noJP 7ROI ：{milestonesDf['iOS_noJP_7ROI'].values[0] * 100:.0f}%
AOS JP 7ROI：{milestonesDf['Android_JP_7ROI'].values[0] * 100:.0f}%
AOS noJP 7ROI：{milestonesDf['Android_noJP_7ROI'].values[0] * 100:.0f}%

Applovin iOS JP 7DaysCampaign 7ROI ：{milestonesDf['iOS_JP_Applovin_7DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin iOS JP 28DaysCampaign 7ROI ：{milestonesDf['iOS_JP_Applovin_28DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin iOS noJP 7DaysCampaign 7ROI ：{milestonesDf['iOS_noJP_Applovin_7DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin iOS noJP 28DaysCampaign 7ROI ：{milestonesDf['iOS_noJP_Applovin_28DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin AOS JP 7DaysCampaign 7ROI：{milestonesDf['Android_JP_Applovin_7DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin AOS JP 28DaysCampaign 7ROI：{milestonesDf['Android_JP_Applovin_28DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin AOS noJP 7DaysCampaign 7ROI：{milestonesDf['Android_noJP_Applovin_7DCampaign_7ROI'].values[0] * 100:.0f}%
Applovin AOS noJP 28DaysCampaign 7ROI：{milestonesDf['Android_noJP_Applovin_28DCampaign_7ROI'].values[0] * 100:.0f}%
    '''
    addText(tenantAccessToken, docId, '', text)
    addHead2(tenantAccessToken, docId, '', '里程碑达标计算方式')
    text = f'''
从里程碑开始时间计算，计算累计的花费与累计的7日收入，并通过 累计的7日收入/累计的花费 获得累计7日ROI。
目前维度细分 按照 分平台 + 分国家，分为4个分类 与 他们的KPI 如下：
iOS JP：累计7日ROI {milestonesDf['iOS_JP_7ROI'].values[0] * 100:.0f}%
iOS noJP：累计7日ROI {milestonesDf['iOS_noJP_7ROI'].values[0] * 100:.0f}%
AOS JP：累计7日ROI {milestonesDf['Android_JP_7ROI'].values[0] * 100:.0f}%
AOS noJP：累计7日ROI {milestonesDf['Android_noJP_7ROI'].values[0] * 100:.0f}%
当累计ROI大于等于KPI时，达标花费等于 累计花费。否则达标花费 为 0。
大盘达标花费 = 所有分类 达标花费 的和。
    '''
    addText(tenantAccessToken, docId, '', text)

    docUrl = 'https://rivergame.feishu.cn/docx/'+docId
    return docUrl

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

    reportData = {
        'todayStr': todayStr,
    }

    totalAndPlatformCountry(todayStr,reportData)
    applovin(todayStr,reportData)

    docUrl = report(reportData)

    message = ''

    testWebhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c'

    webhookUrl = testWebhookUrl
    sendMessageToWebhook2(f"TopHeros 里程碑进度速读AI版 {reportData['todayStr']} 报告已生成",message,'详细报告',docUrl,webhookUrl)
        
    


    

# 历史数据补充，如果有需要补充的历史数据，调佣这个函数，并且调整时间范围
def historyData():
    startDayStr = '20250101'
    endDayStr = '20250430'

    startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')

    for i in range((endDay - startDay).days + 1):
        day = startDay + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y%m%d')
        # print(dayStr)
        main(dayStr)


if __name__ == '__main__':
    main('20250505')
