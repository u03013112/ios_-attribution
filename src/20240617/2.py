
import os
import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getCvStr():
    str = '''
CASE
    WHEN r1usd = 0 THEN 0
    WHEN r1usd > 0 AND r1usd <= 0.99 THEN 1
    WHEN r1usd > 0.99 AND r1usd <= 1.15 THEN 2
    WHEN r1usd > 1.15 AND r1usd <= 1.3 THEN 3
    WHEN r1usd > 1.3 AND r1usd <= 2.98 THEN 4
    WHEN r1usd > 2.98 AND r1usd <= 3.41 THEN 5
    WHEN r1usd > 3.41 AND r1usd <= 5.98 THEN 6
    WHEN r1usd > 5.98 AND r1usd <= 7.46 THEN 7
    WHEN r1usd > 7.46 AND r1usd <= 9.09 THEN 8
    WHEN r1usd > 9.09 AND r1usd <= 12.05 THEN 9
    WHEN r1usd > 12.05 AND r1usd <= 14.39 THEN 10
    WHEN r1usd > 14.39 AND r1usd <= 18.17 THEN 11
    WHEN r1usd > 18.17 AND r1usd <= 22.07 THEN 12
    WHEN r1usd > 22.07 AND r1usd <= 26.57 THEN 13
    WHEN r1usd > 26.57 AND r1usd <= 32.09 THEN 14
    WHEN r1usd > 32.09 AND r1usd <= 37.42 THEN 15
    WHEN r1usd > 37.42 AND r1usd <= 42.94 THEN 16
    WHEN r1usd > 42.94 AND r1usd <= 50.34 THEN 17
    WHEN r1usd > 50.34 AND r1usd <= 58.56 THEN 18
    WHEN r1usd > 58.56 AND r1usd <= 67.93 THEN 19
    WHEN r1usd > 67.93 AND r1usd <= 80.71 THEN 20
    WHEN r1usd > 80.71 AND r1usd <= 100.32 THEN 21
    WHEN r1usd > 100.32 AND r1usd <= 116.94 THEN 22
    WHEN r1usd > 116.94 AND r1usd <= 130.41 THEN 23
    WHEN r1usd > 130.41 AND r1usd <= 153.76 THEN 24
    WHEN r1usd > 153.76 AND r1usd <= 196.39 THEN 25
    WHEN r1usd > 196.39 AND r1usd <= 235.93 THEN 26
    WHEN r1usd > 235.93 AND r1usd <= 292.07 THEN 27
    WHEN r1usd > 292.07 AND r1usd <= 424.48 THEN 28
    WHEN r1usd > 424.48 AND r1usd <= 543.77 THEN 29
    WHEN r1usd > 543.77 AND r1usd <= 753.61 THEN 30
    WHEN r1usd > 753.61 THEN 31
    ELSE 0
END AS cv
    '''
    return str

def getDataAndroid(installTimeStart = '2024-04-01',installTimeEnd = '2024-04-30'):
    filename = f'/src/data/zk2/lw20240617_android_{installTimeStart}_{installTimeEnd}.csv'

    installTimeStartTimestamp = int(datetime.strptime(installTimeStart, '%Y-%m-%d').timestamp())
    installTimeEndTimestamp = int(datetime.strptime(installTimeEnd, '%Y-%m-%d').timestamp())
    # 时区不对，简便解决方案
    installTimeStartTimestamp += 8*3600
    installTimeEndTimestamp += 8*3600

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
SET
	odps.sql.timezone = Africa / Accra;

set
	odps.sql.executionengine.enable.rand.time.seed = true;

@user_info :=
select
	uid,
	mediasource,
	install_timestamp,
    campaign_id,
    country,
	to_char(from_unixtime(install_timestamp), 'YYYY-MM-DD') as install_day
from
	dws_overseas_lastwar_unique_uid
where
	app_package = 'com.fun.lastwar.gp'
	and install_timestamp between {installTimeStartTimestamp} and {installTimeEndTimestamp}
;

@user_revenue :=
select
	game_uid as uid,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 2 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r2usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 7 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r7usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 30 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r30usd,
	COALESCE(
		max(event_time) FILTER (
			WHERE
				event_time - install_timestamp between 0
				and 1 * 86400
		),
		install_timestamp
	) AS last_timestamp
FROM
	rg_bi.dwd_overseas_revenue_allproject
WHERE
	zone = '0'
	and app = 502
	and app_package = 'com.fun.lastwar.gp'
	AND game_uid IS NOT NULL
GROUP BY
	game_uid,
	install_timestamp
;

@adInfo :=
select
    campaign_id,
    campaign_name,
    campaign_type,
    optimization_goal
from dwb_overseas_mediasource_campaign_map
where
    campaign_id <> 'OTHER'
;

@retTable1 :=
select
    user_info.country,
	user_info.uid,
	user_info.install_day as install_date,
	COALESCE(user_revenue.r1usd, 0) as r1usd,
	COALESCE(user_revenue.r2usd, 0) as r2usd,
	COALESCE(user_revenue.r7usd, 0) as r7usd,
	COALESCE(user_revenue.r30usd, 0) as r30usd,
	user_info.install_timestamp,
	COALESCE(
        user_revenue.last_timestamp,
		user_info.install_timestamp
	) as last_timestamp,
	user_info.mediasource as media_source,
	user_info.campaign_id
from
	@user_info as user_info
	left join @user_revenue as user_revenue on user_info.uid = user_revenue.uid
;

@retTable2 :=
select 
    *,
    {getCvStr()}
from 
    @retTable1
;

@retTable3 :=
select
    retTable2.*,
    adInfo.campaign_name,
    adInfo.campaign_type,
    adInfo.optimization_goal
from
    @retTable2 as retTable2
    left join @adInfo as adInfo on retTable2.campaign_id = adInfo.campaign_id
;

select
    cv,
    install_date,
    campaign_type,
    optimization_goal,
    country,
    media_source as media,
    count(1) as user_count,
    sum(r1usd) as r1usd,
    sum(r7usd) as r7usd,
    sum(r30usd) as r30usd
from 
@retTable3
group by 
    cv,
    install_date,
    media,
    campaign_type,
    optimization_goal,
    country
;

        '''
        print(sql)
        df = execSql(sql)
        df['country_code'] = ''
        df.to_csv(filename, index=False)
    return df

def getDataIos(installTimeStart = '2024-04-01',installTimeEnd = '2024-04-30'):
    filename = f'/src/data/zk2/lw20240617_ios_{installTimeStart}_{installTimeEnd}.csv'

    installTimeStartTimestamp = int(datetime.strptime(installTimeStart, '%Y-%m-%d').timestamp())
    installTimeEndTimestamp = int(datetime.strptime(installTimeEnd, '%Y-%m-%d').timestamp())
    # 时区不对，简便解决方案
    installTimeStartTimestamp += 8*3600
    installTimeEndTimestamp += 8*3600

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
SET
	odps.sql.timezone = Africa / Accra;

set
	odps.sql.executionengine.enable.rand.time.seed = true;

@user_info :=
select
	uid,
	mediasource,
	install_timestamp,
    campaign_id,
    country,
	to_char(from_unixtime(install_timestamp), 'YYYY-MM-DD') as install_day
from
	dws_overseas_lastwar_unique_uid
where
	app_package = 'id6448786147'
	and install_timestamp between {installTimeStartTimestamp} and {installTimeEndTimestamp}
;

@user_revenue :=
select
	game_uid as uid,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 2 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r2usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 7 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r7usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time - install_timestamp between 0
				and 30 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r30usd,
	COALESCE(
		max(event_time) FILTER (
			WHERE
				event_time - install_timestamp between 0
				and 1 * 86400
		),
		install_timestamp
	) AS last_timestamp
FROM
	rg_bi.dwd_overseas_revenue_allproject
WHERE
	zone = '0'
	and app = 502
	and app_package = 'id6448786147'
	AND game_uid IS NOT NULL
GROUP BY
	game_uid,
	install_timestamp
;

@adInfo :=
select
    campaign_id,
    campaign_name,
    campaign_type,
    optimization_goal
from dwb_overseas_mediasource_campaign_map
where
    campaign_id <> 'OTHER'
;

@retTable1 :=
select
    user_info.country,
	user_info.uid,
	user_info.install_day as install_date,
	COALESCE(user_revenue.r1usd, 0) as r1usd,
	COALESCE(user_revenue.r2usd, 0) as r2usd,
	COALESCE(user_revenue.r7usd, 0) as r7usd,
	COALESCE(user_revenue.r30usd, 0) as r30usd,
	user_info.install_timestamp,
	COALESCE(
        user_revenue.last_timestamp,
		user_info.install_timestamp
	) as last_timestamp,
	user_info.mediasource as media_source,
	user_info.campaign_id
from
	@user_info as user_info
	left join @user_revenue as user_revenue on user_info.uid = user_revenue.uid
;

@retTable2 :=
select 
    *,
    {getCvStr()}
from 
    @retTable1
;

@retTable3 :=
select
    retTable2.*,
    adInfo.campaign_name,
    adInfo.campaign_type,
    adInfo.optimization_goal
from
    @retTable2 as retTable2
    left join @adInfo as adInfo on retTable2.campaign_id = adInfo.campaign_id
;

select
    cv,
    install_date,
    campaign_type,
    optimization_goal,
    country,
    media_source as media,
    count(1) as user_count,
    sum(r1usd) as r1usd,
    sum(r7usd) as r7usd,
    sum(r30usd) as r30usd
from 
@retTable3
group by 
    cv,
    install_date,
    media,
    campaign_type,
    optimization_goal,
    country
;

        '''

        
        print(sql)
        df = execSql(sql)
        df['country_code'] = ''
        df.to_csv(filename, index=False)
    return df

def debugAndroid():
    df = getDataAndroid(installTimeStart='2024-01-01',installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList),'cvGroup'] = cvGroupName

    # 将install_date转换为Month
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    df = df.groupby(['media','cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    df['r7usd mean'] = df['r7usd'] / df['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)
    df = df.sort_values(by=['media','cvGroup','Month']).reset_index(drop=True)

    mediaList = [
        'Facebook Ads',
        'organic',
        'googleadwords_int',
        'applovin_int',
    ]

    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = df[df['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        cvGroupDf.to_csv(f'/src/data/zk2/debug5_android_{cvGroupName}.csv', index=True)
        meanGroupDf.to_csv(f'/src/data/zk2/debug5_android_mean_{cvGroupName}.csv', index=False)

        plt.figure(figsize=(24,6))

        for media in mediaList:
            mediaDf = cvGroupDf[cvGroupDf['media'] == media]
            plt.plot(mediaDf['Month'], mediaDf['r7usd mean'], label=media)

        plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

        plt.xlabel('Month')
        plt.ylabel('R7USD Mean')
        plt.title(f'{cvGroupName} CV Group')
        plt.legend()
        plt.savefig(f'/src/data/zk2/debug5_android_{cvGroupName}.jpg')
        plt.close()

def debugAndroidTotal():
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList), 'cvGroup'] = cvGroupName

    # 计算总体均值
    meanDf = df.groupby(['cvGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']
    meanDf['media'] = 'Overall Mean'

    # 计算各媒体的均值
    mediaDf = df.groupby(['media', 'cvGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    mediaDf['r7usd mean'] = mediaDf['r7usd'] / mediaDf['user_count']

    # 合并总体均值和各媒体均值
    resultDf = pd.concat([mediaDf, meanDf], ignore_index=True)
    resultDf = resultDf.loc[resultDf['r7usd'] > 1000]
    # 保存并打印结果
    resultDf.to_csv('/src/data/zk2/debug_android_total.csv', index=False)
    print(resultDf)

def debugAndroidCountry():
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList), 'cvGroup'] = cvGroupName

    # 创建新的Month列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Month列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup', 'Month']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    mediaList = [
        'Facebook Ads',
        'organic',
        'googleadwords_int',
        'applovin_int',
    ]

    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    groupbyCountryDf = df.groupby(['media', 'cvGroup', 'Month', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    groupbyCountryDf['r7usd mean'] = groupbyCountryDf['r7usd'] / groupbyCountryDf['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)
    groupbyCountryDf = groupbyCountryDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)

    for countryGroup in countryGroupList:
        countryGroupName = countryGroup['name']
        countryGroupDf = groupbyCountryDf[groupbyCountryDf['countryGroup'] == countryGroupName]

        for cvGroup in cvGroupList:
            cvGroupName = cvGroup['name']
            cvGroupDf = countryGroupDf[countryGroupDf['cvGroup'] == cvGroupName]
            meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

            # 计算上限值
            overall_max = meanGroupDf['r7usd mean'].max()
            upper_limit = overall_max * 3

            plt.figure(figsize=(24, 6))
            for media in mediaList:
                mediaDf = cvGroupDf[cvGroupDf['media'] == media]
                mediaDf = mediaDf.sort_values(by=['Month']).reset_index(drop=True)
                # 截断超过上限的值
                mediaDf['r7usd mean'] = mediaDf['r7usd mean'].apply(lambda x: min(x, upper_limit))
                plt.plot(mediaDf['Month'], mediaDf['r7usd mean'], label=media)

            plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

            plt.xlabel('Month')
            plt.ylabel('R7USD Mean')
            plt.title(f'{countryGroupName} - {cvGroupName} CV Group')
            plt.legend()
            plt.savefig(f'/src/data/zk2/debug5_android_{countryGroupName}_{cvGroupName}.jpg')
            plt.close()

    # 计算每个媒体在所有时间中的平均r7usd，按国家分组
    overallMeanDf = df.groupby(['media', 'cvGroup', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']
    overallMeanDf = overallMeanDf[overallMeanDf['media'].isin(mediaList)]

    # 计算所有媒体的总体均值，按国家分组
    totalMeanDf = df.groupby(['cvGroup', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    totalMeanDf['media'] = 'Overall'
    totalMeanDf['r7usd mean'] = totalMeanDf['r7usd'] / totalMeanDf['user_count']

    # 合并结果
    finalMeanDf = pd.concat([overallMeanDf, totalMeanDf[['media', 'cvGroup', 'countryGroup', 'r7usd mean']]], ignore_index=True)
    finalMeanDf = finalMeanDf[['media', 'cvGroup', 'countryGroup', 'r7usd mean']]

    # 按cvGroup, media, countryGroup进行排序
    finalMeanDf = finalMeanDf[['cvGroup','countryGroup','media','r7usd mean']]
    finalMeanDf = finalMeanDf.sort_values(by=['cvGroup','countryGroup','media', ]).reset_index(drop=True)

    # 保存结果到CSV文件
    finalMeanDf.to_csv('/src/data/zk2/debug5_android_media_cvGroup_country_r7usd_mean.csv', index=False)


def debugAndroidFbCountry():
    df = getDataAndroid(installTimeStart='2024-01-01',installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList),'cvGroup'] = cvGroupName

    
    # df['Month'] = df['install_date'].apply(lambda x: x[:9])
    # 创建新的Month列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Month列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    FacebookAdsDf = df[df['media'] == 'Facebook Ads']

    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    FacebookAdsDf['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        FacebookAdsDf.loc[FacebookAdsDf['country'].isin(countryList),'countryGroup'] = countryGroupName

    groupbyCountryDf = FacebookAdsDf.groupby(['media','cvGroup','Month','countryGroup']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    groupbyCountryDf['r7usd mean'] = groupbyCountryDf['r7usd'] / groupbyCountryDf['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)
    groupbyCountryDf = groupbyCountryDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)

    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = groupbyCountryDf[groupbyCountryDf['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        # 计算上限值
        overall_max = meanGroupDf['r7usd mean'].max()
        upper_limit = overall_max * 3


        for i in range(0, len(countryGroupList), 5):  # 每4个国家一组
            plt.figure(figsize=(24,6))

            for countryGroup in countryGroupList[i:i+5]:  # 选取当前组的国家
                countryGroupName = countryGroup['name']
                countryDf = cvGroupDf[cvGroupDf['countryGroup'] == countryGroupName]
                countryDf = countryDf.sort_values(by=['Month']).reset_index(drop=True)
                # 截断超过上限的值
                countryDf['r7usd mean'] = countryDf['r7usd mean'].apply(lambda x: min(x, upper_limit))
                plt.plot(countryDf['Month'], countryDf['r7usd mean'], label=countryGroupName)

            plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

            plt.xlabel('Month')
            plt.ylabel('R7USD Mean')
            plt.title(f'{cvGroupName} CV Group')
            plt.legend()
            plt.savefig(f'/src/data/zk2/debug5_android_facebookAds_{cvGroupName}_{i//5+1}.jpg')  # 递增序号
            plt.close()

def debugAndroidFbCountry2():
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    # 计算每个媒体的用户国家分布
    mediaList = df['media'].unique()
    result = []

    for media in mediaList:
        mediaDf = df[df['media'] == media]
        totalUsers = mediaDf['user_count'].sum()
        totalR1usd = mediaDf['r1usd'].sum()
        totalR7usd = mediaDf['r7usd'].sum()

        for countryGroup in countryGroupList:
            countryGroupName = countryGroup['name']
            countryGroupDf = mediaDf[mediaDf['countryGroup'] == countryGroupName]
            countryUsers = countryGroupDf['user_count'].sum()
            countryR1usd = countryGroupDf['r1usd'].sum()
            countryR7usd = countryGroupDf['r7usd'].sum()

            userPercentage = (countryUsers / totalUsers) * 100 if totalUsers > 0 else 0
            r1usdPercentage = (countryR1usd / totalR1usd) * 100 if totalR1usd > 0 else 0
            r7usdPercentage = (countryR7usd / totalR7usd) * 100 if totalR7usd > 0 else 0

            result.append({
                'media': media,
                'countryGroup': countryGroupName,
                'userPercentage': userPercentage,
                'r1usdPercentage': r1usdPercentage,
                'r7usdPercentage': r7usdPercentage
            })

    resultDf = pd.DataFrame(result)
    print(resultDf)
    resultDf.to_csv('/src/data/zk2/debug5_android_fb_country.csv', index=False)

def debugAndroidFbAd():
    df = getDataAndroid(installTimeStart='2024-01-01',installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList),'cvGroup'] = cvGroupName

    
    # df['Month'] = df['install_date'].apply(lambda x: x[:9])
    # 创建新的Month列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Month列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    FacebookAdsDf = df[df['media'] == 'Facebook Ads']
    
    groupbyAdTypeDf = FacebookAdsDf.groupby(['media','cvGroup','Month','campaign_type','optimization_goal']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    groupbyAdTypeDf['r7usd mean'] = groupbyAdTypeDf['r7usd'] / groupbyAdTypeDf['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)
    groupbyAdTypeDf = groupbyAdTypeDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)

    # 获取所有实际存在的 campaign_type 和 optimization_goal 的组合
    typeList = FacebookAdsDf[['campaign_type', 'optimization_goal']].drop_duplicates().values.tolist()

    print(typeList)


    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = groupbyAdTypeDf[groupbyAdTypeDf['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        # 计算上限值
        overall_max = meanGroupDf['r7usd mean'].max()
        upper_limit = overall_max * 3

        plt.figure(figsize=(24,6))
        for campaignType, optimizationGoal in typeList:
            label = f'{campaignType} + {optimizationGoal}'
            

            typeDf = cvGroupDf[(cvGroupDf['campaign_type'] == campaignType) & (cvGroupDf['optimization_goal'] == optimizationGoal)]
            typeDf = typeDf.sort_values(by=['Month']).reset_index(drop=True)
            # print(cvGroup,label)
            # print(typeDf.head(10))
            # 截断超过上限的值
            typeDf['r7usd mean'] = typeDf['r7usd mean'].apply(lambda x: min(x, upper_limit))
            plt.plot(typeDf['Month'], typeDf['r7usd mean'], label=label)

        plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

        plt.xlabel('Month')
        plt.ylabel('R7USD Mean')
        plt.title(f'{cvGroupName} CV Group')
        plt.legend()
        plt.savefig(f'/src/data/zk2/debug5_android_facebookAds_ad_{cvGroupName}.jpg')  # 递增序号
        plt.close()

def debugAndroidFbAd2():
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    # 创建广告类型分组
    df['adTypeGroup'] = df['campaign_type'] + ' + ' + df['optimization_goal']

    # 计算每个媒体的广告类型分组的用户国家分布
    # mediaList = df['media'].unique()
    mediaList = ['Facebook Ads']

    result = []

    for media in mediaList:
        mediaDf = df[df['media'] == media]
        totalUsers = mediaDf['user_count'].sum()
        totalR1usd = mediaDf['r1usd'].sum()
        totalR7usd = mediaDf['r7usd'].sum()

        adTypeGroups = mediaDf['adTypeGroup'].unique()
        for adTypeGroup in adTypeGroups:
            adTypeGroupDf = mediaDf[mediaDf['adTypeGroup'] == adTypeGroup]
            adTypeUsers = adTypeGroupDf['user_count'].sum()
            adTypeR1usd = adTypeGroupDf['r1usd'].sum()
            adTypeR7usd = adTypeGroupDf['r7usd'].sum()

            userPercentage = (adTypeUsers / totalUsers) * 100 if totalUsers > 0 else 0
            r1usdPercentage = (adTypeR1usd / totalR1usd) * 100 if totalR1usd > 0 else 0
            r7usdPercentage = (adTypeR7usd / totalR7usd) * 100 if totalR7usd > 0 else 0

            result.append({
                'media': media,
                'adTypeGroup': adTypeGroup,
                'userPercentage': userPercentage,
                'r1usdPercentage': r1usdPercentage,
                'r7usdPercentage': r7usdPercentage
            })

    resultDf = pd.DataFrame(result)
    resultDf = resultDf.loc[resultDf['userPercentage'] > 0]
    print(resultDf)
    resultDf.to_csv('/src/data/zk2/debug5_android_fb_ad.csv', index=False)

def debugIos():
    df = getDataIos(installTimeStart='2024-01-01',installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList),'cvGroup'] = cvGroupName

    # 将install_date转换为Month
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    df = df.groupby(['media','cvGroup','Month']).agg({
        'r7usd':'sum',
        'user_count':'sum'
    }).reset_index()
    df['r7usd mean'] = df['r7usd'] / df['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup','Month']).reset_index(drop=True)
    df = df.sort_values(by=['media','cvGroup','Month']).reset_index(drop=True)

    mediaList = [
        'Facebook Ads',
        'organic',
        'googleadwords_int',
        'applovin_int',
    ]

    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = df[df['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        cvGroupDf.to_csv(f'/src/data/zk2/debug5_ios_{cvGroupName}.csv', index=True)
        meanGroupDf.to_csv(f'/src/data/zk2/debug5_ios_mean_{cvGroupName}.csv', index=False)

        plt.figure(figsize=(24,6))

        for media in mediaList:
            mediaDf = cvGroupDf[cvGroupDf['media'] == media]
            plt.plot(mediaDf['Month'], mediaDf['r7usd mean'], label=media)

        plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

        plt.xlabel('Month')
        plt.ylabel('R7USD Mean')
        plt.title(f'{cvGroupName} CV Group')
        plt.legend()
        plt.savefig(f'/src/data/zk2/debug5_ios_{cvGroupName}.jpg')
        plt.close()

def debugIosTotal():
    df = getDataIos(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList), 'cvGroup'] = cvGroupName

    # 计算总体均值
    meanDf = df.groupby(['cvGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']
    meanDf['media'] = 'Overall Mean'

    # 计算各媒体的均值
    mediaDf = df.groupby(['media', 'cvGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    mediaDf['r7usd mean'] = mediaDf['r7usd'] / mediaDf['user_count']

    # 合并总体均值和各媒体均值
    resultDf = pd.concat([mediaDf, meanDf], ignore_index=True)
    resultDf = resultDf.loc[resultDf['r7usd'] > 1000]
    
    # 保存并打印结果
    resultDf.to_csv('/src/data/zk2/debug_ios_total.csv', index=False)
    print(resultDf)

def debugIosFbCountry():
    df = getDataIos(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList), 'cvGroup'] = cvGroupName

    # 创建新的Month列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Month列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup', 'Month']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    FacebookAdsDf = df[df['media'] == 'Facebook Ads']

    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    FacebookAdsDf['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        FacebookAdsDf.loc[FacebookAdsDf['country'].isin(countryList), 'countryGroup'] = countryGroupName

    groupbyCountryDf = FacebookAdsDf.groupby(['media', 'cvGroup', 'Month', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    groupbyCountryDf['r7usd mean'] = groupbyCountryDf['r7usd'] / groupbyCountryDf['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)
    groupbyCountryDf = groupbyCountryDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)

    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = groupbyCountryDf[groupbyCountryDf['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        # 计算上限值
        overall_max = meanGroupDf['r7usd mean'].max()
        upper_limit = overall_max * 3

        for i in range(0, len(countryGroupList), 5):  # 每5个国家一组
            plt.figure(figsize=(24, 6))

            for countryGroup in countryGroupList[i:i+5]:  # 选取当前组的国家
                countryGroupName = countryGroup['name']
                countryDf = cvGroupDf[cvGroupDf['countryGroup'] == countryGroupName]
                countryDf = countryDf.sort_values(by=['Month']).reset_index(drop=True)
                # 截断超过上限的值
                countryDf['r7usd mean'] = countryDf['r7usd mean'].apply(lambda x: min(x, upper_limit))
                plt.plot(countryDf['Month'], countryDf['r7usd mean'], label=countryGroupName)

            plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

            plt.xlabel('Month')
            plt.ylabel('R7USD Mean')
            plt.title(f'{cvGroupName} CV Group')
            plt.legend()
            plt.savefig(f'/src/data/zk2/debug5_ios_facebookAds_{cvGroupName}_{i//5+1}.jpg')  # 递增序号
            plt.close()

def debugIosFbCountry2():
    df = getDataIos(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    # 计算每个媒体的用户国家分布
    mediaList = df['media'].unique()
    result = []

    for media in mediaList:
        mediaDf = df[df['media'] == media]
        totalUsers = mediaDf['user_count'].sum()
        totalR1usd = mediaDf['r1usd'].sum()
        totalR7usd = mediaDf['r7usd'].sum()

        for countryGroup in countryGroupList:
            countryGroupName = countryGroup['name']
            countryGroupDf = mediaDf[mediaDf['countryGroup'] == countryGroupName]
            countryUsers = countryGroupDf['user_count'].sum()
            countryR1usd = countryGroupDf['r1usd'].sum()
            countryR7usd = countryGroupDf['r7usd'].sum()

            userPercentage = (countryUsers / totalUsers) * 100 if totalUsers > 0 else 0
            r1usdPercentage = (countryR1usd / totalR1usd) * 100 if totalR1usd > 0 else 0
            r7usdPercentage = (countryR7usd / totalR7usd) * 100 if totalR7usd > 0 else 0

            result.append({
                'media': media,
                'countryGroup': countryGroupName,
                'userPercentage': userPercentage,
                'r1usdPercentage': r1usdPercentage,
                'r7usdPercentage': r7usdPercentage
            })

    resultDf = pd.DataFrame(result)
    print(resultDf)
    resultDf.to_csv('/src/data/zk2/debug5_ios_fb_country.csv', index=False)

def debugIosFbAd():
    df = getDataIos(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
    ]

    df['cvGroup'] = 'unknow'
    for cvGroup in cvGroupList:
        cvList = cvGroup['cvList']
        cvGroupName = cvGroup['name']
        df.loc[df['cv'].isin(cvList), 'cvGroup'] = cvGroupName

    # 创建新的Month列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Month列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Month'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    meanDf = df.groupby(['cvGroup', 'Month']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    meanDf['r7usd mean'] = meanDf['r7usd'] / meanDf['user_count']

    FacebookAdsDf = df[df['media'] == 'Facebook Ads']
    
    groupbyAdTypeDf = FacebookAdsDf.groupby(['media', 'cvGroup', 'Month', 'campaign_type', 'optimization_goal']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    groupbyAdTypeDf['r7usd mean'] = groupbyAdTypeDf['r7usd'] / groupbyAdTypeDf['user_count']

    meanDf = meanDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)
    groupbyAdTypeDf = groupbyAdTypeDf.sort_values(by=['cvGroup', 'Month']).reset_index(drop=True)

    # 获取所有实际存在的 campaign_type 和 optimization_goal 的组合
    typeList = FacebookAdsDf[['campaign_type', 'optimization_goal']].drop_duplicates().values.tolist()

    print(typeList)

    for cvGroup in cvGroupList:
        cvGroupName = cvGroup['name']
        cvGroupDf = groupbyAdTypeDf[groupbyAdTypeDf['cvGroup'] == cvGroupName]
        meanGroupDf = meanDf[meanDf['cvGroup'] == cvGroupName]

        # 计算上限值
        overall_max = meanGroupDf['r7usd mean'].max()
        upper_limit = overall_max * 3

        plt.figure(figsize=(24, 6))
        for campaignType, optimizationGoal in typeList:
            label = f'{campaignType} + {optimizationGoal}'

            typeDf = cvGroupDf[(cvGroupDf['campaign_type'] == campaignType) & (cvGroupDf['optimization_goal'] == optimizationGoal)]
            typeDf = typeDf.sort_values(by=['Month']).reset_index(drop=True)
            # 截断超过上限的值
            typeDf['r7usd mean'] = typeDf['r7usd mean'].apply(lambda x: min(x, upper_limit))
            plt.plot(typeDf['Month'], typeDf['r7usd mean'], label=label)

        plt.plot(meanGroupDf['Month'], meanGroupDf['r7usd mean'], linestyle='--', label='Overall Mean')

        plt.xlabel('Month')
        plt.ylabel('R7USD Mean')
        plt.title(f'{cvGroupName} CV Group')
        plt.legend()
        plt.savefig(f'/src/data/zk2/debug5_ios_facebookAds_ad_{cvGroupName}.jpg')  # 递增序号
        plt.close()

def debugIosFbAd2():
    df = getDataIos(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)
    
    # 创建广告类型分组
    df['adTypeGroup'] = df['campaign_type'] + ' + ' + df['optimization_goal']

    # 计算每个媒体的广告类型分组的用户国家分布
    mediaList = ['Facebook Ads']

    result = []

    for media in mediaList:
        mediaDf = df[df['media'] == media]
        totalUsers = mediaDf['user_count'].sum()
        totalR1usd = mediaDf['r1usd'].sum()
        totalR7usd = mediaDf['r7usd'].sum()

        adTypeGroups = mediaDf['adTypeGroup'].unique()
        for adTypeGroup in adTypeGroups:
            adTypeGroupDf = mediaDf[mediaDf['adTypeGroup'] == adTypeGroup]
            adTypeUsers = adTypeGroupDf['user_count'].sum()
            adTypeR1usd = adTypeGroupDf['r1usd'].sum()
            adTypeR7usd = adTypeGroupDf['r7usd'].sum()

            userPercentage = (adTypeUsers / totalUsers) * 100 if totalUsers > 0 else 0
            r1usdPercentage = (adTypeR1usd / totalR1usd) * 100 if totalR1usd > 0 else 0
            r7usdPercentage = (adTypeR7usd / totalR7usd) * 100 if totalR7usd > 0 else 0

            result.append({
                'media': media,
                'adTypeGroup': adTypeGroup,
                'userPercentage': userPercentage,
                'r1usdPercentage': r1usdPercentage,
                'r7usdPercentage': r7usdPercentage
            })

    resultDf = pd.DataFrame(result)
    resultDf = resultDf.loc[resultDf['userPercentage'] > 0]
    print(resultDf)
    resultDf.to_csv('/src/data/zk2/debug5_ios_fb_ad.csv', index=False)

# 用均值估算媒体每周的r7usd的每用户均值，并计算MAPE
def debugAndroid2():
    # 获取数据
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    # 计算每个CV大盘每周的r7usd的每用户均值
    overallMeanDf = df.groupby(['cv', 'Week']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']

    # 真实值
    realDf = df.groupby(['cv', 'Week', 'media']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()

    # 合并均值和真实值
    mergedDf = pd.merge(realDf, overallMeanDf[['cv', 'Week', 'r7usd mean']], on=['cv', 'Week'], how='left')

    # 计算估计值
    mergedDf['r7usd estimated'] = mergedDf['r7usd mean'] * mergedDf['user_count']

    # 计算每个媒体每周的真实值和估计值
    mediaWeekDf = mergedDf.groupby(['media', 'Week']).agg({
        'r7usd': 'sum',
        'r7usd estimated': 'sum'
    }).reset_index()

    # 计算MAPE
    mediaWeekDf['abs_error'] = np.abs((mediaWeekDf['r7usd'] - mediaWeekDf['r7usd estimated']) / mediaWeekDf['r7usd'])
    mapeDf = mediaWeekDf.groupby('media').agg({
        'abs_error': 'mean'
    }).reset_index()
    mapeDf['MAPE'] = mapeDf['abs_error'] * 100

    # 打印每个媒体的MAPE
    for index, row in mapeDf.iterrows():
        print(f"Media: {row['media']}, MAPE: {row['MAPE']:.2f}%")
# 按国家均值估算媒体每周的r7usd的每用户均值，并计算MAPE
def debugAndroid3():
    # 获取数据
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    # 定义国家分组
    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    # 计算每个CV大盘每周的r7usd的每用户均值，按国家分组
    overallMeanDf = df.groupby(['cv', 'Week', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']

    # 真实值
    realDf = df.groupby(['cv', 'Week', 'media', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()

    # 合并均值和真实值
    mergedDf = pd.merge(realDf, overallMeanDf[['cv', 'Week', 'countryGroup', 'r7usd mean']], on=['cv', 'Week', 'countryGroup'], how='left')

    # 计算估计值
    mergedDf['r7usd estimated'] = mergedDf['r7usd mean'] * mergedDf['user_count']

    # 计算每个媒体每周的真实值和估计值，按国家分组
    mediaWeekDf = mergedDf.groupby(['media', 'Week']).agg({
        'r7usd': 'sum',
        'r7usd estimated': 'sum'
    }).reset_index()

    # 计算MAPE
    mediaWeekDf['abs_error'] = np.abs((mediaWeekDf['r7usd'] - mediaWeekDf['r7usd estimated']) / mediaWeekDf['r7usd'])
    mapeDf = mediaWeekDf.groupby('media').agg({
        'abs_error': 'mean'
    }).reset_index()
    mapeDf['MAPE'] = mapeDf['abs_error'] * 100

    # 打印每个媒体的MAPE
    for index, row in mapeDf.iterrows():
        print(f"Media: {row['media']}, MAPE: {row['MAPE']:.2f}%")

from sklearn.cluster import KMeans
def getCountryGroupListByKmeans(N=2):
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    
    # 将country中的空值替换为"Other"
    df['country'] = df['country'].fillna('Other')

    df = df.groupby('country').agg({
        'user_count': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum'
    }).reset_index()

    r7usd_sum = df['r7usd'].sum()
    df['r7usd_percentage'] = df['r7usd'] / r7usd_sum
    df.loc[df['r7usd_percentage'] < 0.02, 'country'] = 'Other'

    
    df = df.groupby('country').agg({
        'user_count': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum'
    }).reset_index()

    df['r7usd/r1usd'] = df['r7usd'] / df['r1usd']

    # 将无法有效计算r7usd/r1usd的值替换为1
    df['r7usd/r1usd'] = df['r7usd/r1usd'].replace([np.inf, -np.inf, np.nan], 1)
    df.loc[df['r1usd'] == 0, 'r7usd/r1usd'] = 1

    # 使用KMeans聚类，按照r7usd/r1usd进行聚类，分为2类
    kmeans = KMeans(n_clusters=N, random_state=0)
    df['cluster'] = kmeans.fit_predict(df[['r7usd/r1usd']])
    
    # 获取每个聚类的质心
    centroids = kmeans.cluster_centers_
    
    # 计算每个聚类的用户数和占比
    total_users = df['user_count'].sum()
    cluster_user_counts = df.groupby('cluster')['user_count'].sum()
    cluster_user_ratios = cluster_user_counts / total_users
    
    # 创建countryGroupList
    countryGroupList = []
    for cluster in df['cluster'].unique():
        countryList = df[df['cluster'] == cluster]['country'].tolist()
        centroid = centroids[cluster][0]
        # user_count = cluster_user_counts[cluster]
        # user_ratio = cluster_user_ratios[cluster]
        
        # 计算每个聚类的用户数
        user_count = df[df['country'].isin(countryList)]['user_count'].sum()
        user_ratio = user_count / total_users
        
        countryGroupList.append({
            'name': f'Cluster {cluster}',
            'countryList': countryList,
            'centroid': centroid,
            'user_count': user_count,
            'user_ratio': user_ratio
        })
    
    return countryGroupList

# 加权重的KMeans
def getCountryGroupListByKmeans2(N=2):
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    
    # 将country中的空值替换为"Other"
    df['country'] = df['country'].fillna('Other')

    df = df.groupby('country').agg({
        'user_count': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum'
    }).reset_index()

    r7usd_sum = df['r7usd'].sum()
    df['r7usd_percentage'] = df['r7usd'] / r7usd_sum
    df.loc[df['r7usd_percentage'] < 0.02, 'country'] = 'Other'

    df = df.groupby('country').agg({
        'user_count': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum'
    }).reset_index()

    df['r7usd/r1usd'] = df['r7usd'] / df['r1usd']

    # 将无法有效计算r7usd/r1usd的值替换为1
    df['r7usd/r1usd'] = df['r7usd/r1usd'].replace([np.inf, -np.inf, np.nan], 1)
    df.loc[df['r1usd'] == 0, 'r7usd/r1usd'] = 1

    # 计算每个国家的权重，按照r7usd总量的2%作为单位
    total_r7usd = df['r7usd'].sum()
    unit = total_r7usd * 0.02
    df['weight'] = df['r7usd'] / unit

    # 权重不足1的按照1来处理
    df['weight'] = df['weight'].apply(lambda x: max(1, int(x)))

    # 数据扩展，根据权重对数据进行扩展
    expanded_data = []
    for _, row in df.iterrows():
        weight = row['weight']
        for _ in range(weight):
            expanded_data.append(row[['country', 'r7usd/r1usd']].values)
    
    expanded_df = pd.DataFrame(expanded_data, columns=['country', 'r7usd/r1usd'])

    # 使用KMeans聚类，按照扩展后的数据进行聚类，分为2类
    kmeans = KMeans(n_clusters=N, random_state=0)
    expanded_df['cluster'] = kmeans.fit_predict(expanded_df[['r7usd/r1usd']])
    
    # 获取每个聚类的质心
    centroids = kmeans.cluster_centers_
    
    # 计算每个聚类的用户数和占比
    cluster_user_counts = df.groupby('country')['user_count'].sum()
    total_users = cluster_user_counts.sum()
    cluster_user_ratios = cluster_user_counts / total_users
    
    # 创建countryGroupList，确保结果是排重过的
    countryGroupList = []
    for cluster in expanded_df['cluster'].unique():
        countryList = expanded_df[expanded_df['cluster'] == cluster]['country'].unique().tolist()
        centroid = centroids[cluster][0]
        
        # 计算每个聚类的用户数
        user_count = df[df['country'].isin(countryList)]['user_count'].sum()
        user_ratio = user_count / total_users
        
        countryGroupList.append({
            'name': f'Cluster {cluster}',
            'countryList': countryList,
            'centroid': centroid,
            'user_count': user_count,
            'user_ratio': user_ratio
        })
    
    return countryGroupList

# 在getCountryGroupListByKmeans2的基础上，修改other的处理方式
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

def getCountryGroupListByKmeans3(N=2):
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    
    # 将country中的空值替换为"Other"
    df['country'] = df['country'].fillna('Other')

    df = df.groupby('country').agg({
        'user_count': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum'
    }).reset_index()

    r7usd_sum = df['r7usd'].sum()
    df['r7usd_percentage'] = df['r7usd'] / r7usd_sum

    # 将占比小于1%的国家标记为"Other"
    small_countries = df[df['r7usd_percentage'] < 0.01].copy()
    large_countries = df[df['r7usd_percentage'] >= 0.01].copy()

    # 对小国家进行初步分类，分为other1和other2
    if not small_countries.empty:
        small_countries['countryBackup'] = small_countries['country']
        small_countries.loc[:, 'r7usd/r1usd'] = small_countries['r7usd'] / small_countries['r1usd']
        small_countries.loc[:, 'r7usd/r1usd'] = small_countries['r7usd/r1usd'].replace([np.inf, -np.inf, np.nan], 1)
        small_countries.loc[small_countries['r1usd'] == 0, 'r7usd/r1usd'] = 1

        kmeans_small = KMeans(n_clusters=2, random_state=0)
        small_countries.loc[:, 'small_cluster'] = kmeans_small.fit_predict(small_countries[['r7usd/r1usd']])
        small_countries.loc[:, 'country'] = small_countries['small_cluster'].apply(lambda x: f'Other{x+1}')
        
        # 打印other1和other2分别包含哪些国家
        other1_countries = small_countries[small_countries['country'] == 'Other1']['countryBackup'].tolist()
        other2_countries = small_countries[small_countries['country'] == 'Other2']['countryBackup'].tolist()
        print("Other1 countries:", other1_countries)
        print("Other2 countries:", other2_countries)
        
        # 计算small_countries的总r7usd
        small_r7usd_sum = small_countries['r7usd'].sum()
        small_unit = small_r7usd_sum * 0.02
        small_countries['weight'] = small_countries['r7usd'] / small_unit

        # 权重不足1的按照1来处理
        small_countries['weight'] = small_countries['weight'].apply(lambda x: max(1, int(x)))

        # 数据扩展，根据权重对数据进行扩展
        expanded_small_data = []
        for _, row in small_countries.iterrows():
            weight = row['weight']
            for _ in range(weight):
                expanded_small_data.append(row[['country', 'r7usd/r1usd']].values)
        
        expanded_small_df = pd.DataFrame(expanded_small_data, columns=['country', 'r7usd/r1usd'])

        # 使用KMeans聚类，按照扩展后的数据进行聚类，分为2类
        kmeans_small_expanded = KMeans(n_clusters=2, random_state=0)
        expanded_small_df['small_cluster'] = kmeans_small_expanded.fit_predict(expanded_small_df[['r7usd/r1usd']])
        
        # 更新small_countries的分类
        small_countries['small_cluster'] = expanded_small_df.groupby('country')['small_cluster'].first().reindex(small_countries['country']).values
        small_countries['country'] = small_countries['small_cluster'].apply(lambda x: f'Other{x+1}')
        
        small_countries = small_countries.groupby('country').agg({
            'user_count': 'sum',
            'r1usd': 'sum',
            'r7usd': 'sum'
        }).reset_index()
    
    # 合并大国家和初步分类的小国家
    df = pd.concat([large_countries, small_countries], ignore_index=True)

    df['r7usd/r1usd'] = df['r7usd'] / df['r1usd']

    # 将无法有效计算r7usd/r1usd的值替换为1
    df['r7usd/r1usd'] = df['r7usd/r1usd'].replace([np.inf, -np.inf, np.nan], 1)
    df.loc[df['r1usd'] == 0, 'r7usd/r1usd'] = 1

    # 计算每个国家的权重，按照r7usd总量的2%作为单位
    total_r7usd = df['r7usd'].sum()
    unit = total_r7usd * 0.02
    df['weight'] = df['r7usd'] / unit

    # 权重不足1的按照1来处理
    df['weight'] = df['weight'].apply(lambda x: max(1, int(x)))

    # 数据扩展，根据权重对数据进行扩展
    expanded_data = []
    for _, row in df.iterrows():
        weight = row['weight']
        for _ in range(weight):
            expanded_data.append(row[['country', 'r7usd/r1usd']].values)
    
    expanded_df = pd.DataFrame(expanded_data, columns=['country', 'r7usd/r1usd'])

    # 使用KMeans聚类，按照扩展后的数据进行聚类，分为N类
    kmeans = KMeans(n_clusters=N, random_state=0)
    expanded_df['cluster'] = kmeans.fit_predict(expanded_df[['r7usd/r1usd']])
    
    # 获取每个聚类的质心
    centroids = kmeans.cluster_centers_
    
    # 计算每个聚类的用户数和占比
    cluster_user_counts = df.groupby('country')['user_count'].sum()
    total_users = cluster_user_counts.sum()
    cluster_user_ratios = cluster_user_counts / total_users
    
    # 创建countryGroupList，确保结果是排重过的
    countryGroupList = []
    for cluster in expanded_df['cluster'].unique():
        countryList = expanded_df[expanded_df['cluster'] == cluster]['country'].unique().tolist()
        centroid = centroids[cluster][0]
        
        # 计算每个聚类的用户数
        user_count = df[df['country'].isin(countryList)]['user_count'].sum()
        user_ratio = user_count / total_users
        
        countryGroupList.append({
            'name': f'Cluster {cluster}',
            'countryList': countryList,
            'centroid': centroid,
            'user_count': user_count,
            'user_ratio': user_ratio
        })
    
    return countryGroupList



# 按照不同的国家分组，计算每个媒体每周的r7usd的每用户均值，并计算MAPE
def debugAndroid4(countryGroupList):
    # 获取数据
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    # 计算每个CV大盘每周的r7usd的每用户均值，按国家分组
    overallMeanDf = df.groupby(['cv', 'Week', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']

    # 真实值
    realDf = df.groupby(['cv', 'Week', 'media', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()

    # 合并均值和真实值
    mergedDf = pd.merge(realDf, overallMeanDf[['cv', 'Week', 'countryGroup', 'r7usd mean']], on=['cv', 'Week', 'countryGroup'], how='left')

    # 计算估计值
    mergedDf['r7usd estimated'] = mergedDf['r7usd mean'] * mergedDf['user_count']

    # 计算每个媒体每周的真实值和估计值，按国家分组
    mediaWeekDf = mergedDf.groupby(['media', 'Week']).agg({
        'r7usd': 'sum',
        'r7usd estimated': 'sum'
    }).reset_index()

    mediaWeekDf = mediaWeekDf[mediaWeekDf['r7usd'] > 1000]

    # 计算MAPE
    mediaWeekDf['abs_error'] = np.abs((mediaWeekDf['r7usd'] - mediaWeekDf['r7usd estimated']) / mediaWeekDf['r7usd'])
    mapeDf = mediaWeekDf.groupby('media').agg({
        'abs_error': 'mean'
    }).reset_index()
    mapeDf['MAPE'] = mapeDf['abs_error'] * 100

    # 打印每个媒体的MAPE
    for index, row in mapeDf.iterrows():
        print(f"Media: {row['media']}, MAPE: {row['MAPE']:.2f}%")

def debugAndroid5():
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)

    groupbyWeekDf = df.groupby(['Week']).agg({
        'r1usd': 'sum',
        'r7usd': 'sum',
    }).reset_index()

    groupbyWeekDf['r7usd/r1usd'] = groupbyWeekDf['r7usd'] / groupbyWeekDf['r1usd']
    print(groupbyWeekDf)

    groupbyMediaAndWeekDf = df.groupby(['media', 'Week']).agg({
        'r1usd': 'sum',
        'r7usd': 'sum',
    }).reset_index()

    df = pd.merge(groupbyMediaAndWeekDf, groupbyWeekDf[['Week', 'r7usd/r1usd']], on='Week', how='left')
    df['r7usd estimated'] = df['r1usd'] * df['r7usd/r1usd']
    df['mape'] = np.abs((df['r7usd'] - df['r7usd estimated']) / df['r7usd'])

    mapeDf = df.groupby('media').agg({
        'mape': 'mean'
    }).reset_index()

    print(mapeDf)


# 用均值估算媒体每周的r7usd的每用户均值，并计算MAPE
def debugAndroid2Month():
    # 获取数据
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    # df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)
    df['Week'] = df['install_date'].dt.to_period('M').apply(lambda r: r.start_time)

    # 计算每个CV大盘每周的r7usd的每用户均值
    overallMeanDf = df.groupby(['cv', 'Week']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']

    # 真实值
    realDf = df.groupby(['cv', 'Week', 'media']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()

    # 合并均值和真实值
    mergedDf = pd.merge(realDf, overallMeanDf[['cv', 'Week', 'r7usd mean']], on=['cv', 'Week'], how='left')

    # 计算估计值
    mergedDf['r7usd estimated'] = mergedDf['r7usd mean'] * mergedDf['user_count']

    # 计算每个媒体每周的真实值和估计值
    mediaWeekDf = mergedDf.groupby(['media', 'Week']).agg({
        'r7usd': 'sum',
        'r7usd estimated': 'sum'
    }).reset_index()

    # 计算MAPE
    mediaWeekDf['abs_error'] = np.abs((mediaWeekDf['r7usd'] - mediaWeekDf['r7usd estimated']) / mediaWeekDf['r7usd'])
    mapeDf = mediaWeekDf.groupby('media').agg({
        'abs_error': 'mean'
    }).reset_index()
    mapeDf['MAPE'] = mapeDf['abs_error'] * 100

    # 打印每个媒体的MAPE
    for index, row in mapeDf.iterrows():
        print(f"Media: {row['media']}, MAPE: {row['MAPE']:.2f}%")
# 按国家均值估算媒体每周的r7usd的每用户均值，并计算MAPE
def debugAndroid3Month():
    # 获取数据
    df = getDataAndroid(installTimeStart='2024-01-01', installTimeEnd='2024-05-31')
    df['media'] = df['media'].apply(lambda x: 'Facebook Ads' if x == 'restricted' else x)

    # 创建新的Week列，每隔7天进行一轮汇总，并用这7天的第一天日期作为Week列内容
    df['install_date'] = pd.to_datetime(df['install_date'], errors='coerce')
    # df['Week'] = df['install_date'].dt.to_period('W-SUN').apply(lambda r: r.start_time)
    df['Week'] = df['install_date'].dt.to_period('M').apply(lambda r: r.start_time)

    # 定义国家分组
    countryGroupList = [
        {'name':'US','countryList':['US']},
        {'name':'KR','countryList':['KR']},
        {'name':'JP','countryList':['JP']},
        {'name':'CN','countryList':['TW','HK','MO']},
        {'name':'SEA','countryList':['ID','TH','VN','PH','MY','SG']},
        {'name':'EU','countryList':['GB','FR','DE','IT','ES','NL','SE','FI','NO','DK','BE','AT','CH','IE','PT','GR','PL','CZ','HU','RO','BG','HR','SK','SI','LT','LV','EE','CY','LU','MT']},
        {'name':'IN','countryList':['IN']},
        {'name':'GCC','countryList':['SA','AE','QA','KW','OM','BH']},
        {'name':'AU','countryList':['AU','NZ']},
        {'name':'Other','countryList':[]},
    ]

    df['countryGroup'] = 'Other'
    for countryGroup in countryGroupList:
        countryList = countryGroup['countryList']
        countryGroupName = countryGroup['name']
        df.loc[df['country'].isin(countryList), 'countryGroup'] = countryGroupName

    # 计算每个CV大盘每周的r7usd的每用户均值，按国家分组
    overallMeanDf = df.groupby(['cv', 'Week', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()
    overallMeanDf['r7usd mean'] = overallMeanDf['r7usd'] / overallMeanDf['user_count']

    # 真实值
    realDf = df.groupby(['cv', 'Week', 'media', 'countryGroup']).agg({
        'r7usd': 'sum',
        'user_count': 'sum'
    }).reset_index()

    # 合并均值和真实值
    mergedDf = pd.merge(realDf, overallMeanDf[['cv', 'Week', 'countryGroup', 'r7usd mean']], on=['cv', 'Week', 'countryGroup'], how='left')

    # 计算估计值
    mergedDf['r7usd estimated'] = mergedDf['r7usd mean'] * mergedDf['user_count']

    # 计算每个媒体每周的真实值和估计值，按国家分组
    mediaWeekDf = mergedDf.groupby(['media', 'Week']).agg({
        'r7usd': 'sum',
        'r7usd estimated': 'sum'
    }).reset_index()

    # 计算MAPE
    mediaWeekDf['abs_error'] = np.abs((mediaWeekDf['r7usd'] - mediaWeekDf['r7usd estimated']) / mediaWeekDf['r7usd'])
    mapeDf = mediaWeekDf.groupby('media').agg({
        'abs_error': 'mean'
    }).reset_index()
    mapeDf['MAPE'] = mapeDf['abs_error'] * 100

    # 打印每个媒体的MAPE
    for index, row in mapeDf.iterrows():
        print(f"Media: {row['media']}, MAPE: {row['MAPE']:.2f}%")

if __name__ == '__main__':
    # debugAndroid()
    # debugAndroidTotal()
    # debugAndroidFbCountry()
    # debugAndroidFbCountry2()
    # debugAndroidFbAd()
    # debugAndroidFbAd2()

    # debugIos()    
    # debugIosTotal()
    # debugIosFbCountry()
    # debugIosFbCountry2()
    # debugIosFbAd()
    # debugIosFbAd2()

    # debugAndroidCountry()

    # debugAndroid2()
    # debugAndroid3()

    # debugAndroid2Month()
    # debugAndroid3Month()


    
    # countryGroupList = getCountryGroupListByKmeans3(N=4)
    # for group in countryGroupList:
    #     print(f"Group Name: {group['name']}, Countries: {group['countryList']}, Centroid: {group['centroid']}, User Count: {group['user_count']}, User Ratio: {group['user_ratio']:.2%}")


    # debugAndroid4(countryGroupList)

    debugAndroid5()







