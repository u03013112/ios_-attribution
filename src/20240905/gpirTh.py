import os
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

appInfoList = [
	{'name':'topheros','app':'116','app_id':'com.greenmushroom.boomblitz.gp'},
	{'name':'topheros','app':'116','app_id':'com.greenmushroom.boomblitz.gp'},
	{'name':'topwar','app':'102','app_id':'com.topwar.gp'},
]

def getDataFromMC(app,app_id,installDayStart = '20240701', installDayEnd = '20240731'):
	filename = f'/src/data/gpir_{app}_{installDayStart}_{installDayEnd}.csv'
	if os.path.exists(filename):
		print('已存在%s'%filename)
		data = pd.read_csv(filename)
	else:

		sql = f'''
@Data :=
SELECT
	customer_user_id AS uid,
	REPLACE(REPLACE(gp_referrer, '#', ','), "\\\\", '') AS gp_referrer,
	mediasource,
	contributor_1_mediasource,
	contributor_2_mediasource,
	contributor_3_mediasource,
	match_type,
	install_day,
	event_time
FROM
	(
		SELECT
			customer_user_id,
			gp_referrer,
			mediasource,
			contributor_1_mediasource,
			contributor_2_mediasource,
			contributor_3_mediasource,
			match_type,
			get_day_from_timestamp(install_unix, '0') as install_day,
			event_time,
			ROW_NUMBER() OVER (
				PARTITION BY customer_user_id
				ORDER BY
					event_time ASC
			) AS rank
		FROM
			rg_bi.ods_platform_appsflyer_events_v3
		WHERE
			app = '{app}'
			AND zone = '0'
			AND app_id = '{app_id}'
			AND day >= '20240301'
			AND event_name = 'install'
	)
WHERE
	rank = 1;

@base_revenue :=
SELECT
	game_uid,
	install_day,
	revenue_value_usd,
	country,
	DATEDIFF(
		to_date(event_day, 'yyyymmdd'),
		to_date(install_day, 'yyyymmdd')
	) AS life_cycle
FROM
	dwd_overseas_revenue_allproject
WHERE
	app = '{app}'
	AND zone = 0
	AND app_package = '{app_id}'
	AND install_day >= '20240101'
;

@revenue :=
SELECT
	game_uid AS uid,
	install_day,
	country,
	SUM(
		CASE
			WHEN life_cycle = 0 THEN revenue_value_usd
			ELSE 0
		END
	) AS revenue_d1,
	SUM(
		CASE
			WHEN life_cycle BETWEEN 0
			AND 6 THEN revenue_value_usd
			ELSE 0
		END
	) AS revenue_d7
FROM
	@base_revenue
GROUP BY
	game_uid,
	install_day,
	country;

@gp_referrer_media :=
SELECT
	uid,
	CASE
		WHEN gp_referrer IS NULL THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=google-play&utm_medium=organic%' THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=(not set)%' THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=apps.facebook.com%'
		OR gp_referrer LIKE '%utm_source=apps.instagram.com%' THEN 'Facebook Ads'
		WHEN gp_referrer LIKE '%wbraid%'
		OR gp_referrer LIKE '%gbraid%'
		OR gp_referrer LIKE '%gclid%' THEN 'googleadwords_int'
		WHEN gp_referrer LIKE '%applovin%' THEN 'applovin_int'
		WHEN gp_referrer LIKE '%tiktokglobal%'
		OR gp_referrer LIKE '%bytedanceglobal%' THEN 'bytedanceglobal_int'
		WHEN gp_referrer LIKE '%pid=KOL%' THEN 'KOL'
		WHEN gp_referrer LIKE '%smartnews%' THEN 'smartnewsads_int'
		ELSE 'OTHER'
	END AS gp_referrer_media
FROM
	@Data;

@final_result_step1 :=
SELECT
	a.uid AS uid,
	a.install_day,
	a.match_type,
	a.mediasource,
	a.contributor_1_mediasource,
	a.contributor_2_mediasource,
	a.contributor_3_mediasource,
	b.country,
	COALESCE(b.revenue_d1, 0) AS revenue_d1,
  	COALESCE(b.revenue_d7, 0) AS revenue_d7
FROM
	@Data AS a
	LEFT JOIN @revenue AS b ON a.uid = b.uid;

@final_result :=
SELECT
	a.uid,
	a.install_day,
	a.match_type,
	a.mediasource,
	a.contributor_1_mediasource,
	a.contributor_2_mediasource,
	a.contributor_3_mediasource,
	c.gp_referrer_media,
	a.country,
	a.revenue_d1,
	a.revenue_d7
FROM
	@final_result_step1 AS a
	LEFT JOIN @gp_referrer_media AS c ON a.uid = c.uid;

-- 输出最终结果
SELECT
	uid,
	install_day,
	match_type,
	mediasource,
	contributor_1_mediasource,
	contributor_2_mediasource,
	contributor_3_mediasource,
	gp_referrer_media,
	country,
	revenue_d1,
	revenue_d7
FROM
	@final_result
WHERE
	install_day BETWEEN '{installDayStart}' AND '{installDayEnd}'
;
		'''
		
		if app == '102':
			# topwar 要特别处理
			sql = f'''
@Data :=
SELECT
	customer_user_id AS uid,
	REPLACE(REPLACE(gp_referrer, '#', ','), "\\\\", '') AS gp_referrer,
	mediasource,
	contributor_1_mediasource,
	contributor_2_mediasource,
	contributor_3_mediasource,
	match_type,
	install_day,
	event_time
FROM
	(
		SELECT
			customer_user_id,
			gp_referrer,
			mediasource,
			contributor_1_mediasource,
			contributor_2_mediasource,
			contributor_3_mediasource,
			match_type,
			get_day_from_timestamp(install_unix, '0') as install_day,
			event_time,
			ROW_NUMBER() OVER (
				PARTITION BY customer_user_id
				ORDER BY
					event_time ASC
			) AS rank
		FROM
			rg_bi.ods_platform_appsflyer_events_v3
		WHERE
			app = '{app}'
			AND zone = '0'
			AND app_id = '{app_id}'
			AND day >= '20240301'
			AND event_name = 'install'
	)
WHERE
	rank = 1;

@base_revenue :=
SELECT
	game_uid,
	install_day,
	revenue_value_usd,
	country,
	DATEDIFF(
		to_date(purchase_day, 'yyyymmdd'),
		to_date(install_day, 'yyyymmdd')
	) AS life_cycle
FROM
	dwd_overseas_revenue_afattribution_realtime
WHERE
	app = '102'
	AND zone = 0
	AND window_cycle = 9999
	AND install_day >= '20240101'
;

@revenue :=
SELECT
	game_uid AS uid,
	install_day,
	country,
	SUM(
		CASE
			WHEN life_cycle = 0 THEN revenue_value_usd
			ELSE 0
		END
	) AS revenue_d1,
	SUM(
		CASE
			WHEN life_cycle BETWEEN 0
			AND 6 THEN revenue_value_usd
			ELSE 0
		END
	) AS revenue_d7
FROM
	@base_revenue
GROUP BY
	game_uid,
	install_day,
	country;

@gp_referrer_media :=
SELECT
	uid,
	CASE
		WHEN gp_referrer IS NULL THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=google-play&utm_medium=organic%' THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=(not set)%' THEN 'organic'
		WHEN gp_referrer LIKE '%utm_source=apps.facebook.com%'
		OR gp_referrer LIKE '%utm_source=apps.instagram.com%' THEN 'Facebook Ads'
		WHEN gp_referrer LIKE '%wbraid%'
		OR gp_referrer LIKE '%gbraid%'
		OR gp_referrer LIKE '%gclid%' THEN 'googleadwords_int'
		WHEN gp_referrer LIKE '%applovin%' THEN 'applovin_int'
		WHEN gp_referrer LIKE '%tiktokglobal%'
		OR gp_referrer LIKE '%bytedanceglobal%' THEN 'bytedanceglobal_int'
		WHEN gp_referrer LIKE '%pid=KOL%' THEN 'KOL'
		WHEN gp_referrer LIKE '%smartnews%' THEN 'smartnewsads_int'
		ELSE 'OTHER'
	END AS gp_referrer_media
FROM
	@Data;

@final_result_step1 :=
SELECT
	a.uid AS uid,
	a.install_day,
	a.match_type,
	a.mediasource,
	a.contributor_1_mediasource,
	a.contributor_2_mediasource,
	a.contributor_3_mediasource,
	b.country,
	COALESCE(b.revenue_d1, 0) AS revenue_d1,
  	COALESCE(b.revenue_d7, 0) AS revenue_d7
FROM
	@Data AS a
	LEFT JOIN @revenue AS b ON a.uid = b.uid;

@final_result :=
SELECT
	a.uid,
	a.install_day,
	a.match_type,
	a.mediasource,
	a.contributor_1_mediasource,
	a.contributor_2_mediasource,
	a.contributor_3_mediasource,
	c.gp_referrer_media,
	a.country,
	a.revenue_d1,
	a.revenue_d7
FROM
	@final_result_step1 AS a
	LEFT JOIN @gp_referrer_media AS c ON a.uid = c.uid;

@install_date :=
	select
	uid,
	get_day_from_timestamp(install_timestamp, '0') as install_day
from 
	dws_overseas_gginstallbegin_contributor_attribution_tw
;	

@final_result2 :=
select
a.uid,
a.install_day,
b.match_type,
b.mediasource,
b.contributor_1_mediasource,
b.contributor_2_mediasource,
b.contributor_3_mediasource,
b.gp_referrer_media,
b.country,
b.revenue_d1,
b.revenue_d7
from 
@final_result as a
left join @final_result as b
on a.uid = b.uid
;


-- 输出最终结果
SELECT
	uid,
	install_day,
	match_type,
	mediasource,
	contributor_1_mediasource,
	contributor_2_mediasource,
	contributor_3_mediasource,
	gp_referrer_media,
	country,
	revenue_d1,
	revenue_d7
FROM
	@final_result2
WHERE
	install_day BETWEEN '{installDayStart}' AND '{installDayEnd}'
;
			'''
		
		print(sql)
		data = execSql(sql)
		data.to_csv(filename, index=False)

	return data


def getTopwarDataFromMC(installDayStart = '20240701', installDayEnd = '20240731'):
	filename = f'/src/data/gpir_topheros_{installDayStart}_{installDayEnd}.csv'
	if os.path.exists(filename):
		print('已存在%s'%filename)
		data = pd.read_csv(filename)
	else:
		sql = f'''
@gpir :=
select
uid,
mediasource as gpir_mediasource,
get_day_from_timestamp(install_timestamp, '0') as install_day
from dws_overseas_gginstallbegin_contributor_attribution_th
;

@af :=
select
game_uid as uid,
mediasource as af_mediasource
from
dws_overseas_topheros_unique_uid
where
app_package = 'com.greenmushroom.boomblitz.gp'
;

@purchase :=
select
game_uid as uid,
install_day,
DATEDIFF(
  to_date(event_day, 'yyyymmdd'),
  to_date(install_day, 'yyyymmdd')
) AS life_cycle,
revenue_value_usd
from
dwd_overseas_revenue_allproject
where
app = '116'
and zone = 0
and app_package = 'com.greenmushroom.boomblitz.gp'
;

@purchase7 :=
select
uid,
install_day,
sum(CASE
WHEN life_cycle BETWEEN 0
AND 6 THEN revenue_value_usd
ELSE 0
END
) AS revenue_d7
from @purchase
group by uid,install_day
;

@result :=
select
af.uid,
af.af_mediasource,
gpir.gpir_mediasource,
gpir.install_day,
purchase7.revenue_d7
from @af as af
left join @gpir as gpir on af.uid = gpir.uid
left join @purchase7 as purchase7 on af.uid = purchase7.uid
;

select
uid,
af_mediasource as mediasource,
gpir_mediasource as gp_referrer_media,
install_day,
revenue_d7
from @result
where install_day between '20240701' and '20240731'
;
		'''
		print(sql)
		data = execSql(sql)
		data.to_csv(filename, index=False)

	return data


def getTopwarDataFromMC2(installDayStart = '20240701', installDayEnd = '20240731'):
	filename = f'/src/data/gpir_topheros_group_{installDayStart}_{installDayEnd}.csv'
	if os.path.exists(filename):
		print('已存在%s'%filename)
		data = pd.read_csv(filename)
	else:
		sql = f'''
@gpir :=
select
uid,
mediasource as gpir_mediasource,
get_day_from_timestamp(install_timestamp, '0') as install_day
from dws_overseas_gginstallbegin_contributor_attribution_th
;

@af :=
select
game_uid as uid,
mediasource as af_mediasource
from
dws_overseas_topheros_unique_uid
where
app_package = 'com.greenmushroom.boomblitz.gp'
;

@purchase :=
select
game_uid as uid,
install_day,
DATEDIFF(
  to_date(event_day, 'yyyymmdd'),
  to_date(install_day, 'yyyymmdd')
) AS life_cycle,
revenue_value_usd
from
dwd_overseas_revenue_allproject
where
app = '116'
and zone = 0
and app_package = 'com.greenmushroom.boomblitz.gp'
;

@purchase7 :=
select
uid,
install_day,
sum(CASE
WHEN life_cycle BETWEEN 0
AND 6 THEN revenue_value_usd
ELSE 0
END
) AS revenue_d7
from @purchase
group by uid,install_day
;

@result :=
select
af.uid,
af.af_mediasource,
gpir.gpir_mediasource,
gpir.install_day,
purchase7.revenue_d7
from @af as af
left join @gpir as gpir on af.uid = gpir.uid
left join @purchase7 as purchase7 on af.uid = purchase7.uid
;

select
count(distinct uid) as installs,
af_mediasource as mediasource,
gpir_mediasource as gp_referrer_media,
sum(revenue_d7) as revenue_d7
from @result
where install_day between '20240701' and '20240731'
group by 
af_mediasource,gpir_mediasource
;
		'''
		print(sql)
		data = execSql(sql)
		data.to_csv(filename, index=False)

	return data


# gpir 覆盖率
def debug2(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir 覆盖率')
	app = appInfo['app']
	app_id = appInfo['app_id']

	# data = getDataFromMC(app,app_id)
	data = getTopwarDataFromMC()
	data['match_type'] = 'null'
	
	data['count'] = 1

	print('sum count:',data['count'].sum())
	print('sum revenue_d7:',data['revenue_d7'].sum())

	data = data.groupby('gp_referrer_media').agg(
		{'count': 'sum', 'revenue_d7': 'sum'}
	).reset_index()
	data = data.sort_values(by='count', ascending=False)
	
	countSum = data['count'].sum()
	revenueSum = data['revenue_d7'].sum()

	data['count_ratio'] = data['count'] / countSum
	data['revenue_ratio'] = data['revenue_d7'] / revenueSum
		
	print(countSum, revenueSum)
	print(data)

def f3(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir f3')
	app = appInfo['app']
	app_id = appInfo['app_id']

	# data = getDataFromMC(app,app_id)
	data = getTopwarDataFromMC2()
	data.rename(columns={
		'installs': 'uid'
	}, inplace=True)
	data['match_type'] = 'null'

	# data['match_type'] = data['match_type'].fillna('null')
	data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'].replace('tiktoklive_int', 'tiktokglobal_int', inplace=True)
	data['gp_referrer_media'].replace('tiktoklive_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'].replace('restricted', 'Facebook Ads', inplace=True)
	data['gp_referrer_media'].replace('organic', 'Organic', inplace=True)

	data['mediasource'] = data['mediasource'].fillna('Organic')

	# for debug
	# 将mediasource == Organic 且 gp_referrer_media == 'applovin_int'的部分进行过滤和保存
	debugDf01 = data[(data['mediasource'] == 'Organic')& (data['gp_referrer_media'] == 'applovin_int')]
	debugDf01.to_csv(f'/src/data/gpir_debug_01_{app}.csv', index=False)
	# print(debugDf01['uid'].sum())
	#
	return 
	data.replace('restricted', 'Facebook Ads', inplace=True)
	groupedByMediaAndGpMediaData = data.groupby(['gp_referrer_media', 'mediasource']).agg(
		total_count=('uid', 'sum'),
		total_revenue_d7=('revenue_d7', 'sum')
	).reset_index()

	groupedByGpMediaData = data.groupby(['gp_referrer_media']).agg(
		total_count=('uid', 'sum'),
		total_revenue_d7=('revenue_d7', 'sum')
	).reset_index()
	groupedByGpMediaData.rename(columns={'total_count': 'total_count_gp'}, inplace=True)
	groupedByGpMediaData.rename(columns={'total_revenue_d7': 'total_revenue_d7_gp'}, inplace=True)

	groupedByMediaAndGpMediaData = pd.merge(groupedByMediaAndGpMediaData, groupedByGpMediaData, on='gp_referrer_media', how='left')
	groupedByMediaAndGpMediaData['在gpir媒体中安装占比'] = groupedByMediaAndGpMediaData['total_count'] / groupedByMediaAndGpMediaData['total_count_gp']
	groupedByMediaAndGpMediaData['在gpir媒体中收入占比'] = groupedByMediaAndGpMediaData['total_revenue_d7'] / groupedByMediaAndGpMediaData['total_revenue_d7_gp']

	groupedByMediaData = data.groupby(['mediasource']).agg(
		total_count=('uid', 'sum'),
		total_revenue_d7=('revenue_d7', 'sum')
	).reset_index()
	groupedByMediaData.rename(columns={'total_count': 'total_count_media'}, inplace=True)
	groupedByMediaData.rename(columns={'total_revenue_d7': 'total_revenue_d7_media'}, inplace=True)
	groupedByMediaData.rename(columns={'mediasource': 'mmp_media'}, inplace=True)
		
	groupedByMediaAndGpMediaData = pd.merge(groupedByMediaAndGpMediaData, groupedByMediaData, left_on='mediasource', right_on='mmp_media', how='left')
	groupedByMediaAndGpMediaData['在mmp媒体中安装占比'] = groupedByMediaAndGpMediaData['total_count'] / groupedByMediaAndGpMediaData['total_count_media']
	groupedByMediaAndGpMediaData['在mmp媒体中收入占比'] = groupedByMediaAndGpMediaData['total_revenue_d7'] / groupedByMediaAndGpMediaData['total_revenue_d7_media']
		
	groupedByMediaAndGpMediaData = groupedByMediaAndGpMediaData.sort_values(by=['gp_referrer_media','total_count'], ascending=False)
	groupedByMediaAndGpMediaData = groupedByMediaAndGpMediaData[['gp_referrer_media','mmp_media','在gpir媒体中安装占比'	,'在gpir媒体中收入占比'	,'在mmp媒体中安装占比'	,'在mmp媒体中收入占比']]
	print(groupedByMediaAndGpMediaData)

	groupedByMediaAndGpMediaData.to_csv(f'/src/data/gpir_{app}.csv', index=False)

	groupedByMediaAndGpMediaData = groupedByMediaAndGpMediaData.sort_values(by=['mmp_media','在mmp媒体中安装占比'], ascending=False)	
	groupedByMediaAndGpMediaData.to_csv(f'/src/data/gpir_mmp_{app}.csv', index=False)

# 详细拆分GPIR与AF归因 安装数与收入金额 在各媒体间的改变
def f4(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir f4')
	app = appInfo['app']
	app_id = appInfo['app_id']

	# # data = getDataFromMC(app,app_id)
	# data = getTopwarDataFromMC()
	# data['match_type'] = 'null'

	# data['match_type'] = data['match_type'].fillna('null')
	# data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	# data['mediasource'].replace('restricted', 'Facebook Ads', inplace=True)
	# data['mediasource'] = data['mediasource'].fillna('organic')
	# data['gp_referrer_media'] = data['gp_referrer_media'].fillna('OTHER')

	data = getTopwarDataFromMC2()
	data.rename(columns={
		'installs': 'uid'
	}, inplace=True)
	
	data['gp_referrer_media'] = data['gp_referrer_media'].fillna('OTHER')
	data['mediasource'] = data['mediasource'].fillna('Organic')

	data.replace('restricted', 'Facebook Ads', inplace=True)

	# 按照 gp_referrer_media 分组
	groupedByGpMediaData = data.groupby(['gp_referrer_media']).agg(
		total_count_gp=('uid', 'sum'),
		total_revenue_d7_gp=('revenue_d7', 'sum')
	).reset_index()

	groupedByGpMediaData['gp_referrer_media'].replace('organic', 'Organic', inplace=True)

	print('groupedByGpMediaData')
	print(groupedByGpMediaData)

	# 按照 mediasource 分组
	groupedByMediaData = data.groupby(['mediasource']).agg(
		total_count_media=('uid', 'sum'),
		total_revenue_d7_media=('revenue_d7', 'sum')
	).reset_index()

	print('groupedByMediaData')
	print(groupedByMediaData)

	# 合并两个分组结果
	mergedData = pd.merge(groupedByGpMediaData, groupedByMediaData, left_on='gp_referrer_media', right_on='mediasource', how='outer')

	# 计算安装数和付费金额的差异比例
	mergedData['安装数差异比例'] = (mergedData['total_count_gp'] - mergedData['total_count_media']) / mergedData['total_count_media']
	mergedData['付费金额差异比例'] = (mergedData['total_revenue_d7_gp'] - mergedData['total_revenue_d7_media']) / mergedData['total_revenue_d7_media']

	# 处理 NaN 值
	mergedData['安装数差异比例'].fillna(0, inplace=True)
	mergedData['付费金额差异比例'].fillna(0, inplace=True)

	# 排序并选择需要的列
	mergedData = mergedData.sort_values(by=['gp_referrer_media', 'mediasource'], ascending=False)
	mergedData = mergedData[['mediasource', '安装数差异比例', '付费金额差异比例']]

	print(mergedData)

	# 保存结果到 CSV 文件
	mergedData.to_csv(f'/src/data/gpir_diff_{app}.csv', index=False)

# 详细分析AF归因中是organic，且GPIR中是非organic的情况
def organic(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir organic')
	app = appInfo['app']
	app_id = appInfo['app_id']

	data = getDataFromMC(app,app_id)

	data['mediasource'] = data['mediasource'].fillna('organic')
	data = data[
		(data['gp_referrer_media'] != 'organic')
		& (data['mediasource'] == 'organic')
	]

	print(data)


def organicDebug(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir organicDebug')
	app = appInfo['app']
	app_id = appInfo['app_id']

	sql = f'''
select
	customer_user_id,
	mediasource,
	match_type,
	gp_referrer,
	event_time,
	gg_install_begin_time as gp_install_begin_time
from
	(
		SELECT
			customer_user_id,
			gp_referrer,
			CASE
				WHEN gp_referrer IS NULL THEN 'organic'
				WHEN gp_referrer LIKE '%utm_source=google-play&utm_medium=organic%' THEN 'organic'
				ELSE 'OTHER'
			END AS gp_referrer_media,
			mediasource,
			match_type,
			get_day_from_timestamp(install_unix, '0') as install_day,
			event_time,
			gg_install_begin_time,
			DATEDIFF(
				to_date(event_time, 'yyyy-mm-dd hh:mi:ss'),
				to_date(gg_install_begin_time, 'yyyy-mm-dd hh:mi:ss')
			) as diff
		FROM
			rg_bi.ods_platform_appsflyer_events_v3
		WHERE
			app = '{app}'
			AND zone = '0'
			AND app_id = '{app_id}'
			AND day >= '20240701'
			AND event_name = 'install'
	)
where
	install_day between '20240701'
	and '20240731'
	and mediasource is null
	and gp_referrer_media <> 'organic'
	and diff < 6;
	'''
	print(sql)
	data = execSql(sql)
	print(data)
	data.to_csv(f'/src/data/gpir_organic_{app}.csv', index=False)


def organicDebug2():
	# 针对 AF归因为 organic，但是 GPIR归因是 applovin_int 的情况
	# 1、共有多少人，多少金额
	# 2、这些人中，从google play安装时间到游戏激活时间 超过7天的有多少人，多少金额
	# 3、再找到剩下的人，逐个观察

	sql = f'''
@gpir :=
select
	uid,
	mediasource as gpir_mediasource,
	get_day_from_timestamp(install_timestamp, '0') as install_day
from
	dws_overseas_gginstallbegin_contributor_attribution_th;



@af :=
select
	game_uid as uid,
	mediasource as af_mediasource
from
	dws_overseas_topheros_unique_uid
where
	app_package = 'com.greenmushroom.boomblitz.gp';



@purchase :=
select
	game_uid as uid,
	install_day,
	DATEDIFF(
		to_date(event_day, 'yyyymmdd'),
		to_date(install_day, 'yyyymmdd')
	) AS life_cycle,
	revenue_value_usd
from
	dwd_overseas_revenue_allproject
where
	app = '116'
	and zone = 0
	and app_package = 'com.greenmushroom.boomblitz.gp';



@purchase7 :=
select
	uid,
	install_day,
	sum(
		CASE
			WHEN life_cycle BETWEEN 0
			AND 6 THEN revenue_value_usd
			ELSE 0
		END
	) AS revenue_d7
from
	@purchase
group by
	uid,
	install_day;



@result01 :=
select
	af.uid,
	af.af_mediasource,
	gpir.gpir_mediasource,
	gpir.install_day,
	purchase7.revenue_d7
from
	@af as af
	left join @gpir as gpir on af.uid = gpir.uid
	left join @purchase7 as purchase7 on af.uid = purchase7.uid;


@result02 :=
select
	uid,
	af_mediasource,
	gpir_mediasource,
	install_day,
	revenue_d7
from
	@result01
where
	install_day between '20240701' and '20240731'
	and (af_mediasource is null or af_mediasource = 'Organic')
	and gpir_mediasource = 'applovin_int'
;

@push :=
select
	get_json_object(
		base64decode(base64decode(push_data)),
		'$.customer_user_id'
	) as uid,
	get_json_object(
		base64decode(base64decode(push_data)),
		'$.event_time'
	) as event_time,
	get_json_object(
		base64decode(base64decode(push_data)),
		'$.gp_install_begin'
	) as gp_install_begin
from
	rg_bi.ods_platform_appsflyer_push_event_total
where
	ds  > '20240920'
	and get_json_object(
		base64decode(base64decode(push_data)),
		'$.event_name'
	) = 'install'
	and get_json_object(
		base64decode(base64decode(push_data)),
		'$.app_id'
	) = 'com.greenmushroom.boomblitz.gp'
;

select
	result02.uid,
	result02.revenue_d7,
	push.event_time,
	push.gp_install_begin	
from @result02 as result02
	left join @push as push on result02.uid = push.uid
;
	'''
	print(sql)
	data = execSql(sql)

	data.to_csv(f'/src/data/gpir_organic_debug2.csv', index=False)

	return data


def organicDebug2_step2():
	df = pd.read_csv('/src/data/gpir_organic_debug2.csv')
	
	df['revenue_d7'] = df['revenue_d7'].fillna(0)

	df0 = df[df['event_time'].isna()]
	print(len(df0))

	df = df[~df['event_time'].isna()]
	# event_time 和 gp_install_begin 都是类似 '2024-07-07 06:43:08.804' 的字符串
	# 转换为 datetime 类型
	df['event_time'] = pd.to_datetime(df['event_time'])
	df['gp_install_begin'] = pd.to_datetime(df['gp_install_begin'])
	# print(df)
	# 计算两个时间的差值
	df['diff'] = df['event_time'] - df['gp_install_begin']
	df['diffDays'] = df['diff'].dt.days

	print(df['diffDays'].value_counts())

	# # # 找到差值大于7天的记录
	# df = df[df['diff'] > pd.Timedelta(days=7)]

	# print(len(df))
	print(df[df['diffDays'] < 7])

def debugAF():
	sql = '''
select
*
from ods_platform_appsflyer_events_v3
where app = 116
and event_name = 'install'
and customer_user_id in ('181626087255','38327101219','459773454276','460031272901','460034287556','466785413063','466792228807','466810972103','466820540359','466821261255'
)
;
	'''
	print(sql)
	data = execSql(sql)
	data.to_csv(f'/src/data/gpir_debug_af20240925.csv', index=False)

def debugAF2():
	df = pd.read_csv('/src/data/gpir_organic_debug2.csv')
	installs = df['uid'].unique()
	revenue = df['revenue_d7'].sum()
	arpu = revenue / len(installs)
	print(len(installs), revenue, arpu)

def lingqiang20240927():
	sql = '''
select
	*
from
	ods_platform_appsflyer_events_v3
where
	app = 116
	and day > 20240901
	and event_name = 'install'
	and mediasource is null
	and gp_referrer like '%applovin%'
	and DATEDIFF(
		to_date(event_time, 'yyyy-mm-dd hh:mi:ss'),
		to_date(gg_install_begin_time, 'yyyy-mm-dd hh:mi:ss')
	) <= 1
	and advertising_id not in (
		'0000-0000',
		'00000000-0000-0000-0000-000000000000'
	)
order by
	event_time desc
limit
	1000;
	'''
	df = execSql(sql)
	df.to_csv('/src/data/lingqiang20240927.csv', index=False)

if __name__ == '__main__':
	# debug2('topheros')
	# f3('topheros')
	# f4('topheros')

	# debug2('topheros')
	# f3('topheros')
	# f4('topheros')


	# organicDebug2()
	# organicDebug2_step2()
	# debugAF()
	# debugAF2()
	lingqiang20240927()



