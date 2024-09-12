import os
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

appInfoList = [
	{'name':'lastwar','app':'502','app_id':'com.fun.lastwar.gp'},
	{'name':'topheros','app':'116','app_id':'com.greenmushroom.boomblitz.gp'},
	{'name':'topwar','app':'102','app_id':'com.topwar.gp'},
]

def getDataFromMC(app,app_id,installDayStart = '20240401', installDayEnd = '20240430'):
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


def getTopwarDataFromMC(installDayStart = '20240401', installDayEnd = '20240430'):
	filename = f'/src/data/gpir_topwar_{installDayStart}_{installDayEnd}.csv'
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
from dws_overseas_gginstallbegin_contributor_attribution_tw
;

@af :=
select
game_uid as uid,
mediasource as af_mediasource
from
tmp_unique_id
where
app = '102'
;

@purchase :=
select
game_uid as uid,
install_day,
DATEDIFF(
  to_date(purchase_day, 'yyyymmdd'),
  to_date(install_day, 'yyyymmdd')
) AS life_cycle,
revenue_value_usd
from
dwd_overseas_revenue_afattribution_realtime
where
app = '102'
and zone = 0
and window_cycle = 9999
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
where install_day between '20240401' and '20240430'
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
	data = getTopwarDataFromMC()
	data['match_type'] = 'null'

	data['match_type'] = data['match_type'].fillna('null')
	data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'].replace('restricted', 'Facebook Ads', inplace=True)
	data['mediasource'] = data['mediasource'].fillna('organic')

	groupedByMediaAndGpMediaData = data.groupby(['gp_referrer_media', 'mediasource']).agg(
		total_count=('uid', 'size'),
		total_revenue_d7=('revenue_d7', 'sum')
	).reset_index()

	groupedByGpMediaData = data.groupby(['gp_referrer_media']).agg(
		total_count=('uid', 'size'),
		total_revenue_d7=('revenue_d7', 'sum')
	).reset_index()
	groupedByGpMediaData.rename(columns={'total_count': 'total_count_gp'}, inplace=True)
	groupedByGpMediaData.rename(columns={'total_revenue_d7': 'total_revenue_d7_gp'}, inplace=True)

	groupedByMediaAndGpMediaData = pd.merge(groupedByMediaAndGpMediaData, groupedByGpMediaData, on='gp_referrer_media', how='left')
	groupedByMediaAndGpMediaData['在gpir媒体中安装占比'] = groupedByMediaAndGpMediaData['total_count'] / groupedByMediaAndGpMediaData['total_count_gp']
	groupedByMediaAndGpMediaData['在gpir媒体中收入占比'] = groupedByMediaAndGpMediaData['total_revenue_d7'] / groupedByMediaAndGpMediaData['total_revenue_d7_gp']

	groupedByMediaData = data.groupby(['mediasource']).agg(
		total_count=('uid', 'size'),
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

	# groupedByMediaAndGpMediaData = groupedByMediaAndGpMediaData.sort_values(by=['mmp_media','在mmp媒体中安装占比'], ascending=False)	
	# groupedByMediaAndGpMediaData.to_csv('/src/data/gpir_mmp.csv', index=False)

# 详细拆分GPIR与AF归因 安装数与收入金额 在各媒体间的改变
def f4(appName):
	appInfo = [x for x in appInfoList if x['name'] == appName][0]
	print(appInfo,'gpir f4')
	app = appInfo['app']
	app_id = appInfo['app_id']

	# data = getDataFromMC(app,app_id)
	data = getTopwarDataFromMC()
	data['match_type'] = 'null'

	data['match_type'] = data['match_type'].fillna('null')
	data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'].replace('restricted', 'Facebook Ads', inplace=True)
	data['mediasource'] = data['mediasource'].fillna('organic')
	data['gp_referrer_media'] = data['gp_referrer_media'].fillna('OTHER')

	# 按照 gp_referrer_media 分组
	groupedByGpMediaData = data.groupby(['gp_referrer_media']).agg(
		total_count_gp=('uid', 'size'),
		total_revenue_d7_gp=('revenue_d7', 'sum')
	).reset_index()

	print('groupedByGpMediaData')
	print(groupedByGpMediaData)

	# 按照 mediasource 分组
	groupedByMediaData = data.groupby(['mediasource']).agg(
		total_count_media=('uid', 'size'),
		total_revenue_d7_media=('revenue_d7', 'sum')
	).reset_index()

	print('groupedByMediaData')
	print(groupedByMediaData)

	# 合并两个分组结果
	mergedData = pd.merge(groupedByGpMediaData, groupedByMediaData, left_on='gp_referrer_media', right_on='mediasource', how='inner')

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


if __name__ == '__main__':
	# debug2('lastwar')
	# f3('lastwar')
	# f4('lastwar')

	debug2('topwar')
	f3('topwar')
	f4('topwar')

