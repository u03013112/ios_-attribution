import os
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

# 这个表没有最近3天的数据，而且最早的数据是2024年3月1日
def getDataFromMC(installDayStart = '20240701', installDayEnd = '20240731'):
	filename = f'/src/data/lastwar_gpir_{installDayStart}_{installDayEnd}.csv'
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
			event_time,
			ROW_NUMBER() OVER (
				PARTITION BY customer_user_id
				ORDER BY
					event_time ASC
			) AS rank
		FROM
			rg_bi.ods_platform_appsflyer_events_v3
		WHERE
			app = '502'
			AND zone = '0'
			AND app_id = 'com.fun.lastwar.gp'
			AND day >= '20240301'
			AND event_name = 'install'
	)
WHERE
	rank = 1;

@data2 :=
select
	customer_user_id,
	gp_referrer,
	match_type
from
	ods_platform_appsflyer_events_v3
where
	app = '502'
	and zone = '0'
	and app_id = 'com.fun.lastwar.gp'
	and day >= '20240301';

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
	app = '502'
	AND zone = 0
	AND app_package = 'com.fun.lastwar.gp'
	AND install_day >= '20240101'
	AND revenue_value_usd > 0;

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
	b.install_day,
	a.match_type,
	a.mediasource,
	a.contributor_1_mediasource,
	a.contributor_2_mediasource,
	a.contributor_3_mediasource,
	b.country,
	b.revenue_d1,
	b.revenue_d7
FROM
	@Data AS a
	JOIN @revenue AS b ON a.uid = b.uid;

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
		print(sql)
		data = execSql(sql)
		data.to_csv(filename, index=False)

	return data

def debug():
	data = getDataFromMC()
	data['mediasource'] = data['mediasource'].fillna('organic')
	df = data[data['gp_referrer_media'] == 'organic']
	df['count'] = 1
	df = df.groupby('mediasource').agg(
		{'count': 'sum'}
	).reset_index()
	df = df.sort_values(by='count', ascending=False)
	print(df)

# gpir的Other分析，包括安装数占比和收入占比
def debug2():
	data = getDataFromMC()
	
	data['count'] = 1
	data = data.groupby('gp_referrer_media').agg(
		{'count': 'sum', 'revenue_d7': 'sum'}
	).reset_index()
	data = data.sort_values(by='count', ascending=False)
	
	countSum = data['count'].sum()
	revenueSum = data['revenue_d7'].sum()

	data['count_ratio'] = data['count'] / countSum
	data['revenue_ratio'] = data['revenue_d7'] / revenueSum

	print(data)
		

def f1():
	data = getDataFromMC()
	data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'] = data['mediasource'].fillna('organic')
	data['contributor_1_mediasource'] = data['contributor_1_mediasource'].fillna('organic')
	data['contributor_2_mediasource'] = data['contributor_2_mediasource'].fillna('organic')
	data['contributor_3_mediasource'] = data['contributor_3_mediasource'].fillna('organic')

	# 根据math_type分组，统计media_source和gp_referrer_media一样的数量，和比例
		
	# 统计 media_source 和 gp_referrer_media 相同的数量
	data['is_same'] = data['mediasource'] == data['gp_referrer_media']
	data['contributor_1_same'] = data['gp_referrer_media'] == data['contributor_1_mediasource']
	data['contributor_2_same'] = data['gp_referrer_media'] == data['contributor_2_mediasource']
	data['contributor_3_same'] = data['gp_referrer_media'] == data['contributor_3_mediasource']
	data['contributor_same'] = data['contributor_1_same'] | data['contributor_2_same'] | data['contributor_3_same']


	# 按照match_type分组，统计数量和比例
	groupedByMatchTypeData = data.groupby('match_type').agg(
		 total_count=('uid', 'size'),
		 same_count=('is_same', 'sum')
	).reset_index()
		
	groupedByMatchTypeData['same_ratio'] = groupedByMatchTypeData['same_count'] / groupedByMatchTypeData['total_count']
	groupedByMatchTypeData = groupedByMatchTypeData.sort_values(by='total_count', ascending=False)
	print(groupedByMatchTypeData)

	# 逐个match_type分析
	# 找到每个match_type中，is_same == 0 的条目，10条~100条，然后观测
	for match_type in data['match_type'].unique():
		print(match_type)
		tmpDf = data[(data['match_type'] == match_type) & (data['is_same'] == False)]
		tmpDfForPrint = tmpDf[['mediasource', 'gp_referrer_media', 'country', 'contributor_1_mediasource', 'contributor_2_mediasource', 'contributor_3_mediasource']]
		# print(tmpDfForPrint.head(10))
	
		# print('contributor_1_same占比：', tmpDf['contributor_1_same'].sum() / tmpDf.shape[0])
		# print('contributor_2_same占比：', tmpDf['contributor_2_same'].sum() / tmpDf.shape[0])
		# print('contributor_3_same占比：', tmpDf['contributor_3_same'].sum() / tmpDf.shape[0])
		print('contributor_same占比：', tmpDf['contributor_same'].sum() / tmpDf.shape[0])

		print('---------------------------------')
		print('---------------------------------')

		
	# 得到结论：
	# gp_referrer 是媒体中的web没有正确归类，数量较少，忽略
	# srn 中有大约28%的数据，gp_referrer_media 出现在助攻名单中
	# id_matching 中有大约62%数据，gp_referrer_media 出现在助攻名单中
	# ig_referrer 中有大约50%数据，gp_referrer_media 出现在助攻名单中
	# fb_referrer 中有大约63%数据，gp_referrer_media 出现在助攻名单中
	# probabilistic 中有大约12%数据，gp_referrer_media 出现在助攻名单中


	# # 按照gp_referrer_media分组，统计数量和比例
	# groupedByMediaData = data.groupby(['gp_referrer_media']).agg(
	# 	total_count=('uid', 'size'),
	# 	same_count=('is_same', 'sum')
	# ).reset_index()

	# groupedByMediaData['same_ratio'] = groupedByMediaData['same_count'] / groupedByMediaData['total_count']
	# groupedByMediaData = groupedByMediaData.sort_values(by='total_count', ascending=False)
	# print(groupedByMediaData)

def f2():
	data = getDataFromMC()
	data['match_type'] = data['match_type'].fillna('null')
	data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
	data['mediasource'] = data['mediasource'].fillna('organic')
	data['is_same'] = data['mediasource'] == data['gp_referrer_media']

	groupedByMediaAndMatchTypeData = data.groupby(['mediasource', 'match_type']).agg(
		total_count=('uid', 'size'),
		same_count=('is_same', 'sum')
	).reset_index()

	groupedByMediaAndMatchTypeData['same_ratio'] = groupedByMediaAndMatchTypeData['same_count'] / groupedByMediaAndMatchTypeData['total_count']
	groupedByMediaAndMatchTypeData = groupedByMediaAndMatchTypeData.sort_values(by='total_count', ascending=False)
	groupedByMediaAndMatchTypeData = groupedByMediaAndMatchTypeData.sort_values(by=['mediasource', 'total_count'], ascending=False)
	print(groupedByMediaAndMatchTypeData)

def f3():
	data = getDataFromMC()
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

	groupedByMediaAndGpMediaData.to_csv('/src/data/gpir.csv', index=False)

	groupedByMediaAndGpMediaData = groupedByMediaAndGpMediaData.sort_values(by=['mmp_media','在mmp媒体中安装占比'], ascending=False)	
	groupedByMediaAndGpMediaData.to_csv('/src/data/gpir_mmp.csv', index=False)

def f4():
    data = getDataFromMC()
    data['match_type'] = data['match_type'].fillna('null')
    data['gp_referrer_media'].replace('bytedanceglobal_int', 'tiktokglobal_int', inplace=True)
    data['mediasource'].replace('restricted', 'Facebook Ads', inplace=True)
    data['mediasource'] = data['mediasource'].fillna('organic')

    # 按照 gp_referrer_media 分组
    groupedByGpMediaData = data.groupby(['gp_referrer_media']).agg(
        total_count_gp=('uid', 'size'),
        total_revenue_d7_gp=('revenue_d7', 'sum')
    ).reset_index()

    # 按照 mediasource 分组
    groupedByMediaData = data.groupby(['mediasource']).agg(
        total_count_media=('uid', 'size'),
        total_revenue_d7_media=('revenue_d7', 'sum')
    ).reset_index()

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
    mergedData = mergedData[['gp_referrer_media', 'mediasource', '安装数差异比例', '付费金额差异比例']]

    print(mergedData)

    # 保存结果到 CSV 文件
    mergedData.to_csv('/src/data/gpir_diff.csv', index=False)



if __name__ == '__main__':
	# f1()
	# f2()
	# f3()
	f4()

	# debug()
	# debug2()
