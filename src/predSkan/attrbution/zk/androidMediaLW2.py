# 按照国家撞库
# 与androidGeo.py区别是，撞库的维度是campaign，不再是媒体

# 思路
# 1、安卓数据->skan报告
# 2、只针对3个媒体，Facebook Ads,googleadwords_int,bytedanceglobal_int
# 3、针对几个比较大的campaign，这个需要计算一下，考虑每个媒体选取前2个campaign
# 4、针对6个campaign，分别计算每个campaign的MAPE

# 对CV进行一定的改进，目前仍保持32档，
# 但是针对首日不付费用户在进行分组，分为7日内不付费和7日内付费两组
# 由于7日付费是预测值，所以不可能非常准确，所以要测试一下各种准确率的情况
# 主要针对不同的 查准率和查全率 进行分组

import io
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import pytz

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

installTimeStart = '2024-01-01'
installTimeEnd = '2024-01-31'

filename = getFilename(f'androidFp{installTimeStart}_{installTimeEnd}')

def getDataFromMC():
    global installTimeStart,installTimeEnd
    global filename

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
	campaign_id,
	install_timestamp,
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

select
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
        '''
        print(sql)
        df = execSql(sql)
        df['country_code'] = ''
        df.to_csv(filename, index=False)
    return df

def loadData():
    # 加载数据
    print('loadData from file:',filename)
    df = pd.read_csv(filename)
    print('loadData done')
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    return df

def getCvMap():
    csv_str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2023-11-28 11:13:03,0,,,
id6448786147,1,af_skad_revenue,0,1,0,0.99,0,24,2023-11-28 11:13:03,0,,,
id6448786147,2,af_skad_revenue,0,1,0.99,1.15,0,24,2023-11-28 11:13:03,0,,,
id6448786147,3,af_skad_revenue,0,1,1.15,1.3,0,24,2023-11-28 11:13:03,0,,,
id6448786147,4,af_skad_revenue,0,1,1.3,2.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,5,af_skad_revenue,0,1,2.98,3.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,6,af_skad_revenue,0,1,3.41,5.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,7,af_skad_revenue,0,1,5.98,7.46,0,24,2023-11-28 11:13:03,0,,,
id6448786147,8,af_skad_revenue,0,1,7.46,9.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,9,af_skad_revenue,0,1,9.09,12.05,0,24,2023-11-28 11:13:03,0,,,
id6448786147,10,af_skad_revenue,0,1,12.05,14.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,11,af_skad_revenue,0,1,14.39,18.17,0,24,2023-11-28 11:13:03,0,,,
id6448786147,12,af_skad_revenue,0,1,18.17,22.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,13,af_skad_revenue,0,1,22.07,26.57,0,24,2023-11-28 11:13:03,0,,,
id6448786147,14,af_skad_revenue,0,1,26.57,32.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,15,af_skad_revenue,0,1,32.09,37.42,0,24,2023-11-28 11:13:03,0,,,
id6448786147,16,af_skad_revenue,0,1,37.42,42.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,17,af_skad_revenue,0,1,42.94,50.34,0,24,2023-11-28 11:13:03,0,,,
id6448786147,18,af_skad_revenue,0,1,50.34,58.56,0,24,2023-11-28 11:13:03,0,,,
id6448786147,19,af_skad_revenue,0,1,58.56,67.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,20,af_skad_revenue,0,1,67.93,80.71,0,24,2023-11-28 11:13:03,0,,,
id6448786147,21,af_skad_revenue,0,1,80.71,100.32,0,24,2023-11-28 11:13:03,0,,,
id6448786147,22,af_skad_revenue,0,1,100.32,116.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,23,af_skad_revenue,0,1,116.94,130.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,24,af_skad_revenue,0,1,130.41,153.76,0,24,2023-11-28 11:13:03,0,,,
id6448786147,25,af_skad_revenue,0,1,153.76,196.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,26,af_skad_revenue,0,1,196.39,235.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,27,af_skad_revenue,0,1,235.93,292.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,28,af_skad_revenue,0,1,292.07,424.48,0,24,2023-11-28 11:13:03,0,,,
id6448786147,29,af_skad_revenue,0,1,424.48,543.77,0,24,2023-11-28 11:13:03,0,,,
id6448786147,30,af_skad_revenue,0,1,543.77,753.61,0,24,2023-11-28 11:13:03,0,,,
id6448786147,31,af_skad_revenue,0,1,753.61,1804,0,24,2023-11-28 11:13:03,0,,,
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf

def makeLevels1(userDf, usd='r1usd', N=32):
    # `makeLevels1`函数接受一个包含用户数据的DataFrame（`userDf`），一个表示用户收入的列名（`usd`，默认为'r1usd'），以及分组的数量（`N`，默认为8）。
    # 其中第0组特殊处理，第0组是收入等于0的用户。
    # 过滤收入大于0的用户进行后续分组，分为N-1组，每组的总收入大致相等。
    # 根据收入列（`usd`）对用户DataFrame（`userDf`）进行排序。
    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值。
    # 计算所有这些用户的总收入。
    # 计算每组的目标收入（总收入除以分组数量）。
    # 初始化当前收入（`current_usd`）和组索引（`group_index`）。
    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入。然后，将该用户的收入值存储到`levels`数组中，并将当前收入重置为0，组索引加1。当组索引达到N-1时，停止遍历。
    # 返回`levels`数组。
    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    return levels

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    userDfCopy[cv] = 0
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
        
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    return userDfCopy


def addCv2(userDf,usd='r7usd',cv='cv', p_pos=1.00, r_pos=1.00):
    # 先进行旧有逻辑，addCv，对用户的24小时付费进行分档
    # 然后再针对cv == 0 的用户，进行新的分档
    # 其中 usd 列 == 0的用户，cv列不变
    # usd列 > 0 的用户，cv列 = cv列 + 32
    
    # 但是由于7日付费金额是预测的，所以不可能非常准确，所以要测试一下各种准确率的情况
    # 即随机的进行一些错误的分档
    # 按照参数中的准确率与查全率，对cv == 0的用户进行分档，例如当p==1.00,r==1.00时，cv == 0的用户的分档是完全正确的
    
    # 针对 cv == 0 的用户进行新的分档
    cv_zero_users = userDf[userDf[cv] == 0].copy()
    
    # 正样本和负样本
    pos_users = cv_zero_users[cv_zero_users[usd] > 0]
    neg_users = cv_zero_users[cv_zero_users[usd] == 0]
    
    total_pos_users = len(pos_users)
    
    # 1. 根据查准率计算
    num_correct_pos = int(total_pos_users * p_pos)
    correctPosDf = pos_users.sample(num_correct_pos, replace=False)
    
    # 2. 根据查全率计算
    num_to_classify_pos = int(total_pos_users * r_pos)
    num_incorrect_pos_from_neg = num_to_classify_pos - num_correct_pos
    incorrectNegDf = neg_users.sample(num_incorrect_pos_from_neg, replace=False)

    # 3. 合并
    needMarkPosDf = pd.concat([correctPosDf, incorrectNegDf])
    
    # 4. 标记
    userDf.loc[needMarkPosDf.index, cv] += 32
    
    return userDf


def dataStep1_24():
    df = pd.read_csv(filename, converters={'campaign_id':str})
    df.loc[df['last_timestamp'] == 0, 'last_timestamp'] = df['install_timestamp']
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    # levels = makeLevels1(df,usd='r1usd',N=32)
    # cvMapDf = makeCvMap(levels)
    cvMapDf = getCvMap()
    cvMapDf.rename(columns={'conversion_value':'cv'},inplace=True)
    df = addCv(df,cvMapDf,usd='r1usd',cv='cv')
    
    df.to_csv(getFilename('androidFpMergeDataStep1Campaign24'), index=False)
    print('dataStep1 24h done')    
    return df

def dataStep1_48():
    df = pd.read_csv(filename, converters={'campaign_id':str})
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r2usd',cv='cv')
    
    df.to_csv(getFilename('androidFpMergeDataStep1Campaign48'), index=False)
    print('dataStep1 48h done')    
    return df

def dataStep2(df):
    df.rename(columns={'media_source':'media'},inplace=True)
    df = df [[
        'uid',
        'install_date',
        'r1usd',
        'r2usd',
        'r7usd',
        'r30usd',
        'install_timestamp',
        'last_timestamp',
        'media',
        'cv',
        'country_code',
        'campaign_id',
    ]]
    df.to_csv(getFilename('androidFpMergeDataStep2Media'), index=False)
    print('dataStep2 done')
    return df

# 制作一个模拟的SKAN报告
def makeSKAN(df):
    df = df.loc[df['media'].isna() == False]
    df = df.loc[df['media'] != 'organic']
    # 重排索引
    df = df.reset_index(drop=True)
    cvDf = df

    # 添加postback_timestamp
    # 如果用户的r1usd == 0，postback_timestamp = install_timestamp + 24小时 + 0~24小时之间随机时间
    # 如果用户的r1usd > 0，postback_timestamp = last_timestamp + 24小时 + 0~24小时之间随机时间
    # 添加postback_timestamp
    zero_r1usd_mask = cvDf['r1usd'] == 0
    non_zero_r1usd_mask = cvDf['r1usd'] > 0

    cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=zero_r1usd_mask.sum())
    # cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'last_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=non_zero_r1usd_mask.sum())
    cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 48 * 3600, size=non_zero_r1usd_mask.sum())

    # postback_timestamp 转成 int
    cvDf['postback_timestamp'] = cvDf['postback_timestamp'].astype(int)

    # print(cvDf.head(30))

    skanDf = cvDf[['uid','postback_timestamp','media','cv','campaign_id','country_code']]
    skanDf.to_csv(getFilename('skan2'), index=False)
    return skanDf

# 计算合法的激活时间范围
def skanAddValidInstallDate(skanDf):
    # 计算skan报告中的用户有效激活时间范围
    # 具体方式如下
    # cv = 0 的用户，有效时间范围是 (postback时间 - 48小时) ~ (postback时间 - 24小时)
    # cv > 0 的用户，有效时间范围是 (postback时间 - 72小时) ~ (postback时间 - 24小时)
    # 将每个用户的有效范围记录到skanDf中，记作min_valid_install_timestamp和max_valid_install_timestamp
    # 为了方便查看，请将postback时间戳和min_valid_install_timestamp和max_valid_install_timestamp都转换为日期格式也记录到skanDf中
    # 命名为postback_date，min_valid_install_date，max_valid_install_date
    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 48*3600).astype(int)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 72*3600).astype(int)
    skanDf.loc[:, 'max_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 24*3600).astype(int)

    # 将时间戳转换为日期格式
    skanDf['postback_date'] = pd.to_datetime(skanDf['postback_timestamp'], unit='s')
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')
    return skanDf

# 激活时间范围 进行近似
# 近似成分钟
def skanValidInstallDate2Min(skanDf,N = 60):
    # skanDf 中列 min_valid_install_timestamp 和 max_valid_install_timestamp 按分钟取整
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // N * N
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // N * N

    S2 = 24 * N
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] // S2) * S2
    skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] // S2) * S2
    

    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')

    return skanDf

# skan数据分组，按照media,cv,min_valid_install_timestamp和max_valid_install_timestamp分组，统计每组的用户数
def skanGroupby(skanDf):
    skanDf['user_count'] = 1

    skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id']).agg(
        {'user_count': 'sum'}
    ).reset_index()

    return skanGroupbyDf

def skanAddGeo(skanDf):
    print('skanAddGeo')
    campaignGeo2Df = pd.read_csv(getFilename('campaignGeo2'), converters={'campaign_id':str})
    campaignGeo2Df['day'] = pd.to_datetime(campaignGeo2Df['day'], format='%Y%m%d')

    # min_valid_install_timestamp 向前推7天，因为广告的转化窗口是7天
    # 但实际确实发现有部分转化时间超过7天的，这里放宽到8天
    skanDf['min_valid_install_timestamp'] -= 8*24*3600
    
    # 将时间戳列转换为datetime格式
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_timestamp'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')

    unmatched_rows = 0

    # 定义一个函数，用于根据campaign_id和时间戳范围查找匹配的country_code_list
    def get_country_code_list(row):
        matched_rows = campaignGeo2Df[
            (campaignGeo2Df['campaign_id'] == row['campaign_id']) &
            (campaignGeo2Df['day'] >= row['min_valid_install_timestamp']) &
            (campaignGeo2Df['day'] <= row['max_valid_install_timestamp'])
        ]

        if matched_rows.empty:
            # print('No matched rows for row: ', row)
            nonlocal unmatched_rows
            unmatched_rows += 1

        # 合并所有匹配行的country_code_list，排序并去重
        country_codes = set()
        for country_code_list in matched_rows['country_code_list']:
            country_codes.update(country_code_list.split('|'))

        return '|'.join(sorted(country_codes))

    # 应用函数，将匹配的country_code_list添加到skanDf
    tqdm.pandas(desc="Processing rows")
    skanDf['country_code_list'] = skanDf.progress_apply(get_country_code_list, axis=1)

    # 将min_valid_install_timestamp 和 max_valid_install_timestamp 重新转换为时间戳格式，单位秒
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'].astype(np.int64) // 10 ** 9

    # min_valid_install_timestamp 恢复，将上面减去的8天加回来
    skanDf['min_valid_install_timestamp'] += 8*24*3600

    # 计算未匹配的行数在总行数中的占比
    unmatched_rows_ratio = unmatched_rows / len(skanDf)

    # 在函数结束时打印未匹配的行数以及未匹配的行数在总行数中的占比
    print(f"Unmatched rows: {unmatched_rows}")
    print(f"Unmatched rows ratio: {unmatched_rows_ratio:.2%}")

    return skanDf
    

# 制作待归因用户Df
def makeUserDf(df):
    userDf = df
    userDf = userDf[['uid','install_timestamp','r1usd','r2usd','r7usd','r30usd','cv','country_code','campaign_id','media']].copy()
    userDf['cv'] = userDf['cv'].astype(int)
    return userDf

def userInstallDate2Min(userDf,N = 60):
    userDf['install_timestamp'] = userDf['install_timestamp'] // N * N

    S2 = 24 * N
    userDf.loc[userDf['cv'] == 0, 'install_timestamp'] = (userDf.loc[userDf['cv'] == 0, 'install_timestamp'] // S2) * S2
    
    return userDf

# user数据分组，按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
def userGroupby(userDf):
    # 按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
    # 将分组结果保存到userGroupbyDf中
    # userGroupbyDf的列名为install_timestamp,cv,user_count和r7usd
    # user_count是每组的用户数
    # r7usd是每组的r7usd汇总
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'uid':'count','r1usd':'sum','r2usd':'sum','r7usd':'sum','r30usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'uid':'user_count'}, inplace=True)
    return userGroupbyDf

from tqdm import tqdm
import gc
def meanAttributionFastv2(userDf, skanDf):
    userDf = userDf.copy(deep=True)
    pending_skan_indices = skanDf.index.tolist()
    N = 10
    attributeDf = pd.DataFrame(columns=['user index', 'media', 'skan index', 'rate'])

    mediaList = skanDf.loc[~skanDf['media'].isnull()]['media'].unique().tolist()
    # print('mediaList:',mediaList)

    userDf['total media rate'] = 0
    for media in mediaList:
        userDf['%s rate'%(media)] = 0

    for i in range(N):  
        user_indices = []
        medias = []
        skan_indices = []
        rates = []
        print(f"开始第 {i + 1} 次分配")
        new_pending_skan_indices = []
        skanDf_to_process = skanDf.loc[pending_skan_indices]
        print(f"待处理的skanDf行数：{len(skanDf_to_process)}")
        
        # 在每次循环开始时，预先计算每一行的media rate的总和
        # userDf['total media rate'] = userDf.apply(lambda x: sum([x[media + ' rate'] for media in mediaList]), axis=1)
        for media in mediaList:
            userDf['total media rate'] = userDf['total media rate'] + userDf[media + ' rate']
        
        print('第%d次分配，时间范围向前推%d小时'%(i+1,i*12))
        
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            media = str(item['media'])
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            min_valid_install_timestamp -= i*12*3600

            condition_rate = userDf['total media rate'] < 0.95
            condition_time = (userDf['install_timestamp'] >= min_valid_install_timestamp) & (userDf['install_timestamp'] <= max_valid_install_timestamp)
            condition_cv = userDf['cv'] == cv if cv >= 0 else pd.Series([True] * len(userDf))

            if cv < 0:
                    condition = condition_rate & condition_time
            else:
                condition = condition_rate & condition_time & condition_cv
                
            matching_rows = userDf[condition]
            total_matching_count = matching_rows['user_count'].sum()

            if total_matching_count > 0:
                rate = item['user_count'] / total_matching_count

                userDf.loc[condition, 'total media rate'] += rate
                user_indices.extend(matching_rows.index)
                medias.extend([media] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
                # print(user_indices)
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        # # 将未分配成功的skan的前10条打印出来
        # print(skanDf_to_process.loc[new_pending_skan_indices].head(10))

        attributeDf2 = pd.DataFrame({'user index': user_indices, 'media': medias, 'skan index': skan_indices, 'rate': rates})
        print('step 1')
        attributeDf = attributeDf.append(attributeDf2, ignore_index=True)
        print('step 2')
        # 找出需要重新分配的行
        grouped_attributeDf = attributeDf.groupby('user index')['rate'].sum()
        index_to_redistribute = grouped_attributeDf[grouped_attributeDf > 1].index
        sorted_rows_to_redistribute = attributeDf[attributeDf['user index'].isin(index_to_redistribute)].sort_values(
            ['user index', 'rate'], ascending=[True, False])
        sorted_rows_to_redistribute['cumulative_rate'] = sorted_rows_to_redistribute.groupby('user index')['rate'].cumsum()
        # 找出需要移除的行
        rows_to_remove = sorted_rows_to_redistribute[sorted_rows_to_redistribute['cumulative_rate'] > 1]
        print('step 3')
        # 记录需要移除的skan index
        removed_skan_indices = set(rows_to_remove['skan index'])
        print('step 4')
        # 从attributeDf中移除这些行
        attributeDf = attributeDf[~attributeDf['skan index'].isin(removed_skan_indices)]
        print('移除过分配的skan：', len(removed_skan_indices),'条')

        # 更新待分配的skan索引列表
        pending_skan_indices = list(set(new_pending_skan_indices).union(removed_skan_indices))

        print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
        
        # 更新media rate
        for media in mediaList:
            userDf[media + ' rate'] = 0
            userDf[media + ' rate'] = attributeDf[attributeDf['media'] == media].groupby('user index')['rate'].sum()
            userDf[media + ' rate'] = userDf[media + ' rate'].fillna(0)
        
        # 计算每个媒体的未分配的用户数
        pending_counts = skanDf.loc[pending_skan_indices].groupby('media')['user_count'].sum()
        pending_counts = pending_counts.fillna(0)
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['user_count'].sum()
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        # 将三个计算结果合并为一个DataFrame
        result_df = pd.concat([total_counts, pending_counts, pending_ratios], axis=1)
        # 设置列名和索引
        result_df.columns = ['总skan用户数', '未分配用户数', '未分配比例']
        result_df.index.name = 'media'
        # 将未分配比例转换为2位小数的百分比
        result_df['未分配比例'] = result_df['未分配比例'].apply(lambda x: f"{x*100:.2f}%")

        # 打印结果
        # print(result_df.sort_values('未分配用户数', ascending=False))

        # 计算所有的未分配用户占比
        total_pending_ratio = pending_counts.sum() / total_counts.sum()
        print("所有的未分配用户占比：")
        print(total_pending_ratio)

        gc.collect()

        if len(pending_skan_indices) == 0:
            print('所有的skan都已经分配完毕')
            break
    

    skanFailedDf = skanDf.loc[pending_skan_indices]
    skanFailedDf['postback_timestamp'] = 0

    mediaRateList = userDf.filter(like='rate', axis=1).columns.tolist()
    # 如果campaignIdRateList中包含'total media rate'，则删除
    if 'total media rate' in mediaRateList:
        mediaRateList.remove('total media rate')
    # userDf = userDf[['customer_user_id', 'install_date', 'day'] + campaignIdRateList]
    # print(userDf.columns)
    # 类型优化
    for col in mediaRateList:
        # 原本是float64，转换为float32，精度足够
        userDf[col] = userDf[col].astype('float32')

    return userDf.reset_index(drop=True)


def meanAttributionResult(userDf,mediaList):
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    for media in mediaList:
        col = media + ' rate'
        userDf[media + ' r1usd'] = userDf['r1usd'] * userDf[col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[col]
        userDf[media + ' r30usd'] = userDf['r30usd'] * userDf[col]
        userDf[media + ' user_count'] = userDf['user_count'] * userDf[col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDf_r1usd = userDf[['install_date'] + [media + ' r1usd' for media in mediaList]]
    userDf_r7usd = userDf[['install_date'] + [media + ' r7usd' for media in mediaList]]
    userDf_r30usd = userDf[['install_date'] + [media + ' r30usd' for media in mediaList]]

    # 对两个子数据框分别进行melt操作
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='media', value_name='r1usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r1usd', '')
    userDf_r1usd = userDf_r1usd.groupby(['install_date', 'media']).sum().reset_index()
    
    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf_r7usd['media'] = userDf_r7usd['media'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'media']).sum().reset_index()

    userDf_r30usd = userDf_r30usd.melt(id_vars=['install_date'], var_name='media', value_name='r30usd')
    userDf_r30usd['media'] = userDf_r30usd['media'].str.replace(' r30usd', '')
    userDf_r30usd = userDf_r30usd.groupby(['install_date', 'media']).sum().reset_index()

    # 将两个子数据框连接在一起
    userDf = userDf_r1usd.merge(userDf_r7usd, on=['install_date', 'media'])
    userDf = userDf.merge(userDf_r30usd, on=['install_date', 'media'])
    
    # Save to CSV
    userDf.to_csv(getFilename('attribution1RetMedia'), index=False)
    return userDf

from sklearn.metrics import r2_score
def checkRet(retDf,prefix='attMedia24_'):
    # 读取原始数据
    rawDf = loadData()
    rawDf = rawDf.loc[rawDf['media'].isna() == False]

    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['media','install_date']).agg(
        {
            'r1usd': 'sum',
            'r2usd': 'sum',
            'r7usd': 'sum',
            'r30usd': 'sum',
            'user_count':'sum'
        }).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date'], how='left',suffixes=('', 'p'))
    
    # 按月汇总，然后计算MAPE
    rawDf['Month'] = pd.to_datetime(rawDf['install_date']).dt.to_period('M')
    rawDf = rawDf.groupby(['media','Month']).agg(
        {
            'r7usd': 'sum',
            'r7usdp': 'sum',
            'r30usd': 'sum',
            'r30usdp': 'sum',
            'user_count':'sum'
        }).reset_index()
    rawDf['MAPE7'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf['MAPE30'] = abs(rawDf['r30usd'] - rawDf['r30usdp']) / rawDf['r30usd']
    print(rawDf)
    rawDf.to_csv(getFilename(prefix+f'groupbyMedia_Mape_{installTimeStart}_{installTimeEnd}'), index=False)

    rawTotalDf = rawDf.groupby(['Month']).agg(
        {
            'r7usd': 'sum',
            'r7usdp': 'sum',
            'r30usd': 'sum',
            'r30usdp': 'sum',
            'user_count':'sum'
        }).reset_index()
    rawTotalDf['MAPE7'] = abs(rawTotalDf['r7usd'] - rawTotalDf['r7usdp']) / rawTotalDf['r7usd']
    rawTotalDf['MAPE30'] = abs(rawTotalDf['r30usd'] - rawTotalDf['r30usdp']) / rawTotalDf['r30usd']
    print(rawTotalDf)
    rawTotalDf.to_csv(getFilename(prefix+f'total_Mape_{installTimeStart}_{installTimeEnd}'), index=False)


# 按照Media进行分组
def checkRetByMedia(retDf,prefix='attCampaign24_'):
    # 读取原始数据
    rawDf = loadData()

    print(rawDf)

    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['campaign_id', 'media_source','install_date']).agg(
        {
            'r1usd': 'sum',
            'r2usd': 'sum',
            'r7usd': 'sum',
            'user_count':'sum'
        }).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['campaign_id', 'install_date'], how='left',suffixes=('', 'p'))
    # 按照media_source进行分组
    rawDf = rawDf.groupby(['media_source','install_date']).agg({'r7usd': 'sum','r7usdp': 'sum'}).reset_index()
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    # rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf = rawDf.loc[rawDf['MAPE']>0]
    rawDf.to_csv(getFilename(prefix+'attribution24RetCheckByMedia'), index=False)

    for media in rawDf['media_source'].unique():
        mediaDf = rawDf[rawDf['media_source'] == media].copy()
        MAPE = mediaDf['MAPE'].mean()
        print(f"\nmedia:{media}:")
        print(f"MAPE: {MAPE}")

        # 计算r7usd和r7usdp的7日均线的MAPE
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']
        MAPE7 = mediaDf['MAPE7'].mean()
        print(f"MAPE7: {MAPE7}")
        
    return 


import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def draw24(df,prefix='attCampaign24_',label = 'r1usd'):
    # df = pd.read_csv(getFilename('attribution24RetCheck'), converters={'campaign_id':str})
    # 将df中的campaign_id转成str类型
    df['campaign_id'] = df['campaign_id'].astype(str)

    # 将不同的媒体分开画图，图片宽一点
    # install_date作为x轴，每隔7天画一个点
    # 双y轴，y1是r7usd和r7usdp；y2是MAPE（用虚线）。
    # 图片保存到'/src/data/zk/att1_{media}.jpg'
    # Convert 'install_date' to datetime
    df['install_date'] = pd.to_datetime(df['install_date'])

    campaignList = getChooseCampaignList()
    for campaignId in campaignList:
        media_df = df[df['campaign_id'] == campaignId]
        
        # Create the plot with the specified figure size
        fig, ax1 = plt.subplots(figsize=(24, 6))

        plt.title(campaignId)

        # Plot r7usd and r7usdp on the left y-axis
        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd')
        ax1.plot(media_df['install_date'], media_df['r7usdp'], label='r7usdp')
        ax1.plot(media_df['install_date'], media_df['r2usd'], label=label)
        ax1.set_ylabel('r7usd and r7usdp')
        ax1.set_xlabel('Install Date')

        # Plot MAPE on the right y-axis with dashed line
        ax2 = ax1.twinx()
        ax2.plot(media_df['install_date'], media_df['MAPE'], label='MAPE', linestyle='--', color='red')
        ax2.set_ylabel('MAPE')

        # Set x-axis to display dates with a 7-day interval
        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(media_df['install_date'][::14], rotation=45)

        # Add legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

        # Save the plot as a jpg image
        plt.savefig(f'/src/data/zk2/{prefix}{campaignId}.jpg', bbox_inches='tight')
        plt.close()

        # 再画一张，r7usd和r7usdp的7日均线的图

        fig, ax3 = plt.subplots(figsize=(24, 6))

        mediaDf = media_df.copy()
        mediaDf['r2usd7'] = mediaDf['r2usd'].rolling(7).mean()
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']

        plt.title(campaignId + 'rolling 7 days')

        ax3.plot(mediaDf['install_date'], mediaDf['r7usd7'], label='r7usd7')
        ax3.plot(mediaDf['install_date'], mediaDf['r7usdp7'], label='r7usdp7')
        ax3.plot(mediaDf['install_date'], mediaDf['r2usd7'], label=f'{label}7')
        ax3.set_ylabel('r7usd7 and r7usdp7')
        ax3.set_xlabel('Install Date')

        ax4 = ax3.twinx()
        ax4.plot(mediaDf['install_date'], mediaDf['MAPE7'], label='MAPE7', linestyle='--', color='red')
        ax4.set_ylabel('MAPE7')

        # Set x-axis to display dates with a 7-day interval
        ax3.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(mediaDf['install_date'][::14], rotation=45)

        # Add legends
        lines1, labels1 = ax3.get_legend_handles_labels()
        lines2, labels2 = ax4.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc='best')

        # Save the plot as a jpg image
        plt.savefig(f'/src/data/zk2/{prefix}rolling7_{campaignId}.jpg', bbox_inches='tight')
        plt.close()

pAndRList = [
    {'p':1.0,'r':1.0},
    {'p':0.8,'r':0.8},
    {'p':0.6,'r':0.6},

    {'p':0.5,'r':0.9},
    {'p':0.3,'r':0.9},
]

def main24(fast = False,onlyCheck = False):
    if onlyCheck == False:
        if fast == False:
            # 24小时版本归因
            getDataFromMC()
            
            df = dataStep1_24()
            df2 = dataStep2(df)

            for pAndR in pAndRList:
                p = pAndR['p']
                r = pAndR['r']
                df2 = addCv2(df2,p_pos=p,r_pos=r)

                skanDf = makeSKAN(df2)
                skanDf = skanAddValidInstallDate(skanDf)

                print('skan data len:',len(skanDf))
                
                skanDf = skanValidInstallDate2Min(skanDf,N = 3600)
                skanDf = skanGroupby(skanDf)
                skanDf.to_csv(getFilename(f'skanAOSCampaignG24_{p:.1f}_{r:.1f}'),index=False)
                print('skan data group len:',len(skanDf))

                userDf = makeUserDf(df2.copy())
                print('user data len:',len(userDf))
                
                userDf = userInstallDate2Min(userDf,N = 3600)
                userDf = userGroupby(userDf)
                userDf.to_csv(getFilename(f'userAOSCampaignG24_{p:.1f}_{r:.1f}'),index=False)
                print('user data group len:',len(userDf))
        
        mediaList = skanDf.loc[~skanDf['media'].isnull()]['media'].unique().tolist()

        for pAndR in pAndRList:
            p = pAndR['p']
            r = pAndR['r']

            print(f'-=-=-=-=-=-=-=-=-=> p:{p},r:{r}')

            userDf = pd.read_csv(getFilename(f'userAOSCampaignG24_{p:.1f}_{r:.1f}'))
            skanDf = pd.read_csv(getFilename(f'skanAOSCampaignG24_{p:.1f}_{r:.1f}'))

            userDf2 = meanAttributionFastv2(userDf, skanDf)
            meanAttributionFastv2Filename = getFilename(f'meanAttribution24_{installTimeStart}_{installTimeEnd}_{pAndR["p"]:.1f}_{pAndR["r"]:.1f}')
            userDf2.to_csv(meanAttributionFastv2Filename, index=False)
            userDf2 = pd.read_csv(meanAttributionFastv2Filename)
            userDf2 = meanAttributionResult(userDf2,mediaList)
            meanAttributionResultFilename = getFilename(f'meanAttributionResult24_{installTimeStart}_{installTimeEnd}_{pAndR["p"]:.1f}_{pAndR["r"]:.1f}')
            userDf2.to_csv(meanAttributionResultFilename, index=False)

            userDf2 = pd.read_csv(meanAttributionResultFilename)
            df = checkRet(userDf2,prefix=f'attMedia24_{installTimeStart}_{installTimeEnd}_{pAndR["p"]:.1f}_{pAndR["r"]:.1f}_')

def main48(fast = False,onlyCheck = False):
    if onlyCheck == False:
        if fast == False:
            # 48小时版本归因
            # getDataFromMC()
            # getCountryFromCampaign()
            # getCountryFromCampaign2()

            df = dataStep1_48()
            df2 = dataStep2(df)

            # 做一下简单的campaign过滤，只获得选中的campaign
            campaignList = getChooseCampaignList()
            df3 = df2.loc[df2['campaign_id'].isin(campaignList)].copy()
            skanDf = makeSKAN(df3)
            skanDf = skanAddValidInstallDate(skanDf)

            print('skan data len:',len(skanDf))
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 3600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG48'),index=False)
            print('skan data group len:',len(skanDf))

            skanDf = skanAddGeo(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG48Geo'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 3600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSCampaignG48'),index=False)
            print('user data group len:',len(userDf))

        else:
            userDf = pd.read_csv(getFilename('userAOSCampaignG48'))
            skanDf = pd.read_csv(getFilename('skanAOSCampaignG48Geo'), converters={'campaign_id':str})
        
        # userDf = meanAttribution(userDf, skanDf)
        # skanDf = skanDf.head(100)
        userDf = meanAttributionFastv2(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution48'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution48'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult48'), index=False)

    userDf = pd.read_csv(getFilename('meanAttributionResult48'), converters={'campaign_id':str})
    df = checkRet(userDf,prefix='attCampaign48_')
    checkRetByMedia(userDf,prefix='attCampaign48_')
    draw24(df,prefix='attCampaign48_')

def debug():
    dayList = [
        {'installTimeStart':'2024-01-01','installTimeEnd':'2024-01-31'},
        {'installTimeStart':'2024-02-01','installTimeEnd':'2024-02-29'},
        {'installTimeStart':'2024-03-01','installTimeEnd':'2024-03-31'},
        {'installTimeStart':'2024-04-01','installTimeEnd':'2024-04-30'},
    ]

    df = pd.DataFrame()

    for pAndR in pAndRList:

        p = pAndR['p']
        r = pAndR['r']

        for day in dayList:
            installTimeStart = day['installTimeStart']
            installTimeEnd = day['installTimeEnd']

            filename = getFilename(f'attMedia24_{installTimeStart}_{installTimeEnd}_{pAndR["p"]:.1f}_{pAndR["r"]:.1f}_groupbyMedia_Mape_{installTimeStart}_{installTimeEnd}')

            df0 = pd.read_csv(filename)

            df = pd.concat([df,df0])


        df = df.loc[df['r7usd'] > 1000]

        df.to_csv(getFilename(f'attMedia24_{pAndR["p"]:.1f}_{pAndR["r"]:.1f}_groupbyMedia_Mape'),index=False)

        groupByMediaDf = df.groupby('media').agg({
            'MAPE7':'mean',
            'MAPE30':'mean'
        }).reset_index()

        print('p:',p,'r:',r)
        print(groupByMediaDf)


if __name__ == '__main__':
    print('main24')
    
    dayList = [
        # {'installTimeStart':'2024-01-01','installTimeEnd':'2024-01-02'},
        {'installTimeStart':'2024-01-01','installTimeEnd':'2024-01-31'},
        {'installTimeStart':'2024-02-01','installTimeEnd':'2024-02-29'},
        {'installTimeStart':'2024-03-01','installTimeEnd':'2024-03-31'},
        {'installTimeStart':'2024-04-01','installTimeEnd':'2024-04-30'},
    ]


    for day in dayList:
        installTimeStart = day['installTimeStart']
        installTimeEnd = day['installTimeEnd']
        
        filename = getFilename(f'androidFp{installTimeStart}_{installTimeEnd}')

        main24(fast = False,onlyCheck = False)



    debug()