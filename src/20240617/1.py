# 计算lastwar 安卓，cv分档后，付费金额与cv金额误差
# 其中共64个档位，N个档位用于24小时内付费金额，（64/N)个档位用于7日付费金额。
# 分别计算24小时内付费金额误差与7日付费金额误差，MAPE24与MAPE168。

import os
import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMC(installTimeStart = '2024-04-01',installTimeEnd = '2024-04-30'):
    filename = f'/src/data/zk2/androidFp_iOS_{installTimeStart}_{installTimeEnd}.csv'

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



def addCv(userDf,cvMapDf,usd='r1usd',cv='cv',avgUsd='avg'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        avg = (min+max)/2
        if avg < 0:
            avg = 0
        # if avg < 0:
        #     avg = max/2
        # print(f'cv1:{cv1},min:{min},max:{max},avg:{avg}')
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),avgUsd
        ] = avg
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    userDfCopy.loc[userDfCopy[usd]>max,avgUsd] = avg
    return userDfCopy

def makeLevels(userDf, usd='r1usd', N=32, minUsd = 0,isM = False):
    
    if isM and minUsd >= 0:
        N = N + 1
        # print('N:',N)

    if minUsd < 0:
        minUsd = 0


    userDf = userDf.copy()
    # 如果userDf没有sumUsd列，就添加sumUsd列，值为usd列的值
    if 'sumUsd' not in userDf.columns:
        userDf['sumUsd'] = userDf[usd]
    
    filtered_df = userDf[(userDf[usd] >= minUsd)]

    # 进行汇总，如果输入的数据已经汇总过，那就再做一次，效率影响不大
    filtered_df = filtered_df.groupby([usd]).agg({'sumUsd':'sum'}).reset_index()

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df['sumUsd'].sum()
    total_min_usd = minUsd * len(df)
    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = (total_usd - total_min_usd) / (N)
    df['sum'] = df['sumUsd'].cumsum()
    
    # # debug
    # print('total_usd:',total_usd)
    # print('total_min_usd:',total_min_usd)
    # print('target_usd:',target_usd)

    for i in range(1,N):
        target = total_min_usd + target_usd*(i)
        # 找到第一个大于target的行
        rows = df[df['sum']>=target]
        if len(rows) > 0:
            row = rows.iloc[0]
            levels[i-1] = row[usd]

    # levels 排重
    levels = list(set(levels))
    # levels 中如果有0，去掉0
    if 0 in levels:
        levels.remove(0)
    # levels 排序
    levels.sort()

    return levels

def makeCvMap(levels,minUsd = 0,isM = False):
    if isM == False or minUsd < 0:
        mapData = {
            'cv':[0],
            'min_event_revenue':[-np.inf],
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
    else:
        avg = (minUsd + levels[0])/2
        # if minUsd < 0:
        #     avg = (levels[0])/2
        if avg < 0:
            avg = 0

        mapData = {
            'cv':[0],
            'min_event_revenue':[minUsd],
            'max_event_revenue':[levels[0]],
            'avg':[avg]
        }
        for i in range(1,len(levels)):
            mapData['cv'].append(len(mapData['cv']))
            min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
            max = levels[i]
            mapData['min_event_revenue'].append(min)
            mapData['max_event_revenue'].append(max)
            mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def main(installTimeStart = '2024-04-01',installTimeEnd = '2024-05-31'):
    filename = f'/src/data/zk2/androidFp_iOS_{installTimeStart}_{installTimeEnd}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        df = pd.read_csv(filename)
    else:
        df = getDataFromMC(installTimeStart,installTimeEnd)
    # 列 uid,install_date,r1usd,r2usd,r7usd,r30usd,install_timestamp,last_timestamp,media_source,campaign_id,country_code

    userDf = df[['uid','install_date','r1usd','r7usd']]
    # 做一些过滤
    userDf.loc[userDf['r1usd']> 2000,'r1usd'] = 2000

    NList = [8,16,32]
    count='count'
    userDf[count] = 1

    for N in NList:
        M = 64//N
        print(f'N={N},M={M}')
        levels = makeLevels(df,usd='r1usd',N=N)
        levels = [round(x,2) for x in levels]
        # print('levels:',levels)
        cvMapDf = makeCvMap(levels)
        cvMapDf.to_csv(f'/src/data/lastwarCV2_{N}x{M}.csv',index=False)
        
        addCvDf = addCv(userDf,cvMapDf,'r1usd','cv1','avg1')
        addCvDf['sumUsd1'] = addCvDf['r1usd'] * addCvDf[count]
        addCvDf['sumAvg1'] = addCvDf['avg1'] * addCvDf[count]

        cv1List = addCvDf['cv1'].unique()
        cv1List.sort()

        retDf = pd.DataFrame()
        for cv1 in cv1List:
            cvDf = addCvDf[addCvDf['cv1'] == cv1]

            levels2 = makeLevels(cvDf,usd='r7usd',N=M,minUsd=cvMapDf['min_event_revenue'][cv1],isM=True)
            levels2 = [round(x,2) for x in levels2]
            # print('levels2:',levels2)
            # print(cvMapDf['min_event_revenue'][cv1])
            cvMapDf2 = makeCvMap(levels2,minUsd=cvMapDf['min_event_revenue'][cv1],isM=True)
            cvMapDf2.to_csv(f'/src/data/lastwarCV2_{N}x{M}_{cv1}.csv',index=False)

            addCvDf2 = addCv(cvDf,cvMapDf2,'r7usd','cv2','avg2')
            addCvDf2['sumUsd2'] = addCvDf2['r7usd'] * addCvDf2[count]
            addCvDf2['sumAvg2'] = addCvDf2['avg2'] * addCvDf2[count]

            retDf = pd.concat([retDf,addCvDf2])
        
        retDf = retDf.groupby(['install_date']).agg({
            'sumUsd1':'sum',
            'sumAvg1':'sum',
            'sumUsd2':'sum',
            'sumAvg2':'sum'
        }).reset_index()
        retDf['mape1'] = abs(retDf['sumUsd1'] - retDf['sumAvg1']) / retDf['sumUsd1']
        retDf['mape2'] = abs(retDf['sumUsd2'] - retDf['sumAvg2']) / retDf['sumUsd2']

        print('mape24:',retDf['mape1'].mean())
        print('mape168:',retDf['mape2'].mean())

        

if __name__ == '__main__':
    main()
