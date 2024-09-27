# 计算安装数相关系数
import io
import os
import requests
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 2024-07-10~2024-09-10 

# 获取大盘的安装数，付费用户安装数
def getTotalInstallCount(startDate = '20240710', endDate = '20240910'):
    filename = f'/src/data/zhangxiang_total_install_count_{startDate}_{endDate}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql = f'''
@data :=
SELECT
	game_uid as customer_user_id,
	COALESCE(
		SUM(
			CASE
				WHEN event_timestamp - install_timestamp between 0
				and 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	install_day,
	country as country_code
FROM
	rg_bi.ads_lastwar_ios_purchase_adv
WHERE
	install_day BETWEEN '{startDate}' AND '{endDate}'
	AND game_uid IS NOT NULL
GROUP BY
	game_uid,
	install_day,
	country
;

select
	sum(
		case
			when r1usd > 0 then 1
			else 0
		end
	) as paid_install_count,
	count(*) as total_install_count,
    sum(r1usd) as total_revenue,
	install_day
from
	@data
group by
	install_day
;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df


def getTotalInstallCountFromFile():
    filename = 'total.csv'
    df = pd.read_csv(filename)
    return df

# 获取skan的安装数，付费用户安装数
# freeTime 和 paidTime 分别是免费用户和付费用户的安装往前推的时间
def getSKANInstallCount(startDate = '20240710', endDate = '20240910',freeTime = 36*60*60, paidTime = 48*60*60):
    filename = f'/src/data/zhangxiang_skan_install_count_{startDate}_{endDate}_{freeTime}_{paidTime}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql = f'''
-- 免费用户查询
@getFreeInstall :=
select
	cast(
		GetTimestampFromTime(timestamp, '0', 'yyyy-MM-dd HH:mm:ss') - {freeTime} as bigint
	) as install_timestamp,
	*
from
	rg_bi.ods_platform_appsflyer_skad_postbacks_copy
where
	app_id = '6448786147'
	and (
		skad_conversion_value is null
		or skad_conversion_value in ('0', '32', 'null')
	)
	and day > 20240701
;

@retFree :=
select
	get_day_from_timestamp(install_timestamp, '0') as install_day,
	count(*) as free_install_count
from
	@getFreeInstall
group by
	install_day
;

-- 付费用户查询
@getPaidInstall :=
select
	cast(
		GetTimestampFromTime(timestamp, '0', 'yyyy-MM-dd HH:mm:ss') - {paidTime} as bigint
	) as install_timestamp,
	*
from
	rg_bi.ods_platform_appsflyer_skad_postbacks_copy
where
	app_id = '6448786147'
	and skad_conversion_value not in ('0', '32', 'null')
	and day > 20240701
;

@retPaid :=
select
	get_day_from_timestamp(install_timestamp, '0') as install_day,
	count(*) as paid_install_count
from
	@getPaidInstall
group by
	install_day
;

-- 合并免费用户和付费用户的结果
select
	coalesce(free.install_day, paid.install_day) as install_day,
	coalesce(paid.paid_install_count, 0) as paid_install_count,
	coalesce(free.free_install_count, 0) as free_install_count
from
	@retFree free full
	outer join @retPaid paid on free.install_day = paid.install_day
where
	coalesce(free.install_day, paid.install_day) between '{startDate}' and '{endDate}'
order by
	install_day
;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

# 按照Cv分组
def getSKANInstallCountCv(startDate = '20240710', endDate = '20240910',freeTime = 36*60*60, paidTime = 48*60*60):
    filename = f'/src/data/zhangxiang_skan_install_count_cv_{startDate}_{endDate}_{freeTime}_{paidTime}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql = f'''
-- 付费用户查询
@getPaidInstall :=
select
	cast(
		GetTimestampFromTime(timestamp, '0', 'yyyy-MM-dd HH:mm:ss') - {paidTime} as bigint
	) as install_timestamp,
	skad_conversion_value
from
	rg_bi.ods_platform_appsflyer_skad_postbacks_copy
where
	app_id = '6448786147'
	and skad_conversion_value not in ('0', '32', 'null')
	and day > 20240701
;

@retPaid :=
select
	get_day_from_timestamp(install_timestamp, '0') as install_day,
	skad_conversion_value,
	count(*) as paid_install_count
from
	@getPaidInstall
group by
	install_day, skad_conversion_value
;

select
	install_day,
    skad_conversion_value,
    paid_install_count
from
	@retPaid
where
	install_day between '{startDate}' and '{endDate}'
;
        '''
        # print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

# 计算相关系数
def main(startDate = '20240710', endDate = '20240915'):
    # totalDf = getTotalInstallCount(startDate, endDate)
    totalDf = getTotalInstallCountFromFile()
    totalDf['free_install_count'] = totalDf['total_install_count'] - totalDf['paid_install_count']

    # freeTimeMin = 24*60*60
    # freeTimeMax = 48*60*60
    # freeTimeList = [i for i in range(freeTimeMin, freeTimeMax, 30*60)]

    # freeCorrMax = 0
    # freeCorrMaxTime = 0

    # for freeTime in freeTimeList:
    #     skanDf = getSKANInstallCount(startDate, endDate, freeTime)
    #     df = pd.merge(totalDf, skanDf, on='install_day', how='inner',suffixes=('_total', '_skan'))
    #     # 计算free_install_count的相关系数
    #     freeCorr = df['free_install_count_total'].corr(df['free_install_count_skan'])
    #     print(f'免费用户，向前推{freeTime/3600}小时 相关系数: {freeCorr}')
    #     if freeCorr > freeCorrMax:
    #         freeCorrMax = freeCorr
    #         freeCorrMaxTime = freeTime/3600

    # print('------------------------------------')
    # print(f'>>免费用户相关系数最大值: {freeCorrMax}, 最大值对应时间: {freeCorrMaxTime}')
    # print('------------------------------------')

    # paidTimeMin = 24*60*60
    # paidTimeMax = 72*60*60
    paidTimeMin = 35*60*60
    paidTimeMax = 45*60*60
    paidTimeList = [i for i in range(paidTimeMin, paidTimeMax, 30*60)]

    paidCorrMax = 0
    paidCorrMaxTime = 0
    paidDf = None

    for paidTime in paidTimeList:
        skanDf = getSKANInstallCount(startDate, endDate, 36*60*60, paidTime)
        
        totalDf['install_day'] = totalDf['install_day'].astype(int)
        skanDf['install_day'] = skanDf['install_day'].astype(int)

        df = pd.merge(totalDf, skanDf, on='install_day', how='inner',suffixes=('_total', '_skan'))
        # install_day 是类似 20240710 这样的字符串，转成日期类型
        df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

        df = df.sort_values(by='install_day', ascending=True)
        # # 计算paid_install_count的相关系数
        # paidCorr = df['paid_install_count_total'].corr(df['paid_install_count_skan'])
        # 计算差分
        diff_total = df['paid_install_count_total'].diff().dropna()
        diff_skan = df['paid_install_count_skan'].diff().dropna()
        # 计算差分后的相关系数
        paidCorr = diff_total.corr(diff_skan)

        print(f'付费用户，向前推{paidTime/3600}小时 相关系数: {paidCorr}')

        if paidCorr > paidCorrMax:
            paidCorrMax = paidCorr
            paidCorrMaxTime = paidTime/3600
            paidDf = df.copy()

    print('------------------------------------')
    print(f'>>付费用户相关系数最大值: {paidCorrMax}, 最大值对应时间: {paidCorrMaxTime}')
    print('------------------------------------')

    # 画图，x轴是日期，y轴是 paid_install_count_total 和 paid_install_count_skan
    paidDf.to_csv('/src/data/zhangxiang.csv', index=False)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(20, 10))
    plt.plot(paidDf['install_day'], paidDf['paid_install_count_total'], label='total')
    plt.plot(paidDf['install_day'], paidDf['paid_install_count_skan'], label='skan')
    plt.legend()
    plt.savefig('/src/data/zhangxiang.png')

def main2(startDate = '20240710', endDate = '20240915'):
    totalDf = getTotalInstallCount(startDate, endDate)

    paidTimeMin = 35*60*60
    paidTimeMax = 45*60*60
    paidTimeList = [i for i in range(paidTimeMin, paidTimeMax, 30*60)]

    paidCorrMax = 0
    paidCorrMaxTime = 0
    paidDf = None

    for paidTime in paidTimeList:
        skanDf = getSKANInstallCount(startDate, endDate, 36*60*60, paidTime)
        
        totalDf['install_day'] = totalDf['install_day'].astype(int)
        skanDf['install_day'] = skanDf['install_day'].astype(int)

        df = pd.merge(totalDf, skanDf, on='install_day', how='inner',suffixes=('_total', '_skan'))

        df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
        df = df.sort_values(by='install_day', ascending=True)

        # 计算差分
        diff_total = df['paid_install_count_total'].diff().dropna()
        diff_skan = df['paid_install_count_skan'].diff().dropna()
        # 计算差分后的相关系数
        paidCorr = diff_total.corr(diff_skan)
        print(f'付费用户，向前推{paidTime/3600}小时 相关系数: {paidCorr}')

        if paidCorr > paidCorrMax:
            paidCorrMax = paidCorr
            paidCorrMaxTime = paidTime/3600
            paidDf = df.copy()


    print('------------------------------------')
    print(f'>>付费用户相关系数最大值: {paidCorrMax}, 最大值对应时间: {paidCorrMaxTime}')
    print('------------------------------------')

    # 画图，x轴是日期，y轴是 paid_install_count_total 和 paid_install_count_skan
    paidDf.to_csv('/src/data/zhangxiang.csv', index=False)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(20, 10))
    plt.plot(paidDf['install_day'], paidDf['paid_install_count_total'], label='total')
    plt.plot(paidDf['install_day'], paidDf['paid_install_count_skan'], label='skan')
    plt.legend()
    plt.savefig('/src/data/zhangxiang2.png')

csvStr20240706 = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2024-07-05 09:48:05,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,0.97,0,24,2024-07-05 09:48:05,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,0.97,0.99,0,24,2024-07-05 09:48:05,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,0.99,1.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,1.92,2.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,2.91,3.28,0,24,2024-07-05 09:48:05,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,3.28,5.85,0,24,2024-07-05 09:48:05,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,5.85,7.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,7.67,9.24,0,24,2024-07-05 09:48:05,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,9.24,12.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,12.4,14.95,0,24,2024-07-05 09:48:05,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,14.95,17.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,17.96,22.37,0,24,2024-07-05 09:48:05,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,22.37,26.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,26.96,31.81,0,24,2024-07-05 09:48:05,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,31.81,36.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,36.25,42.53,0,24,2024-07-05 09:48:05,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,42.53,49.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,49.91,57.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,57.92,67.93,0,24,2024-07-05 09:48:05,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,67.93,81.27,0,24,2024-07-05 09:48:05,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,81.27,98.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,98.25,117.86,0,24,2024-07-05 09:48:05,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,117.86,142.29,0,24,2024-07-05 09:48:05,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,142.29,180.76,0,24,2024-07-05 09:48:05,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,180.76,225.43,0,24,2024-07-05 09:48:05,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,225.43,276.72,0,24,2024-07-05 09:48:05,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,276.72,347.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,347.4,472.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,472.67,620.8,0,24,2024-07-05 09:48:05,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,620.8,972.22,0,24,2024-07-05 09:48:05,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,972.22,2038.09,0,24,2024-07-05 09:48:05,0,,,
'''

# 按照24小时金额计算
def main2Revenue(startDate = '20240710', endDate = '20240915'):
    totalDf = getTotalInstallCount(startDate, endDate)

    paidTimeMin = 35*60*60
    paidTimeMax = 45*60*60
    paidTimeList = [i for i in range(paidTimeMin, paidTimeMax, 30*60)]

    paidCorrMax = 0
    paidCorrMaxTime = 0
    paidDf = None

    csv_file_like_object = io.StringIO(csvStr20240706)
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvMapDf['usd'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue'])/2
    cvMapDf.rename(columns={'conversion_value':'cv'}, inplace=True)
    cvMapDf = cvMapDf[['cv','usd']]

    for paidTime in paidTimeList:
        skanDf = getSKANInstallCountCv(startDate, endDate, 36*60*60, paidTime)
        skanDf.rename(columns={'skad_conversion_value':'cv'}, inplace=True)
        skanDf['cv'] = skanDf['cv'].astype(int)
        skanDf.loc[skanDf['cv'] > 31 ,'cv'] -= 32
        skanDf = pd.merge(skanDf, cvMapDf, on='cv', how='inner')
        skanDf['skan_revenue'] = skanDf['paid_install_count'] * skanDf['usd']
        skanDf = skanDf.groupby('install_day').agg({'skan_revenue':'sum'}).reset_index()

        totalDf['install_day'] = totalDf['install_day'].astype(int)
        skanDf['install_day'] = skanDf['install_day'].astype(int)

        df = pd.merge(totalDf, skanDf, on='install_day', how='inner',suffixes=('_total', '_skan'))

        df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
        df = df.sort_values(by='install_day', ascending=True)

        # print(df)

        # 计算差分
        diff_total = df['total_revenue'].diff().dropna()
        diff_skan = df['skan_revenue'].diff().dropna()
        # 计算差分后的相关系数
        paidCorr = diff_total.corr(diff_skan)
        print(f'付费用户，向前推{paidTime/3600}小时 相关系数: {paidCorr}')

        if paidCorr > paidCorrMax:
            paidCorrMax = paidCorr
            paidCorrMaxTime = paidTime/3600
            paidDf = df.copy()


    print('------------------------------------')
    print(f'>>付费用户相关系数最大值: {paidCorrMax}, 最大值对应时间: {paidCorrMaxTime}')
    print('------------------------------------')

    # 画图，x轴是日期，y轴是 paid_install_count_total 和 paid_install_count_skan
    paidDf.to_csv('/src/data/zhangxiang2.csv', index=False)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(20, 10))
    plt.plot(paidDf['install_day'], paidDf['total_revenue'], label='total')
    plt.plot(paidDf['install_day'], paidDf['skan_revenue'], label='skan')
    plt.legend()
    plt.savefig('/src/data/zhangxiang3.png')



if __name__ == '__main__':
    main2Revenue()