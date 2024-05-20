# campaign id 120208658608900690

import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getCppData():
    df = pd.read_csv('cpp.csv')
    return df

def getAfSkanData():
    filename = f'/src/data/afSkanData20240513.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    skad_conversion_value as cv,
    sum(
        case
            when skad_redownload = 'false' then 1
            else 0
        end
    ) as first_download_count,
    count(*) as count,
    install_date as install_day
from
    ods_platform_appsflyer_skad_details
where
    day between '20240430'
    and '20240515'
    AND app_id = 'id6448786147'
    AND event_name in (
        'af_skad_install',
        'af_skad_redownload'
    )
    and ad_network_campaign_id = '120208658608900690'
group by
    skad_conversion_value,
    install_date
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

# 用融合归因数据与cpp数据做对比
def getMergedData():
    filename = f'/src/data/mergedData20240513.csv'
    if not os.path.exists(filename):
        sql = f'''
SET
	odps.sql.timezone = Africa / Accra;

set
	odps.sql.hive.compatible = true;

set
	odps.sql.executionengine.enable.rand.time.seed = true;

@rhData :=
select
	customer_user_id,
	campaign_id,
	rate
from
	lastwar_ios_funplus02_adv_uid_mutidays_campaign2
where
	day between '20240430'
	and '20240515'
    and campaign_id = '120208658608900690'
;

@biData :=
SELECT
	game_uid as customer_user_id,
	COALESCE(
		SUM(
			CASE
				WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	install_day as install_date
FROM
	rg_bi.ads_lastwar_ios_purchase_adv
WHERE
	game_uid IS NOT NULL
GROUP BY
	game_uid,
	install_day;

@biData2 :=
    select
        customer_user_id,
        r1usd,
        CASE
            WHEN r1usd = 0 THEN 'free'
            ELSE 'pay'
        END as paylevel,
        install_date
    from
        @biData;

select
	rh.campaign_id,
	sum(bi.r1usd * rh.rate) as r1usd,
	bi.install_date,
    bi.paylevel,
	sum(rh.rate) as installs
from
	@rhData as rh
	left join @biData2 as bi on rh.customer_user_id = bi.customer_user_id
group by
	rh.campaign_id,
	bi.install_date,
    bi.paylevel
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_date':str})
    return df

def getMergedRevenueData():
    filename = f'/src/data/mergedRevenueData20240513.csv'
    if not os.path.exists(filename):
        sql = f'''
SET
	odps.sql.timezone = Africa / Accra;

set
	odps.sql.hive.compatible = true;

set
	odps.sql.executionengine.enable.rand.time.seed = true;

@rhData :=
select
	customer_user_id,
	campaign_id,
	rate
from
	lastwar_ios_funplus02_adv_uid_mutidays_campaign2
where
	day between '20240430'
	and '20240515'
	and campaign_id = '120208658608900690';

@biData :=
SELECT
	game_uid as customer_user_id,
	sum(revenue_value_usd) as revenue_value_usd,
	purchase_day as purchase_day
FROM
	rg_bi.ads_lastwar_ios_purchase_adv
WHERE
	game_uid IS NOT NULL
GROUP BY
	game_uid,
	purchase_day;

select
	rh.campaign_id,
	sum(bi.revenue_value_usd * rh.rate) as revenue_value_usd,
	bi.purchase_day
from
	@rhData as rh
	left join @biData as bi on rh.customer_user_id = bi.customer_user_id
group by
	rh.campaign_id,
	bi.purchase_day
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'purchase_day':str})
    return df

# 用skan af数据与cpp数据作对比
def afSkanVsCpp():
    afSkanData = getAfSkanData()
    cppData = getCppData()

    # afSkanData 整理
    # 列install_day是类似2024-05-10的字符串，转为类似2024/5/10的字符串
    afSkanData['install_day'] = afSkanData['install_day'].apply(lambda x: x.replace('-','/'))

    afSkanInstallDf = afSkanData.groupby('install_day').agg(
        {
            'first_download_count':'sum',
            'count':'sum'
        }
    ).reset_index()
    afSkanInstallDf.rename(columns={
        'install_day':'日期',
        'first_download_count':'首次下载数(AF)',
        'count':'下载数(AF)'
    },inplace=True)
    df = pd.merge(cppData, afSkanInstallDf,on = '日期', how='left')
    afSkanData.fillna(0,inplace=True)
    afSkanPayUserDf = afSkanData[(afSkanData['cv']!=0) & (afSkanData['cv']!=32)]
    
    afSkanPayUserDf = afSkanPayUserDf.groupby('install_day').agg(
        {
            'count':'sum'
        }
    ).reset_index() 

    
    afSkanPayUserDf = afSkanPayUserDf[['install_day','count']]
    afSkanPayUserDf.rename(columns={'install_day':'日期','count':'付费用户数(AF)'},inplace=True)
    
    df = pd.merge(df, afSkanPayUserDf,on = '日期', how='left')

    # print(df)
    df.to_csv('/src/data/20240513afSkanVsCpp.csv',index=False)

def mergeVsCpp():
    mergedData = getMergedData()
    mergedRevenueData = getMergedRevenueData()
    cppData = getCppData()


    # mergedData 整理
    # install_date 是类似20240510的字符串，转为类似2024/05/10的字符串
    mergedData['install_date'] = mergedData['install_date'].apply(lambda x: f'{x[:4]}/{x[4:6]}/{x[6:]}')

    mergedInstallDf = mergedData.groupby('install_date').agg(
        {
            'installs':'sum'
        }
    ).reset_index()
    mergedInstallDf['installs'] = mergedInstallDf['installs'].astype(int)
    mergedInstallDf.rename(columns={
        'install_date':'日期',
        'installs':'下载数(融合)'
    },inplace=True)
    df = pd.merge(cppData, mergedInstallDf,on = '日期', how='left')

    mergedPayUserDf = mergedData[mergedData['paylevel']=='pay'].groupby('install_date').agg(
        {
            'installs':'sum'
        }
    ).reset_index()
    mergedPayUserDf.rename(columns={
        'install_date':'日期',
        'installs':'付费用户数(融合)'
    },inplace=True)
    mergedPayUserDf['付费用户数(融合)'] = mergedPayUserDf['付费用户数(融合)'].astype(int)
    df = pd.merge(df, mergedPayUserDf,on = '日期', how='left')

    # mergedRevenueData 整理
    # purchase_day 是类似20240510的字符串，转为类似2024/05/10的字符串
    mergedRevenueData = mergedRevenueData[mergedRevenueData['purchase_day'].notnull()]
    print(mergedRevenueData)
    mergedRevenueData['purchase_day'] = mergedRevenueData['purchase_day'].apply(lambda x: f'{x[:4]}/{x[4:6]}/{x[6:]}')
    mergedRevenueData = mergedRevenueData[['purchase_day','revenue_value_usd']]
    mergedRevenueData.rename(columns={'purchase_day':'日期','revenue_value_usd':'收入流水(融合)'},inplace=True)

    df = pd.merge(df, mergedRevenueData,on = '日期', how='left')

    # print(df)
    df.to_csv('/src/data/20240513mergeVsCpp.csv',index=False)


if __name__ == '__main__':
    afSkanVsCpp()
    mergeVsCpp()
    