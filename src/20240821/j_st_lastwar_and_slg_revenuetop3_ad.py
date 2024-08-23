# 获得slg收费前3名游戏的广告展示占比与lastwar的广告展示占比
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar

def get_month_start_end_dates(month):
    # 将月份字符串转换为datetime对象
    start_date = datetime.strptime(month + '01', '%Y%m%d')
    # 获取该月的最后一天
    _, last_day = calendar.monthrange(start_date.year, start_date.month)
    # 计算结束日期
    end_date = start_date.replace(day=last_day)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# 获得slg收费前3名游戏的app_id
# month: 获取畅销榜的月份
# day：appid 映射表的日期，一般的采用month的最后一天
def getSlgTop3AppIdList(month = '202406'):
    global isOnlineVersion
    filename = f'/src/data/slgTop3AppIdList_{month}.csv'
    if isOnlineVersion == False and os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        _,day = get_month_start_end_dates(month)
        day = day.replace('-','')
        sql = f'''
-- 提取lastwar的app_id
@lastwarAppIds :=
SELECT
	month,
	country,
	app_id AS unified_app_id
FROM
	j_st_slg_revenuetop3_monthly
WHERE
	app_id = '64075e77537c41636a8e1c58'
	and month = '{month}'
	and country in(
		'US', 'AU', 'CA', 'CN', 'FR', 'DE', 'GB', 'IT', 'JP', 'KR', 'RU', 'AR', 'AT', 'BE', 'BR', 'CL', 'CO', 'DK', 'EC', 'HK', 'IN', 'ID', 'IL', 'LU', 'MY', 'MX', 'NL', 'NZ', 'NO', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'SA', 'SG', 'ZA', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'UA', 'AE', 'VN'
	);

-- 提取SLG类型游戏的app_id（不包含lastwar）
@slgRevenueData :=
SELECT
	month,
	country,
	app_id,
	revenue,
	RANK() OVER (
		PARTITION BY month,
		country
		ORDER BY
			revenue DESC
	) AS rank
FROM
	j_st_slg_revenuetop3_monthly
WHERE
	app_id <> '64075e77537c41636a8e1c58'
	and month = '{month}'
	and country in(
		'US', 'AU', 'CA', 'CN', 'FR', 'DE', 'GB', 'IT', 'JP', 'KR', 'RU', 'AR', 'AT', 'BE', 'BR', 'CL', 'CO', 'DK', 'EC', 'HK', 'IN', 'ID', 'IL', 'LU', 'MY', 'MX', 'NL', 'NZ', 'NO', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'SA', 'SG', 'ZA', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'UA', 'AE', 'VN'
	);

-- 计算每个国家每个月前3名的app_id
@top3AppIds :=
SELECT
	month,
	country,
	app_id AS unified_app_id
FROM
	@slgRevenueData
WHERE
	rank <= 3;

-- 提取lastwar的app_id和unified_app_id
@lastwarAppIdsWithUnified :=
SELECT
	lwa.month,
	lwa.country,
	dsta.app_id AS app_id,
	lwa.unified_app_id
FROM
	@lastwarAppIds lwa
	JOIN dwd_sensortower_topapps dsta ON lwa.unified_app_id = dsta.unified_app_id
WHERE
	dsta.day = '{day}';

-- 提取前3名应用的app_id和unified_app_id
@top3AppIdsWithUnified :=
SELECT
	t3a.month,
	t3a.country,
	dsta.app_id AS app_id,
	t3a.unified_app_id
FROM
	@top3AppIds t3a
	JOIN dwd_sensortower_topapps dsta ON t3a.unified_app_id = dsta.unified_app_id
WHERE
	dsta.day = '{day}';
;

SELECT
	month,
	country,
	app_id,
	unified_app_id
FROM
	@lastwarAppIdsWithUnified
UNION
ALL
SELECT
	month,
	country,
	app_id,
	unified_app_id
FROM
	@top3AppIdsWithUnified;
        '''
        print(sql)
        df = execSql(sql)
        if isOnlineVersion == False:
            df.to_csv(filename,index=False)
    return df

sensortowerToken = ''

def getSensortowerToken():
    global sensortowerToken
    if sensortowerToken != '':
        return sensortowerToken
    
    sql = '''
        select k,v from j_st_config;
    '''
    df = execSql(sql)
    token = df[df['k'] == 'token']['v'].values[0]
    token = 'ST0_' + token
    sensortowerToken = token

    return token

def getAdData(app_ids=[],networks=[],countries=[],start_date='2024-06-01',end_date='2024-07-31'):
    # https://api.sensortower.com/v1/unified/ad_intel/network_analysis?app_ids=5570fc1cfe55ad5778000621&start_date=2024-01-01&end_date=2024-06-30&period=month&networks=Facebook&countries=US%2CJP%2CKR&auth_token=YOUR_AUTHENTICATION_TOKEN
    
    appIdsStr = '%2C'.join(app_ids)
    networksStr = '%2C'.join(networks)
    countriesStr = '%2C'.join(countries)
    url = f'https://api.sensortower.com/v1/unified/ad_intel/network_analysis?app_ids={appIdsStr}&start_date={start_date}&end_date={end_date}&period=month&networks={networksStr}&countries={countriesStr}&auth_token={getSensortowerToken()}'
    print(url)
    response = requests.get(url)
    data = response.json()
    # print(data)
    df = pd.DataFrame(data)

    return df

def main(month = '202406'):
    global isOnlineVersion
    slgTop3AppIdListDf = getSlgTop3AppIdList(month)
    slgTop3AppIdListDf['app_id'] = slgTop3AppIdListDf['unified_app_id']
    slgTop3AppIdListDf = slgTop3AppIdListDf.groupby(['country','app_id']).agg({'unified_app_id':'first'}).reset_index()
    slgTop3AppIdListDf = slgTop3AppIdListDf[['country','app_id']]

    countriesAllowLise = ['US', 'AU', 'CA', 'CN', 'FR', 'DE', 'GB', 'IT', 'JP', 'KR', 'RU', 'AR', 'AT', 'BE', 'BR', 'CL', 'CO', 'DK', 'EC', 'HK', 'IN', 'ID', 'IL', 'LU', 'MY', 'MX', 'NL', 'NZ', 'NO', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'SA', 'SG', 'ZA', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'UA', 'AE', 'VN']
    slgTop3AppIdListDf = slgTop3AppIdListDf[slgTop3AppIdListDf['country'].isin(countriesAllowLise)]
    countries = slgTop3AppIdListDf['country'].unique().tolist()

    slgTop3Df = slgTop3AppIdListDf[slgTop3AppIdListDf['app_id'] != '64075e77537c41636a8e1c58']
    lastwarDf = slgTop3AppIdListDf[slgTop3AppIdListDf['app_id'] == '64075e77537c41636a8e1c58']
    slgAppIdList = slgTop3AppIdListDf['app_id'].unique().tolist()

    networks = ["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]
    
    startDate,endDate = get_month_start_end_dates(month)

    l = len(slgAppIdList)//5
    if len(slgAppIdList) % 5 > 0:
        l += 1
    
    # print('slgAppIdList:',slgAppIdList)
    # print('l:',l)

    df = pd.DataFrame()
    for i in range(l):
        filename = f'/src/data/ad20240822Top3_{i}_{month}.csv'
        if isOnlineVersion == False and os.path.exists(filename):
            print('已存在%s'%filename)
            adDf = pd.read_csv(filename)
        else:
            minIndex = i * 5
            maxIndex = minIndex + 5

            slgAppIdList5 = slgAppIdList[minIndex:maxIndex]
            
            adDf = getAdData(app_ids=slgAppIdList5,networks=networks,countries=countries,start_date=startDate,end_date=endDate)
            if isOnlineVersion == False:
                adDf.to_csv(filename,index=False)

        adDf = adDf.merge(slgTop3AppIdListDf,on=['app_id','country'],how='inner')
        df = pd.concat([df,adDf],ignore_index=True)

    df = df.sort_values(['country','app_id','date'])
    df = df[['app_id','country','network','sov']]
    writeTable1(df,month)

    # 设定各媒体权重
    # 参考lastwar 202406月 广告展示量
    mediaWeight = {
        'Admob': 7,
        'Applovin': 2,
        'Facebook': 5,
        'Instagram': 5,
        'Meta Audience Network': 0.2,
        'TikTok': 6,
        'Youtube': 7,
    }
    mediaWeightDf = pd.DataFrame(list(mediaWeight.items()), columns=['network', 'weight'])

    # print(slgTop3Df)
    slgTop3Df = slgTop3Df[['country','app_id']].merge(df,on=['app_id','country'],how='inner')
    slgTop3Df = slgTop3Df.sort_values(['country','app_id'])
    # print(slgTop3Df)
    slgTop3Df = slgTop3Df.merge(mediaWeightDf,on='network',how='left')
    slgTop3Df['weight'] = slgTop3Df['weight'].fillna(0)
    slgTop3Df['sov*weight'] = slgTop3Df['sov'] * slgTop3Df['weight']
    slgTop3Df = slgTop3Df.groupby(['country']).agg(
        {
            'sov*weight':'sum'
        }
    ).reset_index()
    # print(slgTop3Df)

    lastwarDf = lastwarDf[['country','app_id']].merge(df,on=['app_id','country'],how='inner')
    lastwarDf = lastwarDf.sort_values(['country','app_id'])
    lastwarDf = lastwarDf.merge(mediaWeightDf,on='network',how='left')
    lastwarDf['weight'] = lastwarDf['weight'].fillna(0)
    lastwarDf['sov*weight'] = lastwarDf['sov'] * lastwarDf['weight']
    lastwarDf = lastwarDf.groupby(['country']).agg(
        {
            'sov*weight':'sum'
        }
    ).reset_index()
    # print(lastwarDf)

    df = slgTop3Df.merge(lastwarDf,on='country',how='inner',suffixes=('_slg','_lastwar'))
    df = df.rename(columns={'sov*weight_slg':'slg_top3_sov_sum','sov*weight_lastwar':'lastwar_sov_sum'})
    writeTable2(df,month)

def init():
    global execSql
    global month
    global isOnlineVersion

    if 'o' in globals():
        print('this is online version')
        isOnlineVersion = True

        from odps import options
        # UTC+0
        options.sql.settings = {
            'odps.sql.timezone':'Africa/Accra',
            "odps.sql.submit.mode" : "script"
        }

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        month = args['month']
    else:
        print('this is local version')
        isOnlineVersion = False

        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        month = '202406'

from odps.models import Schema, Column, Partition
def createTable1():
    global isOnlineVersion
    if isOnlineVersion:
        columns = [
            Column(name='app_id', type='string', comment='unified_app_id'),
            Column(name='country', type='string', comment='["US", "AU", "CA", "CN", "FR", "DE", "GB", "IT", "JP", "KR", "RU", "AR", "AT", "BE", "BR", "CL", "CO", "DK", "EC", "HK", "IN", "ID", "IL", "LU", "MY", "MX", "NL", "NZ", "NO", "PA", "PE", "PH", "PL", "PT", "RO", "SA", "SG", "ZA", "ES", "SE", "CH", "TW", "TH", "TR", "UA", "AE", "VN"]'),
            Column(name='network', type='string', comment='["Admob","Applovin","Facebook","Instagram","Meta Audience Network","TikTok","Youtube"]'),
            Column(name='sov', type='double', comment='广告曝光占比')
        ]
        
        partitions = [
            Partition(name='month', type='string', comment='like 202406')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('j_st_slg_revenuetop3_ad_sov_monthly', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable1(df,month):
    print('try to write table:j_st_slg_revenuetop3_ad_sov_monthly')
    print(df.head(5))
    global isOnlineVersion
    if isOnlineVersion:
        t = o.get_table('j_st_slg_revenuetop3_ad_sov_monthly')
        t.delete_partition('month=%s'%(month), if_exists=True)
        with t.open_writer(partition='month=%s'%(month), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv('/src/data/j_st_slg_revenuetop3_ad_sov_monthly_%s.csv'%(month),index=False)
    
def createTable2():
    global isOnlineVersion
    if isOnlineVersion:
        columns = [
            Column(name='country', type='string', comment='["US", "AU", "CA", "CN", "FR", "DE", "GB", "IT", "JP", "KR", "RU", "AR", "AT", "BE", "BR", "CL", "CO", "DK", "EC", "HK", "IN", "ID", "IL", "LU", "MY", "MX", "NL", "NZ", "NO", "PA", "PE", "PH", "PL", "PT", "RO", "SA", "SG", "ZA", "ES", "SE", "CH", "TW", "TH", "TR", "UA", "AE", "VN"]'),
            Column(name='slg_top3_sov_sum', type='double', comment='广告曝光占比'),
            Column(name='lastwar_sov_sum', type='double', comment='广告曝光占比')
        ]
        
        partitions = [
            Partition(name='month', type='string', comment='like 202406')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('j_st_lastwar_and_slg_revenuetop3_ad_sov_monthly', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable2(df,month):
    print('try to write table:j_st_lastwar_and_slg_revenuetop3_ad_sov_monthly')
    print(df.head(5))
    global isOnlineVersion
    if isOnlineVersion:
        t = o.get_table('j_st_lastwar_and_slg_revenuetop3_ad_sov_monthly')
        t.delete_partition('month=%s'%(month), if_exists=True)
        with t.open_writer(partition='month=%s'%(month), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv('/src/data/j_st_lastwar_and_slg_revenuetop3_ad_sov_monthly_%s.csv'%(month),index=False)
    


if __name__ == '__main__':
    init()
    createTable1()
    createTable2()

    global month
    main(month)
