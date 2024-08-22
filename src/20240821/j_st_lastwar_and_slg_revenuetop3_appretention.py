# 获得slg收费前3名游戏的留存率
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar

# 获得slg收费前3名游戏的app_id
# month: 获取畅销榜的月份
# day：appid 映射表的日期，一般的采用month的最后一天
def getSlgTop3AppIdList(month = '202406'):
    filename = f'/src/data/slgTop3AppIdList_{month}.csv'
    if os.path.exists(filename):
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
		'AU',
		'BR',
		'CA',
		'DE',
		'ES',
		'FR',
		'GB',
		'IN',
		'IT',
		'JP',
		'KR',
		'US'
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
		'AU',
		'BR',
		'CA',
		'DE',
		'ES',
		'FR',
		'GB',
		'IN',
		'IT',
		'JP',
		'KR',
		'US'
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
        df.to_csv(filename,index=False)
    return df

def get_month_start_end_dates(month):
    # 将月份字符串转换为datetime对象
    start_date = datetime.strptime(month + '01', '%Y%m%d')
    # 获取该月的最后一天
    _, last_day = calendar.monthrange(start_date.year, start_date.month)
    # 计算结束日期
    end_date = start_date.replace(day=last_day)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def is_number(value):
        # 如果值本身是整数或浮点数，直接返回 True
        if isinstance(value, (int, float)):
            return True
        
        # 如果值是字符串，尝试转换为浮点数
        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False
        
        # 其他类型返回 False
        return False

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

# 获得留存数据
def getRetention(app_ids=[],platform='ios',date_granularity='all_time',start_date='2021-01-01',end_date='2021-04-01',country=''):
    filename = f'/src/data/stRetention20240821_{platform}_{start_date}_{end_date}_{country}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # https://api.sensortower.com/v1/ios/usage/retention?app_ids=5cc98b703ea98357b8ed3ce0&date_granularity=quarterly&start_date=2021-01-01&end_date=2021-04-01&country=US&auth_token=YOUR_AUTHENTICATION_TOKEN
        url = 'https://api.sensortower.com/v1/{}/usage/retention?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(platform,','.join(app_ids),date_granularity,start_date,end_date,getSensortowerToken())
        print(url)

        if country != '':
            url += '&country='+country
        r = requests.get(url)
        if r.status_code != 200:
            print('Error: getRetention failed, status_code:',r.status_code)
            print(r.text)
            return None
        
        # print(r.text)
        ret = r.json()
        app_data = ret['app_data']

        retentions = []
        for data in app_data:
            app_id = data['app_id']
            # app_id 转为str
            if type(app_id) != str:
                app_id = str(app_id)
            date = data['date']
            country = data['country']
            retention = data['corrected_retention']
            
            retentions.append({
                'app_id':app_id,
                'date':date,
                'country':country,
                'retention0':retention[0],
                'retention1':retention[1],
                'retention6':retention[6],
                'retention29':retention[29],
            })

        df = pd.DataFrame(retentions)
        df.to_csv(filename,index=False)

    return df

# 获得slg收费前3名游戏的留存率
def getSlgTop3AppRetention(month = '202406'):
    slgTop3AppIdListDf = getSlgTop3AppIdList(month)
    slgTop3Df = slgTop3AppIdListDf[slgTop3AppIdListDf['unified_app_id'] != '64075e77537c41636a8e1c58']
    lastwarDf = slgTop3AppIdListDf[slgTop3AppIdListDf['unified_app_id'] == '64075e77537c41636a8e1c58']
    slgTop3AppIdList = slgTop3AppIdListDf['app_id'].unique().tolist()
    # print(slgTop3AppIdList)
    iosAppIdList = []
    androidAppIdList = []
    for app_id in slgTop3AppIdList:
        if is_number(app_id):
            iosAppIdList.append(app_id)
        else:
            androidAppIdList.append(app_id)

    # print(iosAppIdList)
    # print(androidAppIdList)

    # st只支持这些国家
    allowCountries = [
        'AU','BR','CA','DE','ES','FR','GB','IN','IT','JP','KR','US'
    ]

    start_date, end_date = get_month_start_end_dates(month)
    
    retentionDf = pd.DataFrame()

    for country in allowCountries:
        iosRetentions = getRetention(app_ids=iosAppIdList,platform='ios',date_granularity='all_time',start_date=start_date,end_date=end_date,country=country)
        iosRetentions['app_id'] = iosRetentions['app_id'].astype(str)
        iosRetentions['platform'] = 'ios'

        androidRetentions = getRetention(app_ids=androidAppIdList,platform='android',date_granularity='all_time',start_date=start_date,end_date=end_date,country=country)
        androidRetentions['platform'] = 'android'

        retentionDf = pd.concat([retentionDf,iosRetentions,androidRetentions],ignore_index=True)

    # print(retentionDf)
    slgTop3Df = pd.merge(slgTop3Df,retentionDf,on=['app_id','country'],how='left')
    slgTop3Df = slgTop3Df.groupby(['month','country','platform']).agg({
        'retention0':'mean',
        'retention1':'mean',
        'retention6':'mean',
        'retention29':'mean'
    }).reset_index()
    
    lastwarDf = pd.merge(lastwarDf,retentionDf,on=['app_id','country'],how='left')
    lastwarDf = lastwarDf[['month','country','platform','retention0','retention1','retention6','retention29']]

    retDf = pd.merge(slgTop3Df,lastwarDf,on=['month','country','platform'],how='left',suffixes=('_slg_top3_mean','_lw'))
    print(retDf)

    return retDf

# 计算上个月的月份
def get_previous_month(day):
    # 将 day 转换为 datetime 对象
    current_date = datetime.strptime(day, '%Y%m%d')
    
    # 减去一个月
    first_of_current_month = current_date.replace(day=1)
    last_of_previous_month = first_of_current_month - timedelta(days=1)
    
    # 格式化为 YYYYMM
    previous_month = last_of_previous_month.strftime('%Y%m')
    
    return previous_month

def init():
    global execSql
    global month

    tmpDir = '/src/data/'
    if not os.path.exists(tmpDir):
        os.makedirs(tmpDir)

    if 'o' in globals():
        print('this is online version')

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
        day = args['day']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        day = '20240701'
    
    # 月份是day的上个月，即如果day是20240801，则month是202407
    month = get_previous_month(day)
    print('month:',month)

from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='country', type='string', comment=''),
            Column(name='platform', type='string', comment=''),
            Column(name='retention0_slg_top3_mean', type='double', comment=''),
            Column(name='retention1_slg_top3_mean', type='double', comment=''),
            Column(name='retention6_slg_top3_mean', type='double', comment=''),
            Column(name='retention29_slg_top3_mean', type='double', comment=''),
            Column(name='retention0_lw', type='double', comment=''),
            Column(name='retention1_lw', type='double', comment=''),
            Column(name='retention6_lw', type='double', comment=''),
            Column(name='retention29_lw', type='double', comment='')
        ]
        
        partitions = [
            Partition(name='month', type='string', comment='like 202406')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('j_st_lastwar_and_slg_revenuetop3_appretention', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable(df,month):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('j_st_lastwar_and_slg_revenuetop3_appretention')
        t.delete_partition('month=%s'%(month), if_exists=True)
        with t.open_writer(partition='day=%s'%(month), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv('/src/data/j_st_lastwar_and_slg_revenuetop3_appretention_%s.csv'%(month),index=False)
    

if __name__ == '__main__':
    init()
    createTable()

    global month

    df = getSlgTop3AppRetention(month)
    writeTable(df,month)