import pandas as pd
import numpy as np

def init():
    global execSql
    global dayStr

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
        dayStr = args['dayStr']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local
        dayStr = '20240817'

    print('业务日期（昨日日期）:', dayStr)

def createTable():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='install_day', type='string', comment='安装日期'),
            Column(name='platform', type='string', comment='app identifier'),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='group_name', type='string', comment='g3__2_10'),
            Column(name='max_r', type='double', comment='max_r'),
            Column(name='pay_user_group_name', type='string', comment='like:0~2,2~10 or 10~inf'),
            Column(name='cost', type='double', comment='cost'),
            Column(name='pu_1d', type='double', comment='pay user count'),
            Column(name='revenue_1d', type='double', comment='revenue'),
            Column(name='revenue_1d_before_nerf', type='double', comment='revenue before nerf'),
            Column(name='actual_arppu', type='double', comment='actual arppu'),
            Column(name='actual_arppu_before_nerf', type='double', comment='actual arppu before nerf'),
            Column(name='predicted_arppu', type='double', comment='predicted arppu'),
            Column(name='predicted_arppu_before_nerf', type='double', comment='predicted arppu before nerf'),
            Column(name='cost_change_ratio', type='double', comment='cost change ratio'),
            Column(name='pu_change_ratio', type='double', comment='pu change ratio'),
            Column(name='is_weekend', type='int', comment='is weekend')
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)

def getConfigurations(platform, currentMondayStr, forTest = False):
    print(f"获取配置：platform={platform}, currentMondayStr={currentMondayStr}")

    # 为了测试速度
    if forTest:
        return [{
            'group_name':'g1__all',
            'max_r': 200,
            'payUserGroupList':[
                {'name': 'all', 'min': 0, 'max': np.inf}
            ],
        }]

    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
    sql = f'''
    SELECT
        group_name,
        pay_user_group,
        min_value,
        max_value,
        max_r
    FROM
        lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_configurations
    WHERE
        app = '{app_package}'
        AND day = '{currentMondayStr}'
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    data = execSql(sql)
    
    configurations = []
    grouped = data.groupby(['group_name', 'max_r'])
    for (group_name,max_r), group_data in grouped:
        payUserGroupList = []
        for _, row in group_data.iterrows():
            payUserGroupList.append({
                'name': row['pay_user_group'],
                'min': row['min_value'],
                'max': row['max_value']
            })
        configurations.append({
            'group_name': group_name,
            'max_r': max_r,
            'payUserGroupList': payUserGroupList
        })
    
    return configurations

def getHistoricalData2(dayStr):
    print(f"获取预测ARPPU和最后一天的PU：dayStr={dayStr}")

    def getHistoricalData(install_day_start, install_day_end):
        sql = f'''
@rawData :=
SELECT
	install_day,
	country,
	mediasource,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
FROM
	lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data
WHERE
	day between {install_day_start} and {install_day_end};



--main
@mainData :=
select
	install_day,
	'ALL' as country,
	'ALL' as mediasource,
	sum(revenue_1d) as revenue_1d,
	sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
	sum(pu_1d) as pu_1d,
	sum(cost) as cost,
	platform,
	group_name,
	pay_user_group,
	max_r
from
	@rawData
group by
	install_day,
	platform,
	group_name,
	pay_user_group,
	max_r;



@mediaData :=
SELECT
	install_day,
	'ALL' as country,
	case
		when mediasource = 'applovin_int' then 'APPLOVIN'
		when mediasource = 'Facebook Ads' then 'FACEBOOK'
		when mediasource = 'googleadwords_int' then 'GOOGLE'
		else mediasource
	end as mediasource,
	sum(revenue_1d) as revenue_1d,
	sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
	sum(pu_1d) as pu_1d,
	sum(cost) as cost,
	platform,
	group_name,
	pay_user_group,
	max_r
from
	@rawData
WHERE
	mediasource in (
		'applovin_int',
		'Facebook Ads',
		'googleadwords_int'
	)
group by
	install_day,
	platform,
	group_name,
	pay_user_group,
	max_r,
	mediasource;



@countryData :=
select
	install_day,
	country,
	'ALL' as mediasource,
	sum(revenue_1d) as revenue_1d,
	sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
	sum(pu_1d) as pu_1d,
	sum(cost) as cost,
	platform,
	group_name,
	pay_user_group,
	max_r
from
	@rawData
WHERE
	country in ('US', 'JP', 'KR', 'T1')
group by
	install_day,
	platform,
	group_name,
	pay_user_group,
	max_r,
	country;



@mediaAndCountryData :=
SELECT
	install_day,
	country,
	case
		when mediasource = 'applovin_int' then 'APPLOVIN'
		when mediasource = 'Facebook Ads' then 'FACEBOOK'
		when mediasource = 'googleadwords_int' then 'GOOGLE'
		else mediasource
	end as mediasource,
	sum(revenue_1d) as revenue_1d,
	sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
	sum(pu_1d) as pu_1d,
	sum(cost) as cost,
	platform,
	group_name,
	pay_user_group,
	max_r
from
	@rawData
WHERE
	mediasource in (
		'applovin_int',
		'Facebook Ads',
		'googleadwords_int'
	)
	AND country in ('US', 'JP', 'KR', 'T1')
group by
	install_day,
	platform,
	group_name,
	pay_user_group,
	max_r,
	mediasource,
	country;



@result :=
SELECT
	install_day,
	country,
	mediasource,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
FROM
	@mainData
UNION
ALL
SELECT
	install_day,
	country,
	mediasource,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
FROM
	@mediaData
UNION
ALL
SELECT
	install_day,
	country,
	mediasource,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
FROM
	@countryData
UNION
ALL
SELECT
	install_day,
	country,
	mediasource,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
FROM
	@mediaAndCountryData;

select
	install_day,
	country,
	mediasource as media,
	revenue_1d,
	revenue_1d_before_nerf,
	pu_1d,
	cost,
	platform,
	group_name,
	pay_user_group,
	max_r
from
	@result
;
        '''
        
        print("执行的SQL语句如下：\n")
        print(sql)
        
        # 执行SQL查询并返回结果
        data = execSql(sql)
        return data

    def preprocessData(data):
        data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
        
        # 确保数据按指定的列进行排序
        data = data.sort_values(by=['platform', 'group_name', 'pay_user_group', 'max_r', 'media', 'country', 'install_day'])
    
        # 计算 cost_change_ratio 和 pu_change_ratio
        data['cost_change_ratio'] = data.groupby(['platform', 'group_name', 'pay_user_group', 'max_r', 'media', 'country'])['cost'].pct_change()
        data['pu_change_ratio'] = data.groupby(['platform', 'group_name', 'pay_user_group', 'max_r', 'media', 'country'])['pu_1d'].pct_change()
        
        # 计算 actual_arppu
        data['actual_arppu'] = data['revenue_1d'] / data['pu_1d']
        data['actual_arppu_before_nerf'] = data['revenue_1d_before_nerf'] / data['pu_1d']

        # 计算 predicted_arppu
        data['predicted_arppu'] = data.groupby(['platform', 'group_name', 'pay_user_group', 'max_r', 'media', 'country'])['actual_arppu'].transform(lambda x: x.rolling(window=15, min_periods=1).mean().shift(1))
        data['predicted_arppu_before_nerf'] = data.groupby(['platform', 'group_name', 'pay_user_group', 'max_r', 'media', 'country'])['actual_arppu_before_nerf'].transform(lambda x: x.rolling(window=15, min_periods=1).mean().shift(1))

        # 添加 is_weekend 列
        data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
        data['is_weekend'] = data['install_day'].dt.dayofweek.apply(lambda x: 1 if x >= 5 else 0)
        
        return data

    # 获取从dayStr往前推N天的数据，计算平均ARPPU作为预测的ARPPU 
    N = 16

    endDate = pd.to_datetime(dayStr, format='%Y%m%d')
    endDateStr = endDate.strftime('%Y%m%d')

    startDate = endDate - pd.Timedelta(days=N)
    startDateStr = startDate.strftime('%Y%m%d')    

    historical_data = getHistoricalData(startDateStr, endDateStr)
    retDf = preprocessData(historical_data)

    retDf = retDf[retDf['install_day'] == endDate]

    retDf['install_day'] = retDf['install_day'].dt.strftime('%Y%m%d')
    retDf.rename(columns={'pay_user_group': 'pay_user_group_name'}, inplace=True)
    return retDf



def main(dayStr):
    predictArppuAndLastPu = getHistoricalData2(dayStr)

    writeTable(predictArppuAndLastPu, dayStr)

if __name__ == '__main__':
    init()
    createTable()
    deletePartition(dayStr)

    main(dayStr)
