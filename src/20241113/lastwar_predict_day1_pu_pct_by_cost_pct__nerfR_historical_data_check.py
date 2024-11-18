import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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
            print(sql)
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                print(pd_df.head(5))
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
        dayStr = '20240618'  # 本地测试时的日期，可自行修改

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='install_day', type='string', comment='安装日期'),
            Column(name='app', type='string', comment='app package'),
            Column(name='country', type='string', comment='国家'),
            Column(name='mediasource', type='string', comment='媒体来源'),
            Column(name='max_r', type='double', comment='maximum revenue'),
            Column(name='revenue_1d', type='double', comment='1天收入'),
            Column(name='revenue_1d_before_nerf', type='double', comment='1天收入,削弱前'),
            Column(name='nerf_ratio', type='double', comment='削弱比例')
        ]
        partitions = [
            Partition(name='day', type='string', comment='预测日期')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data_check'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data_check'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')

def writeToTable(data, dayStr):
    print('try to write data to table:')
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data_check'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(data)
        print(f"data written to table partition day={dayStr}.")
    else:
        print('writeToTable failed, o is not defined')
        print(dayStr)
        print(data)

# 大盘
def main(dayStr):
    platformList = ['android', 'ios']

    for platform in platformList:
        sql = f'''
select
    install_day,
    group_name,
    sum(revenue_1d) as revenue_1d,
    sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
    (1 - (sum(revenue_1d)/sum(revenue_1d_before_nerf))) as nerf_ratio,
    max_r
from
    lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data
where
    day = '{dayStr}'
    and platform = '{platform}'
    and group_name = 'g1__all'
group by
    install_day,
    group_name,
    max_r
;
        '''
        # print(sql)
        data = execSql(sql)
        data = data[['install_day', 'revenue_1d', 'revenue_1d_before_nerf', 'nerf_ratio', 'max_r']]
        data['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        data['country'] = 'ALL'
        data['mediasource'] = 'ALL'
        print(data.head(5))

        writeToTable(data, dayStr)

def mainMedia(dayStr):
    platformList = ['android']

    for platform in platformList:
        sql = f'''
select
    install_day,
    mediasource,
    group_name,
    sum(revenue_1d) as revenue_1d,
    sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
    (1 - (sum(revenue_1d)/sum(revenue_1d_before_nerf))) as nerf_ratio,
    max_r
from
    lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data
where
    day = '{dayStr}'
    and platform = '{platform}'
    and group_name = 'g1__all'
group by
    install_day,
    group_name,
    mediasource,
    max_r
;
        '''
        # print(sql)
        data = execSql(sql)

        # media过滤，
        interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int']
        data = data[data['mediasource'].isin(interested_medias)]
        # media改名
        data['mediasource'] = data['mediasource'].replace('applovin_int', 'APPLOVIN')
        data['mediasource'] = data['mediasource'].replace('Facebook Ads', 'FACEBOOK')
        data['mediasource'] = data['mediasource'].replace('googleadwords_int', 'GOOGLE')

        data = data[['install_day', 'mediasource', 'revenue_1d', 'revenue_1d_before_nerf', 'nerf_ratio', 'max_r']]
        data['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        data['country'] = 'ALL'
        print(data.head(5))

        writeToTable(data, dayStr)


def mainCountry(dayStr):
    platformList = ['android','ios']

    for platform in platformList:
        sql = f'''
select
    install_day,
    country,
    group_name,
    sum(revenue_1d) as revenue_1d,
    sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
    (1 - (sum(revenue_1d)/sum(revenue_1d_before_nerf))) as nerf_ratio,
    max_r
from
    lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data
where
    day = '{dayStr}'
    and platform = '{platform}'
    and group_name = 'g1__all'
group by
    install_day,
    group_name,
    country,
    max_r
;
        '''
        # print(sql)
        data = execSql(sql)
        interested_countries = ['US', 'JP', 'KR', 'T1']
        data = data[data['country'].isin(interested_countries)]

        data = data[['install_day', 'country', 'revenue_1d', 'revenue_1d_before_nerf', 'nerf_ratio', 'max_r']]
        data['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        data['mediasource'] = 'ALL'
        print(data.head(5))

        writeToTable(data, dayStr)

def mainMediaAndCountry(dayStr):
    platformList = ['android']

    for platform in platformList:
        sql = f'''
select
    install_day,
    mediasource,
    country,
    group_name,
    sum(revenue_1d) as revenue_1d,
    sum(revenue_1d_before_nerf) as revenue_1d_before_nerf,
    (1 - (sum(revenue_1d)/sum(revenue_1d_before_nerf))) as nerf_ratio,
    max_r
from
    lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data
where
    day = '{dayStr}'
    and platform = '{platform}'
    and group_name = 'g1__all'
group by
    install_day,
    group_name,
    mediasource,
    country,
    max_r
;
        '''
        # print(sql)
        data = execSql(sql)

        # media过滤，
        interested_medias = ['applovin_int', 'Facebook Ads', 'googleadwords_int']
        data = data[data['mediasource'].isin(interested_medias)]
        # media改名
        data['mediasource'] = data['mediasource'].replace('applovin_int', 'APPLOVIN')
        data['mediasource'] = data['mediasource'].replace('Facebook Ads', 'FACEBOOK')
        data['mediasource'] = data['mediasource'].replace('googleadwords_int', 'GOOGLE')

        interested_countries = ['US', 'JP', 'KR', 'T1']
        data = data[data['country'].isin(interested_countries)]

        data = data[['install_day', 'mediasource', 'country', 'revenue_1d', 'revenue_1d_before_nerf', 'nerf_ratio', 'max_r']]
        data['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        print(data.head(5))

        writeToTable(data, dayStr)

if __name__ == '__main__':
    init()
    createTable()
    deletePartition(dayStr)

    main(dayStr)
    mainMedia(dayStr)
    mainCountry(dayStr)
    mainMediaAndCountry(dayStr)
