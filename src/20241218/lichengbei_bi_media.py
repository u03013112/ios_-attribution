import os
import pandas as pd

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
        dayStr = '20240902'

    print('dayStr:', dayStr)


def createTable1():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='startday', type='string', comment=''),
            Column(name='endday', type='string', comment=''),
            Column(name='install_day', type='string', comment=''),
            Column(name='media', type='string', comment=''),
            Column(name='cost', type='double', comment=''),
            Column(name='sum_cost', type='double', comment=''),
            Column(name='target_usd', type='double', comment=''),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_lcb_pic1_media', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def createTable2():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='platform', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='startday', type='string', comment=''),
            Column(name='endday', type='string', comment=''),
            Column(name='install_day', type='string', comment=''),
            Column(name='media', type='string', comment=''),
            Column(name='cost', type='double', comment=''),
            Column(name='sum_cost', type='double', comment=''),
            Column(name='sum_d7roi', type='double', comment=''),
            Column(name='target_d7roi', type='double', comment=''),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_lcb_pic2_meida', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr, tableName):
    if 'o' in globals():
        t = o.get_table(tableName)
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr, tableName):
    print('try to write table:')
    print(df.head(5))
    if len(df) == 0:
        print('No data to write.')
        return
    if 'o' in globals():
        t = o.get_table(tableName)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)
        df.to_csv('/src/data/lastwar_lcb_pic1_media.csv', index=False)



def getLichengbeiData():
    
    sql = '''
select
    startday,
    endday,
    app_package_sys,
    country_group,
    target_usd,
    target_d7roi
from ads_application_lastwar_milestones
;
        '''
    data = execSql(sql)

    return data

def getAndroidData():
    sql = '''
select
    install_day,
    country,
    mediasource as media,
    sum(usd) as cost,
    sum(d7) as revenue_d7
from tmp_lw_cost_and_roi_by_day
where
    install_day >= 20240401
group by
    install_day,
    country,
    mediasource
;
        '''
    data = execSql(sql)
    
    return data

def getIosData():
    sql = '''
select
    install_day,
    country,
    mediasource as media,
    sum(usd) as cost,
    sum(d7) as revenue_d7
from tmp_lw_cost_and_roi_by_day_ios
where
    install_day >= 20240401
group by
    install_day,
    country,
    mediasource
;
        '''
    data = execSql(sql)
        
    return data

def main():
    getLichengbeiDf = getLichengbeiData()
    # 解决有中间变化KPI的问题，只取最小的
    getLichengbeiDf = getLichengbeiDf.groupby(['startday', 'endday', 'app_package_sys', 'country_group', 'target_usd']).agg({
        'target_d7roi': 'min'
    }).reset_index()
    getLichengbeiDf = getLichengbeiDf[getLichengbeiDf['app_package_sys'].isin(['AOS', 'IOS'])]
    
    androidDf = getAndroidData()
    androidDf['platform'] = 'AOS'

    iosDf = getIosData()
    iosDf['platform'] = 'IOS'

    df = pd.concat([androidDf, iosDf], axis=0)

    # 做一些数据处理
    lichengbeiCountryList = getLichengbeiDf['country_group'].unique()
    # df中的country 只保留在lichengbei中的，剩余的都统一为other
    df['country'] = df['country'].apply(lambda x: x if x in lichengbeiCountryList else 'OTHER')
    df = df.groupby(['install_day', 'platform', 'country', 'media']).agg({
        'cost': 'sum',
        'revenue_d7': 'sum'
    }).reset_index()

    # 结果Df需要列： day, platform, country, cost, revenue_d7, target_usd, target_d7roi, sum_cost, sum_revenue_d7, sum_d7roi
    # 其中day：安装日期，platform：平台，country：国家，cost：当日成本，revenue_d7：当日d7收入，
    # target_usd：lichengbei中的目标usd,lichengbei与df的关联条件是platform和country相同，startday <= day <= endday
    # target_d7roi：lichengbei中的目标d7roi,
    # sum_cost：当日成本累计，累计条件式同一个里程碑内的cost的和
    # sum_revenue_d7：当日d7收入累计
    # sum_d7roi：当日d7roi累计，sum_revenue_d7 / sum_cost

    # 初始化结果 DataFrame
    result_df = pd.DataFrame()

    # 遍历每个里程碑
    for _, row in getLichengbeiDf.iterrows():
        startday = row['startday']
        endday = row['endday']
        platform = row['app_package_sys']
        country = row['country_group']
        target_usd = row['target_usd']
        target_d7roi = row['target_d7roi']

        # 过滤出符合条件的 df 数据
        mask = (
            (df['install_day'] >= startday) &
            (df['install_day'] <= endday) &
            (df['platform'] == platform) &
            (df['country'] == country)
        )
        filtered_df = df[mask].copy()

        # 按照 install_day 排序
        filtered_df = filtered_df.sort_values(by='install_day')

        # 按照media+install_day分组
        media_group = filtered_df.groupby(['media'])

        media_result_df = pd.DataFrame()
        for media, media_group_df in media_group:
            # 计算累计值
            media_group_df['sum_cost'] = media_group_df['cost'].cumsum()
            media_group_df['startday'] = startday
            media_group_df['endday'] = endday
            media_group_df['target_usd'] = target_usd
            # 合并到结果 DataFrame
            media_result_df = pd.concat([media_result_df, media_group_df], axis=0)

        # 计算累计值
        filtered_df = filtered_df.groupby(['install_day', 'platform', 'country']).agg({
            'cost': 'sum',
            'revenue_d7': 'sum'
        }).reset_index()

        filtered_df['sum_cost'] = filtered_df['cost'].cumsum()
        filtered_df['sum_revenue_d7'] = filtered_df['revenue_d7'].cumsum()
        filtered_df['sum_d7roi'] = filtered_df['sum_revenue_d7'] / filtered_df['sum_cost']
        filtered_df['startday'] = startday
        filtered_df['endday'] = endday
        filtered_df['target_usd'] = target_usd
        filtered_df['target_d7roi'] = target_d7roi
        

        result_df_tmp = media_result_df.merge(filtered_df[['install_day', 'platform', 'country', 'sum_d7roi', 'target_d7roi']], 
                                            on=['install_day', 'platform', 'country'], how='left')

        # 合并到结果 DataFrame
        result_df = pd.concat([result_df, result_df_tmp], axis=0)

    # 重置索引
    result_df.reset_index(drop=True, inplace=True)

    # sum_d7roi < target_d7roi 的记录, sum_cost = 0
    mask = result_df['sum_d7roi'] < result_df['target_d7roi']
    result_df.loc[mask, 'sum_cost'] = 0

    # result_df['install_day'] = pd.to_datetime(result_df['install_day'], format='%Y%m%d')

    # 大盘
    totalDf = result_df.groupby(['install_day','media']).agg({
        'cost': 'sum',
        'sum_cost': 'sum',
        'startday':'max',
        'endday':'max',
        'target_usd':'max'
    }).reset_index()
    
    startdayList = totalDf['startday'].unique()
    for day in startdayList:
        deletePartition(day, 'lastwar_lcb_pic1_media')
        deletePartition(day, 'lastwar_lcb_pic2_meida')

    for (startday, endday), group in totalDf.groupby(['startday', 'endday']):
        pic1Df = group[['startday','endday','install_day', 'media', 'cost', 'sum_cost', 'target_usd']].copy()
        # write to DB
        startdayList = pic1Df['startday'].unique()
        for day in startdayList:
            pic1Df2 = pic1Df[pic1Df['startday'] == day]
            writeTable(pic1Df2, day, 'lastwar_lcb_pic1_media')

    for (platform, country, media, startday, endday), group in result_df.groupby(['platform', 'country', 'media', 'startday', 'endday']):
        pic2Df = group[['platform','country', 'media', 'startday','endday','install_day','cost','sum_cost','sum_d7roi','target_d7roi']].copy()
        # write to DB
        startdayList = pic2Df['startday'].unique()
        for day in startdayList:
            pic2Df2 = pic2Df[pic2Df['startday'] == day]
            writeTable(pic2Df2, day, 'lastwar_lcb_pic2_meida')
        
    
if __name__ == '__main__':
    init()
    createTable1()
    createTable2()
    main()
