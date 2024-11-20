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
        dayStr = '20240930'  # 本地测试时的日期，可自行修改

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='app', type='string', comment='app package'),
            Column(name='platform', type='string', comment='platform'),
            Column(name='media', type='string', comment='media source'),
            Column(name='country', type='string', comment='country'),
            Column(name='group_name', type='string', comment='group name'),
            Column(name='max_r', type='double', comment='max_r'),
            Column(name='min_mape', type='double', comment='minimum MAPE'),
            Column(name='day_mape', type='double', comment='day MAPE')
        ]
        partitions = [
            Partition(name='day', type='string', comment='预测日期')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_min_week_mape_report2'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_min_week_mape_report2'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')

def writeToTable(df, dayStr):
    print('try to write verification results to table:')
    print(df.head(5))
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_min_week_mape_report2'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeToTable failed, o is not defined')
        print(dayStr)
        print(df)

def getHistoricalData(installDayStart, installDayEnd):
    sql = f'''
select
    app,
    platform,
    install_day,
    media,
    country,
    group_name,
    pay_user_group_name,
    actual_pu,
    predicted_pu,
    actual_arppu,
    predicted_arppu,
    actual_revenue,
    predicted_revenue,
    max_r
from lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_test2
where day > 0
and install_day between {installDayStart} and {installDayEnd}
;
    '''
    data = execSql(sql)
    return data

def getMinWeekMape(installDayStart, installDayEnd):
    print(f"获取最小MAPE：installDayStart={installDayStart}, installDayEnd={installDayEnd}")
    # 获取历史数据
    historical_data = getHistoricalData(installDayStart, installDayEnd)
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    # 计算 天MAPE
    dayDf = historical_data.groupby(['install_day','app','platform', 'media', 'country', 'group_name','max_r']).agg({
        'actual_revenue': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()

    dayDf['mape_revenue'] = np.abs((dayDf['actual_revenue'] - dayDf['predicted_revenue']) / dayDf['actual_revenue'])
    dayDf2 = dayDf.groupby(['app','platform','media', 'country', 'group_name','max_r']).agg({
        'mape_revenue': 'mean'
    }).reset_index()

    # 计算 周MAPE
    historical_data['week'] = historical_data['install_day'].dt.strftime('%Y-%W')
    weekDf = historical_data.groupby(['week','app','platform', 'media', 'country', 'group_name','max_r']).agg({
        'actual_revenue': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()
    weekDf['mape_revenue'] = np.abs((weekDf['actual_revenue'] - weekDf['predicted_revenue']) / weekDf['actual_revenue'])

    weekDf2 = weekDf.groupby(['app','platform','media', 'country', 'group_name','max_r']).agg({
        'mape_revenue': 'mean'
    }).reset_index()

    # 找到按照 media 和 country 分组的最小 MAPE 对应的 group_name，以及最小的 MAPE 值
    minMapeDf2 = weekDf2.groupby(['app','platform','media', 'country','max_r']).agg(
        minMape=('mape_revenue', 'min')
    ).reset_index()

    minMapeDf2 = minMapeDf2.merge(weekDf2, on=['app','platform','media', 'country','max_r'], how='left')
    minMapeDf2 = minMapeDf2[minMapeDf2['mape_revenue'] == minMapeDf2['minMape']]
    minMapeDf2 = minMapeDf2.drop_duplicates(subset=['app','platform','media', 'country','max_r'])

    # 合并日MAPE
    minMapeDf2 = minMapeDf2.merge(dayDf2, on=['app','platform','media', 'country', 'group_name','max_r'], suffixes=('_week', '_day'))

    # 选择需要的列
    resultDf = minMapeDf2[['app','platform','media', 'country', 'group_name', 'minMape', 'mape_revenue_day','max_r']]
    resultDf.rename(columns={'mape_revenue_day': 'dayMape'}, inplace=True)
    return resultDf

def main():
    init()
    createTable()
    
    # 统计往前推N周的数据
    N = 8
    
    # 获取当前日期
    currentDate = datetime.strptime(dayStr, '%Y%m%d')
    # 找到本周一
    currentMonday = currentDate - timedelta(days=currentDate.weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')
    
    lastSunday = currentMonday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    nWeeksAgo = pd.to_datetime(currentMonday, format='%Y%m%d') - pd.Timedelta(weeks=N)
    nWeeksAgoStr = nWeeksAgo.strftime('%Y%m%d')

    minWeekMapeDf = getMinWeekMape(nWeeksAgoStr, lastSundayStr)
    
    minWeekMapeDf = minWeekMapeDf.rename(columns={'minMape': 'min_mape', 'dayMape': 'day_mape'})

    # 删除旧分区
    deletePartition(currentMondayStr)
    
    # 写入表格
    writeToTable(minWeekMapeDf, currentMondayStr)

if __name__ == "__main__":
    main()
