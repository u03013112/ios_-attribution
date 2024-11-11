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
        dayStr = '20241104'  # 本地测试时的日期，可自行修改

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='app', type='string', comment='app package'),
            Column(name='group_name', type='string', comment='group name'),
            Column(name='pay_user_group', type='string', comment='pay user group'),
            Column(name='min_value', type='double', comment='minimum value'),
            Column(name='max_value', type='double', comment='maximum value')
        ]
        partitions = [
            Partition(name='day', type='string', comment='预测日期')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__configurations'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__configurations'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')

def writeConfigurationsToTable(configurations, app_package, dayStr):
    print('try to write configurations to table:')
    config_list = []
    for config in configurations:
        group_name = config['group_name']
        for group in config['payUserGroupList']:
            max_value = group['max'] if group['max'] != np.inf else 1e10  # 将 np.inf 替换为 1e10
            config_list.append({
                'app': app_package,
                'group_name': group_name,
                'pay_user_group': group['name'],
                'min_value': group['min'],
                'max_value': max_value
            })
    config_df = pd.DataFrame(config_list)
    print(config_df.head(5))
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__configurations'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(config_df)
        print(f"Configurations written to table partition day={dayStr}.")
    else:
        print('writeConfigurationsToTable failed, o is not defined')
        print(dayStr)
        print(config_df)

def getConfigurations(platform, lastSundayStr):
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
    lastSunday = pd.to_datetime(lastSundayStr, format='%Y%m%d')
    startDay = lastSunday - pd.Timedelta(weeks=8) + timedelta(days=1)  # 8周前的周一
    startDayStr = startDay.strftime('%Y%m%d')

    sql = f'''
WITH d1_purchase_data AS (
    SELECT
        install_day,
        game_uid,
        SUM(revenue_value_usd) AS revenue_1d
    FROM
        dwd_overseas_revenue_allproject
    WHERE
        app = 502
        AND app_package = '{app_package}'
        AND zone = 0
        AND day >= {startDayStr}
        AND install_day BETWEEN {startDayStr} AND {lastSundayStr}
        AND DATEDIFF(
            FROM_UNIXTIME(event_time),
            FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
            'dd'
        ) = 0
    GROUP BY
        install_day,
        game_uid
),
ranked_data AS (
    SELECT
        revenue_1d,
        NTILE(100) OVER (
            ORDER BY
                revenue_1d
        ) AS percentile_rank
    FROM
        d1_purchase_data
)
SELECT
    MAX(
        CASE
            WHEN percentile_rank = 12 THEN revenue_1d
        END
    ) AS p12_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 25 THEN revenue_1d
        END
    ) AS p25_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 37 THEN revenue_1d
        END
    ) AS p37_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 50 THEN revenue_1d
        END
    ) AS p50_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 62 THEN revenue_1d
        END
    ) AS p62_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 75 THEN revenue_1d
        END
    ) AS p75_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 87 THEN revenue_1d
        END
    ) AS p87_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 100 THEN revenue_1d
        END
    ) AS p100_revenue_1d
FROM
    ranked_data;
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    data = execSql(sql)
    
    p12 = data['p12_revenue_1d'].values[0]
    p25 = data['p25_revenue_1d'].values[0]
    p37 = data['p37_revenue_1d'].values[0]
    p50 = data['p50_revenue_1d'].values[0]
    p62 = data['p62_revenue_1d'].values[0]
    p75 = data['p75_revenue_1d'].values[0]
    p87 = data['p87_revenue_1d'].values[0]

    configurations = [
        {
            'group_name':'g1__all',
            'payUserGroupList':[
                {'name': 'all', 'min': 0, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ],
        },
        {
            'group_name':'g2__2',
            'payUserGroupList':[
                {'name': '0_2', 'min': 0, 'max': 2},
                {'name': '2_inf', 'min': 2, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ],
        },
        {
            'group_name': 'g2__percentile50',
            'payUserGroupList': [
                {'name': '0_50', 'min': 0, 'max': p50},
                {'name': '50_inf', 'min': p50, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ]
        },
        {
            'group_name': 'g4__percentile25_50_75',
            'payUserGroupList': [
                {'name': '0_25', 'min': 0, 'max': p25},
                {'name': '25_50', 'min': p25, 'max': p50},
                {'name': '50_75', 'min': p50, 'max': p75},
                {'name': '75_inf', 'min': p75, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ]
        },
        {
            'group_name': 'g8__percentile12_25_37_50_62_75_87',
            'payUserGroupList': [
                {'name': '0_12', 'min': 0, 'max': p12},
                {'name': '12_25', 'min': p12, 'max': p25},
                {'name': '25_37', 'min': p25, 'max': p37},
                {'name': '37_50', 'min': p37, 'max': p50},
                {'name': '50_62', 'min': p50, 'max': p62},
                {'name': '62_75', 'min': p62, 'max': p75},
                {'name': '75_87', 'min': p75, 'max': p87},
                {'name': '87_inf', 'min': p87, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ]
        }
    ]

    return configurations

def main():
    init()
    createTable()
    
    # 获取当前日期
    currentDate = datetime.strptime(dayStr, '%Y%m%d')
    # 找到本周一
    currentMonday = currentDate - timedelta(days=currentDate.weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')
    
    lastSunday = currentMonday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    # 删除旧分区
    deletePartition(currentMondayStr)
    
    # 获取配置并写入表格
    configurations_android = getConfigurations('android', lastSundayStr)
    configurations_ios = getConfigurations('ios', lastSundayStr)
    
    writeConfigurationsToTable(configurations_android, 'com.fun.lastwar.gp', currentMondayStr)
    writeConfigurationsToTable(configurations_ios, 'id6448786147', currentMondayStr)

if __name__ == "__main__":
    main()
