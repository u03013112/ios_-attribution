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
        # 创建历史数据表（如果不存在）
        hist_columns = [
            Column(name='install_day', type='string', comment='安装日期'),
            Column(name='country', type='string', comment='国家'),
            Column(name='mediasource', type='string', comment='媒体来源'),
            Column(name='revenue_1d', type='double', comment='1天收入'),
            Column(name='pu_1d', type='bigint', comment='1天付费用户数'),
            Column(name='cost', type='double', comment='成本'),
            Column(name='platform', type='string', comment='平台'),
            Column(name='group_name', type='string', comment='组名'),
            Column(name='pay_user_group', type='string', comment='付费用户组')
        ]
        hist_partitions = [
            Partition(name='day', type='string', comment='预测日期')
        ]
        hist_schema = Schema(columns=hist_columns, partitions=hist_partitions)
        hist_table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__historical_data'
        o.create_table(hist_table_name, hist_schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(table_name, dayStr):
    if 'o' in globals():
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')

def getConfigurations(platform, dayStr):
    """
    从数据库中读取配置，并组装成Python对象。
    """
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
    sql = f'''
    SELECT
        group_name,
        pay_user_group,
        min_value,
        max_value
    FROM
        lastwar_predict_day1_pu_pct_by_cost_pct__configurations
    WHERE
        app = '{app_package}'
        AND day = '{dayStr}'
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    data = execSql(sql)
    
    configurations = []
    grouped = data.groupby('group_name')
    for group_name, group_data in grouped:
        payUserGroupList = []
        for _, row in group_data.iterrows():
            payUserGroupList.append({
                'name': row['pay_user_group'],
                'min': row['min_value'],
                'max': row['max_value']
            })
        configurations.append({
            'group_name': group_name,
            'payUserGroupList': payUserGroupList
        })
    
    return configurations

def generate_case_statements(group_list, value_field, aggregate='SUM', is_count=False):
    """
    生成SQL的CASE语句部分，用于按组聚合字段。
    """
    statements = []
    for group in group_list:
        # 处理Infinity的情况
        max_value = '999999999' if group['max'] == np.inf else group['max']
        if is_count:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN 1
                ELSE 0
            END
        ) AS pu_1d_{group['name']},
            """
        else:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN {value_field}
                ELSE 0
            END
        ) AS revenue_1d_{group['name']},
            """
        statements.append(statement)
    return "\n".join(statements)

def getHistoricalData(dayStr, platform='android', payUserGroupList=None):
    # 根据平台选择不同的表名和应用包名
    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'

    # 生成动态的CASE语句
    if payUserGroupList:
        revenue_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=False)
        count_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=True)
    else:
        revenue_case = ""
        count_case = ""

    sql = f'''
    @cost_data :=
    SELECT
        install_day,
        CASE 
            WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
            ELSE mediasource 
        END AS mediasource,
        country,
        SUM(usd) AS usd
    FROM
        {table_name}
    WHERE
        install_day = '{dayStr}'
    GROUP BY
        install_day,
        CASE 
            WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
            ELSE mediasource 
        END,
        country
    ;

    @d1_purchase_data :=
    SELECT
        install_day,
        game_uid,
        country,
        mediasource,
        SUM(revenue_value_usd) AS revenue_1d
    FROM
        dwd_overseas_revenue_allproject
    WHERE
        app = 502
        AND app_package = '{app_package}'
        AND zone = 0
        AND day >= '{dayStr}'
        AND install_day = '{dayStr}'
        AND DATEDIFF(
            FROM_UNIXTIME(event_time),
            FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
            'dd'
        ) = 0
    GROUP BY
        install_day,
        game_uid,
        country,
        mediasource
    ;

    @country_map :=
    SELECT
        d1.game_uid,
        d1.install_day,
        d1.country,
        d1.mediasource,
        d1.revenue_1d,
        map.countrygroup AS countrygroup
    FROM
        @d1_purchase_data AS d1
        LEFT JOIN cdm_laswwar_country_map AS map 
            ON d1.country = map.country
    ;

    @result :=
    SELECT
        install_day,
        countrygroup AS country,
        mediasource,
        {revenue_case}
        {count_case}
        SUM(revenue_1d) AS revenue_1d,
        COUNT(DISTINCT game_uid) AS pu_1d
    FROM
        @country_map
    GROUP BY
        install_day,
        countrygroup,
        mediasource
    ;

    SELECT
        COALESCE(r.install_day, c.install_day) AS install_day,
        COALESCE(r.country, c.country) AS country,
        COALESCE(r.mediasource, c.mediasource) AS mediasource,
        {', '.join([f"r.revenue_1d_{group['name']}" for group in payUserGroupList]) if payUserGroupList else ''},
        {', '.join([f"r.pu_1d_{group['name']}" for group in payUserGroupList]) if payUserGroupList else ''},
        r.revenue_1d,
        r.pu_1d,
        c.usd AS cost
    FROM
        @result AS r
        FULL OUTER JOIN @cost_data AS c 
            ON r.install_day = c.install_day
            AND r.country = c.country
            AND r.mediasource = c.mediasource
    ;
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    data = execSql(sql)
    
    return data

def processHistoricalData(data, platform, group_name, payUserGroupList):
    processed_data = pd.DataFrame()
    for group in payUserGroupList:
        temp_data = data[['install_day', 'country', 'mediasource', f'revenue_1d_{group["name"]}', f'pu_1d_{group["name"]}', 'cost']].copy()
        temp_data.rename(columns={f'revenue_1d_{group["name"]}': 'revenue_1d', f'pu_1d_{group["name"]}': 'pu_1d'}, inplace=True)
        temp_data['platform'] = platform
        temp_data['group_name'] = group_name
        temp_data['pay_user_group'] = group['name']
        processed_data = pd.concat([processed_data, temp_data], ignore_index=True)
    
    return processed_data

def writeHistoricalDataToTable(data, dayStr):
    print('try to write historical data to table:')
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__historical_data'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(data)
        print(f"Historical data written to table partition day={dayStr}.")
    else:
        print('writeHistoricalDataToTable failed, o is not defined')
        print(dayStr)
        print(data)

def main():
    init()
    createTable()
    
    # 获取当前日期
    currentDate = datetime.strptime(dayStr, '%Y%m%d')
    # 找到本周一
    currentMonday = currentDate - timedelta(days=currentDate.weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')
    
    # 删除旧分区
    deletePartition('lastwar_predict_day1_pu_pct_by_cost_pct__historical_data', dayStr)
    
    # 获取配置
    configurations_android = getConfigurations('android', currentMondayStr)
    configurations_ios = getConfigurations('ios', currentMondayStr)

    # 获取历史数据并处理
    for config in configurations_android:
        historical_data = getHistoricalData(dayStr, 'android', config['payUserGroupList'])
        processed_data = processHistoricalData(historical_data, 'android', config['group_name'], config['payUserGroupList'])
        writeHistoricalDataToTable(processed_data, dayStr)
    
    for config in configurations_ios:
        historical_data = getHistoricalData(dayStr, 'ios', config['payUserGroupList'])
        processed_data = processHistoricalData(historical_data, 'ios', config['group_name'], config['payUserGroupList'])
        writeHistoricalDataToTable(processed_data, dayStr)

if __name__ == "__main__":
    main()
