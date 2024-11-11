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

def getHistoricalData(install_day_start, install_day_end, platform='android', group_name=None):
    table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__historical_data'
    
    # 构建SQL查询语句
    sql = f'''
SELECT
    install_day,
    country,
    mediasource,
    revenue_1d,
    pu_1d,
    cost,
    platform,
    group_name,
    pay_user_group
FROM
    {table_name}
WHERE
    day BETWEEN '{install_day_start}' AND '{install_day_end}'
    AND platform = '{platform}'
    '''
    
    # 如果指定了group_name，则添加到查询条件中
    if group_name:
        sql += f" AND group_name = '{group_name}'"
    
    print("执行的SQL语句如下：\n")
    print(sql)
    
    # 执行SQL查询并返回结果
    data = execSql(sql)
    
    return data

def preprocessData(data, media=None, country=None):
    """
    预处理数据，包括日期转换、过滤、聚合、重塑和特征工程。
    """

    # 1. 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
    
    # 2. 过滤数据
    if media:
        data = data[data['mediasource'] == media]
    if country:
        data = data[data['country'] == country]
    
    # 3. 按 'install_day' 和 'pay_user_group' 分组并汇总所需列
    aggregation_dict = {
        'cost': 'sum',
        'revenue_1d': 'sum',
        'pu_1d': 'sum'
    }
    
    aggregated_data = data.groupby(['install_day', 'pay_user_group']).agg(aggregation_dict).reset_index()
    
    # 4. 计算 cost_change_ratio 和 pu_change_ratio
    aggregated_data['cost_change_ratio'] = aggregated_data.groupby('pay_user_group')['cost'].pct_change()
    aggregated_data['pu_change_ratio'] = aggregated_data.groupby('pay_user_group')['pu_1d'].pct_change()
    
    # 计算实际成本和付费用户的前一天值
    aggregated_data['actual_cost_shifted'] = aggregated_data.groupby('pay_user_group')['cost'].shift(1)
    aggregated_data['actual_pu_shifted'] = aggregated_data.groupby('pay_user_group')['pu_1d'].shift(1)
    
    # 移除第一天（无法计算变动比例）
    aggregated_data = aggregated_data.dropna(subset=['cost_change_ratio', 'pu_change_ratio'])
    
    # 5. 计算 actual_ARPPU 和 predicted_ARPPU
    # 计算实际 ARPPU
    aggregated_data['actual_arppu'] = aggregated_data['revenue_1d'] / aggregated_data['pu_1d']
    aggregated_data['actual_arppu'].replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 计算预测 ARPPU：先shift一天，再计算过去15天的均值
    aggregated_data['actual_arppu_shifted'] = aggregated_data.groupby('pay_user_group')['actual_arppu'].shift(1)
    aggregated_data['predicted_arppu'] = aggregated_data.groupby('pay_user_group')['actual_arppu_shifted'].rolling(window=15, min_periods=1).mean().reset_index(level=0, drop=True)
    
    # 6. 重命名和选择最终列
    aggregated_data = aggregated_data.rename(columns={'install_day': 'ds', 'pu_change_ratio': 'y'})
    
    # 最终选择列
    df = aggregated_data[['ds', 'actual_cost_shifted', 'cost', 'cost_change_ratio', 'actual_pu_shifted', 'pu_1d', 'y', 'pay_user_group', 'actual_arppu', 'predicted_arppu', 'revenue_1d']]
    
    # 添加周末特征
    df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)

    return df

# 示例调用
if __name__ == "__main__":
    # 初始化数据库连接等
    init()
    
    # 读取历史数据
    install_day_start = '20241001'
    install_day_end = '20241110'
    platform = 'android'
    group_name = 'g2__2'
    
    historical_data = getHistoricalData(install_day_start, install_day_end, platform, group_name)
    print(historical_data.head())

    # 预处理数据
    processed_data = preprocessData(historical_data)
    print(processed_data.sort_values(['pay_user_group','ds']))
