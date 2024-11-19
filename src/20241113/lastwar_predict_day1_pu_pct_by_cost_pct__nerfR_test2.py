# 改为逐天预测，每天的预测结果都会写入表格
import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

def init():
    global execSql
    global dayStr
    global model_cache
    global historicalData_cache
    global predict_cache

    model_cache = {}
    historicalData_cache = {}
    predict_cache = {}

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
        dayStr = '20241113'  # 本地测试时的日期，可自行修改

    print('测试日期:', dayStr)

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='install_day', type='string', comment='install day'),
            Column(name='app', type='string', comment='app identifier'),
            Column(name='platform', type='string', comment='platform'),
            Column(name='media', type='string', comment='media source'),
            Column(name='country', type='string', comment='country'),
            Column(name='group_name', type='string', comment='group name'),
            Column(name='max_r', type='double', comment='maximum revenue'),
            Column(name='pay_user_group_name', type='string', comment='pay user group name'),
            Column(name='actual_pu', type='double', comment='actual pay users'),
            Column(name='predicted_pu', type='double', comment='predicted pay users'),
            Column(name='actual_arppu', type='double', comment='actual ARPPU'),
            Column(name='predicted_arppu', type='double', comment='predicted ARPPU'),
            Column(name='actual_revenue', type='double', comment='actual revenue'),
            Column(name='predicted_revenue', type='double', comment='predicted revenue')
        ]
        partitions = [
            Partition(name='day', type='string', comment='prediction date, like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_test2'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_test2'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')


def getHistoricalData(install_day_start, install_day_end):
    table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2'
    
    # 构建SQL查询语句
    sql = f'''
SELECT
    install_day,
    platform,
    media,
    country,
    group_name,
    max_r,
    pay_user_group_name,
    cost_change_ratio,
    revenue_1d,
    pu_1d,
    cost,
    actual_arppu, 
    predicted_arppu,
    is_weekend
FROM
    {table_name}
WHERE
    day BETWEEN '{install_day_start}' AND '{install_day_end}'
    '''
    
    print("执行的SQL语句如下：\n")
    print(sql)
    
    # 执行SQL查询并返回结果
    data = execSql(sql)

    return data

def preprocessData(data0, media=None, country=None):
    """
    预处理数据，包括日期转换、过滤、聚合、重塑和特征工程。
    """
    data = data0.copy()
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
    aggregated_data = aggregated_data.rename(columns={
        'install_day': 'ds', 
        'pay_user_group':'pay_user_group_name',
        'pu_change_ratio': 'y'
    })
    
    # 最终选择列
    df = aggregated_data[['ds', 'actual_cost_shifted', 'cost', 'cost_change_ratio', 'actual_pu_shifted', 'pu_1d', 'y', 'pay_user_group_name', 'actual_arppu', 'predicted_arppu', 'revenue_1d']]
    
    # 添加周末特征
    df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)

    return df


def loadModels(platform, media, country, group_name, pay_user_group_name, dayStr):
    global model_cache

    # 构建缓存键
    cache_key = (platform, media, country, group_name, pay_user_group_name, dayStr)

    # 检查缓存中是否已有模型
    if cache_key in model_cache:
        print(f"Loading model from cache for {cache_key}")
        return model_cache[cache_key]

    # 如果缓存中没有，则从数据库加载
    sql = f'''
        select
            model
        from
            lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_train2
        where
            day = '{dayStr}'
            and platform = '{platform}'
            and media = '{media}'
            and country = '{country}'
            and group_name = '{group_name}'
            and pay_user_group_name = '{pay_user_group_name}'
        '''
    print(sql)
    models_df = execSql(sql)
    if models_df.empty:
        print("No models found for the given conditions.")
        return None
    # 取出第一个模型
    row = models_df.iloc[0]
    model = model_from_json(row['model'])

    # 将模型存入缓存
    model_cache[cache_key] = model

    return model

def writeVerificationResultsToTable(df, dayStr):
    print('try to write verification results to table:')
    print(df.head(5))
    if len(df) == 0:
        print('No data to write.')
        return
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_test2'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeVerificationResultsToTable failed, o is not defined')
        print(dayStr)
        print(df)

def main():
    # dayStr 是验算日，yesterday是被验算日。每天验算的是前一天的数据。
    global dayStr

    currentDay = pd.to_datetime(dayStr, format='%Y%m%d')
    
    currentMonDay = currentDay - pd.Timedelta(days=currentDay.dayofweek)
    currentMonDayStr = currentMonDay.strftime('%Y%m%d')

    N = 1
    nDaysAgo = currentDay - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')
    
    historical_data = getHistoricalData(nDaysAgoStr, dayStr)

    # 初步分组，不分max_r，因为模型只涉及付费用户数，与付费金额无关
    groupData = historical_data.groupby(['platform','media','country','group_name','pay_user_group_name'])

    retDf = pd.DataFrame()

    for (platform, media, country, group_name, pay_user_group_name), group_data0 in groupData:
        print(f"platform: {platform}, media: {media}, country: {country}, group_name: {group_name}, pay_user_group_name: {pay_user_group_name}")
        if platform == 'ios' and media != 'ALL':
            print(f"Skip media: {media} for ios")
            continue
        maxRList = group_data0['max_r'].unique()

        predicted_pu = None

        for maxR in maxRList:
            print(f"max_r: {maxR}")
            group_dataCopy = group_data0[group_data0['max_r']==maxR].copy()
            group_dataCopy = group_dataCopy.sort_values('install_day', ascending=True)
            group_dataCopy['actual_cost_shifted'] = group_dataCopy['cost'].shift(1)
            group_dataCopy['actual_pu_shifted'] = group_dataCopy['pu_1d'].shift(1)
            group_dataCopy.rename(columns={
                'install_day':'ds',
                'pu_change_ratio':'y'
            }, inplace=True)
            group_dataCopy = group_dataCopy[group_dataCopy['ds'] == dayStr].reset_index(drop=True)

            if predicted_pu is None:
                model = loadModels(platform, media, country, group_name, pay_user_group_name, currentMonDayStr)
                if model is None:
                    print(f"No model found for pay_user_group_name: {pay_user_group_name}")
                    break
                forecast = model.predict(group_dataCopy)
                yhat = forecast['yhat'].reset_index(drop=True)
                predicted_pu = group_dataCopy['actual_pu_shifted'] * (1 + yhat)
                
            
            group_dataCopy['predicted_pu'] = predicted_pu
            group_dataCopy['predicted_revenue'] = group_dataCopy['predicted_pu'] * group_dataCopy['predicted_arppu']
            group_dataCopy.rename(columns={
                'ds': 'install_day',
                'pu_1d': 'actual_pu',
                'revenue_1d': 'actual_revenue'   
            }, inplace=True)
            
            retDf0 = group_dataCopy[['install_day','platform', 'media', 'country', 'group_name', 'pay_user_group_name', 'actual_pu', 'predicted_pu', 'actual_arppu', 'predicted_arppu', 'actual_revenue', 'predicted_revenue', 'max_r']]
            print('Verification results:')
            print(retDf0)
            retDf = retDf.append(retDf0)
            
        
    retDf['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
    writeVerificationResultsToTable(retDf, dayStr)

if __name__ == "__main__":
    global dayStr

    init()
    createTable()
    deletePartition(dayStr)

    main()
