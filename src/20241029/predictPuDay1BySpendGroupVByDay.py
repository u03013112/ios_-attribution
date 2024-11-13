# 改为逐天预测，每天的预测结果都会写入表格
import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

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
        dayStr = '20241021'  # 本地测试时的日期，可自行修改


def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='media', type='string', comment='media source'),
            Column(name='country', type='string', comment='country'),
            Column(name='install_day', type='string', comment='install day'),
            Column(name='group_name', type='string', comment='group name'),
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
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct_verification_by_day'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct_verification_by_day'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')


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

def loadModels(app, media, country,group_name,pay_user_group_name,dayStr):
    sql = f'''
        select
            model
        from
            lastwar_predict_day1_pu_pct_by_cost_pct
        where
            day = '{dayStr}'
            and app = '{app}'
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
    return model

def makePredictions(preprocessed_data, model, app, media, country, group_name, pay_user_group_name):
    # 准备用于预测的特征
    model_df = preprocessed_data.copy()

    # 使用模型预测付费用户变化率
    forecast = model.predict(model_df)
    
    # 保证ds的数据类型是datetime64[ns]
    model_df['ds'] = pd.to_datetime(model_df['ds'])
    forecast['ds'] = pd.to_datetime(forecast['ds'])

    model_df = model_df.merge(forecast[['ds', 'yhat']], on='ds', how='left')
    
    # 重命名预测列
    model_df = model_df.rename(columns={
        'yhat': 'pu_change_ratio_predicted',
        'pu_1d': 'actual_pu',
        'revenue_1d': 'actual_revenue'
    })

    # 计算预测的付费用户数
    model_df['predicted_pu'] = model_df['actual_pu_shifted'] * (1 + model_df['pu_change_ratio_predicted'])
    
    # 预测收入 = 预测付费用户数 * 预测ARPPU
    model_df['predicted_revenue'] = model_df['predicted_pu'] * model_df['predicted_arppu']
    
    # 添加其他必要信息
    model_df['app'] = app
    model_df['media'] = media
    model_df['country'] = country
    model_df['group_name'] = group_name
    model_df['pay_user_group_name'] = pay_user_group_name
    
    # 选择并重命名最终需要的列
    final_df = model_df[['app' ,'ds', 'media', 'country', 'group_name' , 'pay_user_group_name', 'actual_pu', 'predicted_pu', 'actual_arppu', 'predicted_arppu', 'actual_revenue', 'predicted_revenue']]
    
    return final_df

def writeVerificationResultsToTable(df, dayStr):
    print('try to write verification results to table:')
    print(df.head(5))
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct_verification_by_day'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            # 将 install_day 转换为字符串
            df['install_day'] = df['install_day'].dt.strftime('%Y%m%d')
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeVerificationResultsToTable failed, o is not defined')
        print(dayStr)
        print(df)

def main(configurations,group_by_media=False, group_by_country=False):
    # dayStr 是验算日，yesterday是被验算日。每天验算的是前一天的数据。
    global dayStr

    groupName = configurations['group_name']
    payUserGroupList = configurations['payUserGroupList']

    currentDay = pd.to_datetime(dayStr, format='%Y%m%d')
    
    currentMonDay = currentDay - pd.Timedelta(days=currentDay.dayofweek)
    currentMonDayStr = currentMonDay.strftime('%Y%m%d')

    # 往前多取一些数据，是为了估算ARPPU
    N = 20
    nDaysAgo = currentDay - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    # mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    # countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US'] if group_by_country else [None]
    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    countryList = ['JP', 'KR', 'US', 'T1'] if group_by_country else [None]

    for platform in platformList:
        app = appDict[platform]
        print(f"\nProcessing platform: {platform}, app: {app}")
        historical_data = getHistoricalData(nDaysAgoStr, dayStr, platform, groupName)
        print(f"Platform: {platform}, App: {app}, Data Length: {len(historical_data)}")
        print(historical_data.head())
        
        for media in mediaList:
            for country in countryList:
                if platform == 'ios' and media:
                    print(f"Skip media: {media} for iOS")
                    continue

                print('\n\n')
                print(f"platform: {platform}, app: {app}, media: {media}, country: {country}")
                # 数据预处理
                
                df = preprocessData(historical_data, media, country)
                
                yesterdayDf = df[df['ds'] == currentDay]

                # 遍历每个 pay_user_group_name
                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']
                    print(f"\n正在处理 pay_user_group_name: {payUserGroupName}")

                    # 过滤当前组的数据
                    test_subset = yesterdayDf[yesterdayDf['pay_user_group_name'] == payUserGroupName].copy()

                    if len(test_subset) == 0:
                        print(f"No data for pay_user_group_name: {payUserGroupName}")
                        continue

                    # test_subset 按照ds升序排序
                    test_subset = test_subset.sort_values('ds')
                    print(f"过滤后准备预测数据: 长度 {len(test_subset)}")
                    print(test_subset.head())

                    # test_subset = test_subset[['ds', 'cost_change_ratio','is_weekend']]

                    # if test_subset['actual_pu_shifted'] == 0 or test_subset['y'] > 1e10:
                    #     print(f"actual_pu_shifted = 0 or y > 1e10, skip")
                    #     continue
                    # 过滤掉 actual_pu_shifted == 0 或 y > 1e10 的行
                    test_subset = test_subset[~((test_subset['actual_pu_shifted'] == 0) | (test_subset['y'] > 1e10))]


                    # if test_subset['actual_cost_shifted'] == 0 or test_subset['cost'] == 0 or test_subset['cost_change_ratio'] > 1e10:
                    #     print(f"actual_cost_shifted = 0 or cost = 0, skip")
                    #     continue
                    test_subset = test_subset[~((test_subset['actual_cost_shifted'] == 0) | (test_subset['cost'] == 0) | (test_subset['cost_change_ratio'] > 1e10))]

                    if len(test_subset) == 0:
                        print('过滤不正常数据后，没有数据了')
                        continue

                    mediaMap = {
                        'Facebook Ads': 'FACEBOOK',
                        'applovin_int': 'APPLOVIN',
                        'googleadwords_int': 'GOOGLE'
                    }
                    mediaMaped = mediaMap[media] if media in mediaMap else media
                    

                    # 加载模型
                    model = loadModels(app, mediaMaped if mediaMaped else 'ALL', country if country else 'ALL', groupName, payUserGroupName, currentMonDayStr)
                    if model is None:
                        print(f"No models found for app: {app}, media: {mediaMaped}, country: {country}")
                        continue
                    
                    # 进行预测
                    predictions_df = makePredictions(test_subset, model, app, mediaMaped if mediaMaped else 'ALL', country if country else 'ALL', groupName, payUserGroupName)
                
                    if predictions_df is not None:
                        # 写入DB
                        predictions_df.rename(columns={'ds': 'install_day'}, inplace=True)
                        writeVerificationResultsToTable(predictions_df, dayStr)
                    else:
                        print(f"No predictions for pay_user_group_name: {payUserGroupName}")


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


if __name__ == "__main__":
    global dayStr

    init()
    createTable()
    deletePartition(dayStr)

    # 找到上周的周一和周日
    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    currentMondayStr = currentMonday.strftime('%Y%m%d') 

    configurations = getConfigurations('android', currentMondayStr)
    
    # 依次调用 main 函数
    for configuration in configurations:
        main(configuration, False, False)
        main(configuration, True, False)
        main(configuration, False, True)
        main(configuration, True, True)
