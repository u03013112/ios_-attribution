import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
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
        dayStr = '20240617'

    print('dayStr:', dayStr)

def getHistoricalData(install_day_start, install_day_end, platform='android', group_name=None):
    table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data'
    
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
    pay_user_group,
    max_r
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


def train_model(train_df):
    """
    训练 Prophet 模型。
    """
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('cost_change_ratio')
    # model.add_regressor('is_weekend')

    train_df2 = train_df[['ds', 'y', 'cost_change_ratio','is_weekend']]
    # 去掉输入列中NaN和inf
    train_df2 = train_df2.replace([np.inf, -np.inf], np.nan).dropna()

    if len(train_df2) < 30:
        print("训练数据不足（少于30条），跳过训练。")
        return None

    model.fit(train_df2)
    
    # 打印模型训练日志
    print("Model Training Completed")

    return model


def createTable():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='model', type='string', comment=''),
            Column(name='group_name', type='string', comment='g3__2_10'),
            Column(name='pay_user_group_name', type='string', comment='like:0~2,2~10 or 10~inf'),
            Column(name='max_r', type='double', comment='maximum revenue')
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_train', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_train')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_train')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)

def main(configurations,group_by_media=False, group_by_country=False):
    global dayStr

    groupName = configurations['group_name']
    maxR = configurations['max_r']
    payUserGroupList = configurations['payUserGroupList']

    # 找到本周的周一
    monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    mondayStr = monday.strftime('%Y%m%d')

    print(f"本周一： {mondayStr}")

    # 向前推8周
    # start_date = monday - pd.Timedelta(weeks=8)
    start_date = monday - pd.Timedelta(days=60)
    startDateStr = start_date.strftime('%Y%m%d')

    lastSunday = monday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    print(f'向前推60天：{startDateStr}~{lastSundayStr}')

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    # mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    # countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US'] if group_by_country else [None]
    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    countryList = ['JP', 'KR', 'US', 'T1'] if group_by_country else [None]

    modelDf = pd.DataFrame(columns=['app', 'media', 'country', 'model', 'group_name','pay_user_group_name'])

    for platform in platformList:
        app = appDict[platform]
        # 获取当前平台的历史数据
        historical_data = getHistoricalData(startDateStr, lastSundayStr, platform, groupName)
        # 过滤 max_r
        historical_data = historical_data[historical_data['max_r'] == maxR]
        print(f"Platform: {platform}, App: {app}, Data Length: {len(historical_data)}")
        print(historical_data.head())
        # 按照分组进行遍历
        for media in mediaList:
            for country in countryList:
                if platform == 'ios' and media :
                    print(f"ios平台不支持media过滤，跳过 media: {media}")
                    continue

                print('\n\n')
                print(f"platform: {platform}, app: {app}, media: {media}, country: {country}")
                # 数据预处理
                df = preprocessData(historical_data, media, country)
                print(f"Data Length After Preprocessing: {len(df)}")
                print(df.head())

                # 遍历每个 pay_user_group_name
                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']
                    print(f"\n正在处理 pay_user_group_name: {payUserGroupName}")

                    # 过滤当前组的数据
                    train_subset = df[df['pay_user_group_name'] == payUserGroupName].copy()
                
                    # train_subset 按照ds升序排序
                    train_subset = train_subset.sort_values('ds')
                    print(f"Data Length After Filtering: {len(train_subset)}")
                    print(train_subset.head())

                    # 训练模型
                    model = train_model(train_subset)

                    # 保存模型
                    if model is not None:
                        model_json = model_to_json(model)
                        print(model_json)
                        media_mapped = media if media else 'ALL'
                        country_mapped = country if country else 'ALL'


                        # 对 media 进行重命名
                        media_mapping = {
                            'Facebook Ads': 'FACEBOOK',
                            'applovin_int': 'APPLOVIN',
                            'googleadwords_int': 'GOOGLE',
                            'ALL': 'ALL'
                        }
                        media_mapped = media_mapping.get(media_mapped, media_mapped)

                        modelDf = modelDf.append({
                            'app': app, 
                            'media': media_mapped, 
                            'country': country_mapped, 
                            'model': model_json,
                            'group_name': groupName,
                            'pay_user_group_name': payUserGroupName,
                            'max_r': maxR
                        }, ignore_index=True)
                    else:
                        print(f"Skipping model for platform: {platform}, media: {media}, country: {country} due to insufficient data.")

    # 写入表格前打印 modelDf
    print("\nFinal modelDf before writing to table:")
    print(modelDf.head())

    # 写入表格
    writeTable(modelDf, mondayStr)

# 获取配置
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
        max_value,
        max_r
    FROM
        lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_configurations
    WHERE
        app = '{app_package}'
        AND day = '{dayStr}'
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


if __name__ == "__main__":
    import logging

    logging.getLogger("prophet").setLevel(logging.WARNING)
    logging.getLogger("cmdstanpy").disabled=True

    init()
    createTable()
    
    # 获取当前日期
    currentDate = datetime.strptime(dayStr, '%Y%m%d')
    # 找到本周一
    currentMonday = currentDate - timedelta(days=currentDate.weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')
    
    # 删除指定分区
    deletePartition(currentMondayStr)

    configurations = getConfigurations('android', currentMondayStr)
    
    # 依次调用 main 函数
    for configuration in configurations:
        print(configuration)

        main(configuration, False, False)
        
        exit()
        main(configuration, True, False)
        main(configuration, False, True)
        main(configuration, True, True)
