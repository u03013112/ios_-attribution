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
        dayStr = '20240817'

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='platform', type='string', comment='app identifier'),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='group_name', type='string', comment='g3__2_10'),
            Column(name='max_r', type='double', comment='max_r'),
            Column(name='pay_user_group_name', type='string', comment='like:0~2,2~10 or 10~inf'),
            Column(name='predicted_arppu', type='double', comment='predicted arppu'),
            Column(name='predicted_arppu_before_nerf', type='double', comment='predicted arppu before nerf'),
            Column(name='last_pu', type='double', comment='last pu'),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_predict_arppu_and_last_pu', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_predict_arppu_and_last_pu')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_predict_arppu_and_last_pu')
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

def getPredictArppuAndLastPu(dayStr,configurations):
    print(f"获取预测ARPPU和最后一天的PU：dayStr={dayStr}")

    def getHistoricalData(install_day_start, install_day_end, platform='android', configuration=None):
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_historical_data'
        
        groupName = configuration['group_name']
        maxR = configuration['max_r']

        # 构建SQL查询语句
        sql = f'''
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
    {table_name}
WHERE
    day BETWEEN '{install_day_start}' AND '{install_day_end}'
    AND platform = '{platform}'
    AND group_name = '{groupName}'
    AND max_r = {maxR}
;
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
            'revenue_1d_before_nerf': 'sum',
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

        aggregated_data['actual_arppu_before_nerf'] = aggregated_data['revenue_1d_before_nerf'] / aggregated_data['pu_1d']
        aggregated_data['actual_arppu_before_nerf'].replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # 计算预测 ARPPU：先shift一天，再计算过去15天的均值
        aggregated_data['actual_arppu_shifted'] = aggregated_data.groupby('pay_user_group')['actual_arppu'].shift(1)
        aggregated_data['predicted_arppu'] = aggregated_data.groupby('pay_user_group')['actual_arppu_shifted'].rolling(window=15, min_periods=1).mean().reset_index(level=0, drop=True)
        
        aggregated_data['actual_arppu_before_nerf_shifted'] = aggregated_data.groupby('pay_user_group')['actual_arppu_before_nerf'].shift(1)
        aggregated_data['predicted_arppu_before_nerf'] = aggregated_data.groupby('pay_user_group')['actual_arppu_before_nerf_shifted'].rolling(window=15, min_periods=1).mean().reset_index(level=0, drop=True)
        
        # if media is None and country is None:
        #     print(aggregated_data)

        # 6. 重命名和选择最终列
        aggregated_data = aggregated_data.rename(columns={
            'install_day': 'ds', 
            'pay_user_group':'pay_user_group_name',
            'pu_change_ratio': 'y'
        })
        
        # 最终选择列
        df = aggregated_data[['ds', 'actual_cost_shifted', 'cost', 'cost_change_ratio', 'actual_pu_shifted', 'pu_1d', 'y', 'pay_user_group_name', 'actual_arppu', 'predicted_arppu', 'predicted_arppu_before_nerf', 'revenue_1d']].copy()
        
        # 添加周末特征
        df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)

        return df

    # 获取从dayStr往前推N天的数据，计算平均ARPPU作为预测的ARPPU 
    N = 16

    endDate = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=1)
    endDateStr = endDate.strftime('%Y%m%d')

    startDate = endDate - pd.Timedelta(days=N)
    startDateStr = startDate.strftime('%Y%m%d')    

    retDf = pd.DataFrame()

    for platform in ['android', 'ios']:
        for configuration in configurations:
            groupName = configuration['group_name']
            payUserGroupList = configuration['payUserGroupList']

            historical_data = getHistoricalData(startDateStr, endDateStr, platform, configuration)
            historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

            countryList = historical_data['country'].unique()
            mediaList = historical_data['mediasource'].unique()

            # 大盘
            allDf = preprocessData(historical_data)
            for payUserGroup in payUserGroupList:
                payUserGroupName = payUserGroup['name']
                filtered_df = allDf[(allDf['pay_user_group_name'] == payUserGroupName) & (allDf['ds'] == endDate)]
                if not filtered_df.empty:
                    arppu = filtered_df['predicted_arppu'].iloc[0]
                    arppuBeforeNerf = filtered_df['predicted_arppu_before_nerf'].iloc[0]
                    lastPu = filtered_df['pu_1d'].iloc[0]
                else:
                    arppu = 0
                    arppuBeforeNerf = 0
                    lastPu = 0
                    
                allRetDf = pd.DataFrame({
                    'platform': [platform],
                    'country': ['ALL'],
                    'media': ['ALL'],
                    'group_name': [groupName],
                    'max_r': [configuration['max_r']],
                    'pay_user_group_name': [payUserGroupName],
                    'predicted_arppu': [arppu],
                    'predicted_arppu_before_nerf': [arppuBeforeNerf],
                    'last_pu': [lastPu]
                })
                retDf = pd.concat([retDf, allRetDf])
            
            # 分国家
            for country in countryList:
                countryDf = preprocessData(historical_data, country=country)
                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']
                    filtered_df = countryDf[(countryDf['pay_user_group_name'] == payUserGroupName) & (countryDf['ds'] == endDate)]
                    if not filtered_df.empty:
                        arppu = filtered_df['predicted_arppu'].iloc[0]
                        arppuBeforeNerf = filtered_df['predicted_arppu_before_nerf'].iloc[0]
                        lastPu = filtered_df['pu_1d'].iloc[0]
                    else:
                        arppu = 0
                        arppuBeforeNerf = 0
                        lastPu = 0
                    countryRetDf = pd.DataFrame({
                        'platform': [platform],
                        'country': [country],
                        'media': ['ALL'],
                        'group_name': [groupName],
                        'max_r': [configuration['max_r']],
                        'pay_user_group_name': [payUserGroupName],
                        'predicted_arppu': [arppu],
                        'predicted_arppu_before_nerf': [arppuBeforeNerf],
                        'last_pu': [lastPu]
                    })
                    retDf = pd.concat([retDf, countryRetDf])

            # 分媒体 和 分国家+分媒体 只有安卓有
            if platform == 'android':
                # 分媒体
                for media in mediaList:
                    mediaDf = preprocessData(historical_data, media=media)
                    for payUserGroup in payUserGroupList:
                        payUserGroupName = payUserGroup['name']
                        filtered_df = mediaDf[(mediaDf['pay_user_group_name'] == payUserGroupName) & (mediaDf['ds'] == endDate)]
                        if not filtered_df.empty:
                            arppu = filtered_df['predicted_arppu'].iloc[0]
                            arppuBeforeNerf = filtered_df['predicted_arppu_before_nerf'].iloc[0]
                            lastPu = filtered_df['pu_1d'].iloc[0]
                        else:
                            arppu = 0
                            arppuBeforeNerf = 0
                            lastPu = 0

                        mediaRetDf = pd.DataFrame({
                            'platform': [platform],
                            'country': ['ALL'],
                            'media': [media],
                            'group_name': [groupName],
                            'max_r': [configuration['max_r']],
                            'pay_user_group_name': [payUserGroupName],
                            'predicted_arppu': [arppu],
                            'predicted_arppu_before_nerf': [arppuBeforeNerf],
                            'last_pu': [lastPu]
                        })
                        retDf = pd.concat([retDf, mediaRetDf])

                # 分国家+分媒体
                for country in countryList:
                    for media in mediaList:
                        countryMediaDf = preprocessData(historical_data, media=media, country=country)
                        for payUserGroup in payUserGroupList:
                            payUserGroupName = payUserGroup['name']
                            filtered_df = countryMediaDf[(countryMediaDf['pay_user_group_name'] == payUserGroupName) & (countryMediaDf['ds'] == endDate)]
                            if not filtered_df.empty:
                                arppu = filtered_df['predicted_arppu'].iloc[0]
                                arppuBeforeNerf = filtered_df['predicted_arppu_before_nerf'].iloc[0]
                                lastPu = filtered_df['pu_1d'].iloc[0]
                            else:
                                arppu = 0
                                arppuBeforeNerf = 0
                                lastPu = 0
                            countryMediaRetDf = pd.DataFrame({
                                'platform': [platform],
                                'country': [country],
                                'media': [media],
                                'group_name': [groupName],
                                'max_r': [configuration['max_r']],
                                'pay_user_group_name': [payUserGroupName],
                                'predicted_arppu': [arppu],
                                'predicted_arppu_before_nerf': [arppuBeforeNerf],
                                'last_pu': [lastPu]
                            })
                            retDf = pd.concat([retDf, countryMediaRetDf])

    
    # 对 media 进行重命名
    media_mapping = {
        'Facebook Ads': 'FACEBOOK',
        'applovin_int': 'APPLOVIN',
        'googleadwords_int': 'GOOGLE',
        'ALL': 'ALL'
    }
    retDf['media'] = retDf['media'].map(media_mapping)
    # 将 media 为空的行的 media drop 掉
    retDf = retDf[retDf['media'].notnull()]

    return retDf



def main(dayStr):
    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    currentMondayStr = currentMonday.strftime('%Y%m%d')

    configurations = getConfigurations('android', currentMondayStr, forTest=False)
    print(f"配置列表: {configurations}")

    predictArppuAndLastPu = getPredictArppuAndLastPu(dayStr, configurations)

    writeTable(predictArppuAndLastPu, dayStr)

if __name__ == '__main__':
    init()
    createTable()
    deletePartition(dayStr)

    main(dayStr)
