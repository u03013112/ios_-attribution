import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

def init():
    global execSql
    global dayStr
    global model_cache

    model_cache = {}

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
        dayStr = '20241117'  # 本地测试时的日期，可自行修改

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='media', type='string', comment='media source'),
            Column(name='country', type='string', comment='country'),
            Column(name='type', type='string', comment='-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3,best。其中best是满足倒推1日ROI的最大花费。'),
            Column(name='cost', type='double', comment='cost'),
            Column(name='predicted_pu', type='double', comment='predicted pay users'),
            Column(name='predicted_arppu', type='double', comment='predicted ARPPU'),
            Column(name='predicted_revenue', type='double', comment='predicted revenue'),
            Column(name='predicted_roi', type='double', comment='predicted roi'),
            Column(name='max_r', type='double', comment='max_r'),
            Column(name='is_best', type='double', comment='is_best'),
            Column(name='nerf_ratio', type='double', comment='nerf ratio'),
        ]
        partitions = [
            Partition(name='day', type='string', comment='预测日期')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_report2'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_report2'
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
        table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_report2'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            # 将 install_day 转换为字符串
            # df['install_day'] = df['install_day'].dt.strftime('%Y%m%d')
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeToTable failed, o is not defined')
        print(dayStr)
        print(df)

def getMinWeekMape(currentMondayStr):
    print(f"获取最小MAPE：currentMondayStr={currentMondayStr}")
    sql = f'''
select
    app,
    platform,
    media,
    country,
    group_name,
    max_r,
    min_mape,
    day_mape
from lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_min_week_mape_report2
where day = '{currentMondayStr}'
;
    '''
    print(sql)
    data = execSql(sql)
    
    # 选择需要的列
    resultDf = data
    resultDf.rename(columns={'day_mape': 'dayMape'}, inplace=True)

    return resultDf

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

def getRoiThreshold(lastDayStr, platform, media, country):
    app = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'

    media_condition = f"and media = '{media}' and organic = 1" if media != 'ALL' else "and media = 'ALL'"
    country_condition = f"and country = '{country}'" if country != 'ALL' else "and country = 'ALL'"


    sql = f'''
    select
        roi_001_best
    from
        ads_predict_base_roi_day1_window_multkey
    where
        app = 502
        and type = '{app}'
        and end_date = '{lastDayStr}'
        {media_condition}
        {country_condition}
    ;
    '''
    roi_threshold_df = execSql(sql)
    if roi_threshold_df.empty:
        print("未找到 ROI 阈值。用保守值 2% 代替。")
        return 0.02
    return roi_threshold_df.iloc[0]['roi_001_best']

def find_max_cost_meeting_roi(predict_df, lastDayStr, platform):
    """
    在预测结果中为每个国家和媒体组合找到满足 ROI 阈值的最大预测花费金额。
    
    参数：
        predict_df (pd.DataFrame): 之前预测的结果数据框，需包含 'predicted_roi' 和 'cost' 等列。
        lastDayStr (str): 上一天的日期字符串，用于获取目标 ROI。
        platform (str): 平台名称，如 'android' 或 'ios'。
    
    返回：
        pd.DataFrame: 所有满足条件的单条预测结果，格式类似于 `predict_macro` 的输出。
        如果没有满足条件的结果，则返回空的 DataFrame。
    """
    # 确定需要分组的列，这里假设按 'country' 和 'media' 组合
    group_columns = ['country', 'media']
    unique_groups = predict_df[group_columns].drop_duplicates()

    print(f"需要处理的国家和媒体组合数: {len(unique_groups)}")

    selected_rows = []

    maxRList = predict_df['max_r'].unique()

    for _, group in unique_groups.iterrows():
        country = group['country']
        media = group['media']
        
        # 获取目标 ROI 阈值
        target_roi = getRoiThreshold(lastDayStr, platform, media, country)
        print(f"处理组合 - 国家: {country}, 媒体: {media}, 目标 ROI 阈值: {target_roi}")

        for maxR in maxRList:
            print(f"处理组合 - 国家: {country}, 媒体: {media}, max_r: {maxR}")
            # 筛选出当前组合的预测记录
            group_df = predict_df[
                (predict_df['country'] == country) & 
                (predict_df['media'] == media) & 
                (predict_df['platform'] == platform) &
                (predict_df['max_r'] == maxR)
            ]

            # 筛选出预测 ROI 大于等于目标 ROI 的记录
            filtered_df = group_df[group_df['predicted_roi'] >= target_roi]
            print(f"组合 - {country}, {media} 满足 ROI >= {target_roi} 的记录数: {len(filtered_df)}")

            if filtered_df.empty:
                print(f"组合 - {country}, {media} 没有满足 ROI 阈值的预测结果。")
                continue

            # 找到 'cost' 最大的记录
            max_cost_row = filtered_df.loc[filtered_df['cost'].idxmax()]
            print(f"组合 - {country}, {media} 选择的最大预测花费金额记录: cost = {max_cost_row['cost']}")

            # 将选择的记录添加到列表中
            selected_rows.append(max_cost_row)

    if not selected_rows:
        print("没有任何组合满足 ROI 条件。")
        return pd.DataFrame()

    # 将所有选择的记录合并为一个 DataFrame
    result_df = pd.DataFrame(selected_rows)
    
    return result_df

def main():
    global dayStr

    today = pd.to_datetime(dayStr, format='%Y%m%d')
    
    

    tomorrow = pd.to_datetime(dayStr, format='%Y%m%d') + pd.Timedelta(days=1)
    tomorrowStr = tomorrow.strftime('%Y%m%d')

    tomorrowIsWeekend = 1 if tomorrow.dayofweek in [5, 6] else 0

    print(f"预测{tomorrowStr}可能花费。")

    # 找到需要被预测日的周一
    currentMonday = pd.to_datetime(tomorrow, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(tomorrow, format='%Y%m%d').dayofweek)
    currentMondayStr = currentMonday.strftime('%Y%m%d')

    # 获得最佳方案
    minWeekMapeDf = getMinWeekMape(currentMondayStr)
    # print(f"最佳方案：")
    # print(minWeekMapeDf[(minWeekMapeDf['platform']=='android')&(minWeekMapeDf['media'] == 'ALL') & (minWeekMapeDf['country'] == 'ALL')])

    # 向前14天，共计15天
    N = 14
    nDaysAgo = today - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')
    
    historical_data = getHistoricalData(nDaysAgoStr, dayStr)

    # 只保留最佳方案对应的group_name

    df = historical_data.merge(minWeekMapeDf, on=['platform','media','country','group_name','max_r'], how='right')

    # 初步分组，不分max_r，因为模型只涉及付费用户数，与付费金额无关
    groupData = df.groupby(['platform','media','country','group_name','pay_user_group_name','max_r'])

    retDf = pd.DataFrame()
    for (platform, media, country, group_name, pay_user_group_name,max_r), group in groupData:
        # # for test
        # if (platform == 'android' and media == 'ALL' and country == 'ALL' and max_r == 200.0) == False:
        #     # print(f"Skip platform: {platform}, media: {media}, country: {country}")
        #     continue

        print(f"platform: {platform}, media: {media}, country: {country}, group_name: {group_name}, pay_user_group_name: {pay_user_group_name}, max_r: {max_r}")
        
        if platform == 'ios' and media != 'ALL':
            print(f"Skip media: {media} for ios")
            continue
        
        group = group.sort_values(by='install_day').reset_index(drop=True)
        # print(group)

        dayCost = group[group['install_day'] == dayStr]['cost'].sum()
        dayPu = group[group['install_day'] == dayStr]['pu_1d'].sum()
        predictArppu = group['actual_arppu'].mean()
        # print(f"dayCost: {dayCost}, dayPu: {dayPu}, predictArppu: {predictArppu}")

        for cost_change_ratio in [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3]:
            cost = dayCost * (1 + cost_change_ratio)

            inputDf = pd.DataFrame({
                'ds': [pd.to_datetime(dayStr, format='%Y%m%d')],
                'cost_change_ratio': [cost_change_ratio],
                'is_weekend': [tomorrowIsWeekend]    
            })

            model = loadModels(platform, media, country, group_name, pay_user_group_name, currentMondayStr)
            
            if model is None:
                print(f"未加载到模型: {platform}, {media}, {country}, {group_name}, {pay_user_group_name}, {currentMondayStr}")
                continue

            forecast = model.predict(inputDf)
            yhat = forecast['yhat'].values[0]
            predictedPu = dayPu * (1 + yhat)
            predictedRevenue = predictedPu * predictArppu

            ret = pd.DataFrame({
                'platform': [platform],
                'media': [media],
                'country': [country],
                'group_name': [group_name],
                'pay_user_group_name': [pay_user_group_name],
                'max_r': [max_r],
                'cost_change_ratio': [cost_change_ratio],
                'yesterday_cost': [dayCost],
                'cost': [cost],
                'yesterday_pu': [dayPu],
                'predicted_pu': [predictedPu],
                'predicted_arppu': [predictArppu],
                'predicted_revenue': [predictedRevenue]
            })

            retDf = pd.concat([retDf, ret], ignore_index=True)


    # 聚合预测结果
    allRet = retDf.groupby(['platform', 'country', 'media', 'cost_change_ratio','max_r']).agg({
        'yesterday_cost': 'mean',
        'cost': 'mean',
        'yesterday_pu': 'sum',
        'predicted_pu': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()
    allRet['predicted_arppu'] = allRet['predicted_revenue'] / allRet['predicted_pu']
    allRet['predicted_roi'] = allRet['predicted_revenue'] / allRet['cost']
    

    allRet['app'] = allRet['platform'].apply(lambda x: 'com.fun.lastwar.gp' if x == 'android' else 'id6448786147')

    # 设置 'install_day' 为 dayStr
    allRet['install_day'] = dayStr

    # 设置 'type' 为 cost_change_ratio 的字符串表示
    allRet['type'] = allRet['cost_change_ratio'].astype(str)

    # TODO: 暂时没算
    allRet['nerf_ratio'] = 0.0

    allRet['is_best'] = 0
        
    groupDf = allRet.groupby(['platform','media','country'])
    for (platform, media, country), group in groupDf:
        target_roi = getRoiThreshold(dayStr, platform, media, country)

        # # for test
        # target_roi = target_roi * 0.5

        maxRList = group['max_r'].unique()
        for maxR in maxRList:
            print(f"处理组合 - 国家: {country}, 媒体: {media}, max_r: {maxR}")
            groupMaxR = group[group['max_r'] == maxR]
            filtered_df = groupMaxR[groupMaxR['predicted_roi'] >= target_roi]
            print(f"满足 ROI >= {target_roi} 的记录数: {len(filtered_df)}")

            if filtered_df.empty:
                print(f"组合 - {country}, {media} 没有满足 ROI 阈值的预测结果。")
                continue

            # 找到 'cost' 最大的记录
            max_cost_row = filtered_df.loc[filtered_df['cost'].idxmax()]
            print(f"选择的最大预测花费金额记录: cost = {max_cost_row['cost']}")

            # 在allRet中找到对应的记录，并在is_best列中标记为1
            allRet.loc[(allRet['platform'] == platform) &
                                    (allRet['media'] == media) &
                                    (allRet['country'] == country) &
                                    (allRet['max_r'] == maxR) &
                                    (allRet['cost'] == max_cost_row['cost']), 'is_best'] = 1
    
    # TODO:列筛选
    # 选择与表结构匹配的列
    predictions_to_write = allRet[['app', 'media', 'country', 'type','cost', 'max_r',
                                            'predicted_pu', 'predicted_arppu', 'predicted_revenue', 'predicted_roi','is_best','nerf_ratio']]
    
    # 写入预测结果到表中
    deletePartition(tomorrowStr)
    writeToTable(predictions_to_write, tomorrowStr)
    print(f"平台 {platform} 的预测结果已写入数据库。")

    return

if __name__ == '__main__':
    init()
    createTable()
    # 删表放到main中，写入之前
    
    main()
