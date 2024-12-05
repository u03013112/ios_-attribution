import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

import json
import base64

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
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_report'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_report'
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
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_report'
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

def getHistoricalData(install_day_start, install_day_end):
    # 构建SQL查询语句
    sql = f'''
SELECT
    install_day,
    platform,
    media,
    country,
    max_r,
    cost,
    revenue_1d
FROM
    lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2
WHERE
    day BETWEEN '{install_day_start}' AND '{install_day_end}'
    and group_name = 'g1__all'
;
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    
    # 执行SQL查询并返回结果
    data = execSql(sql)
    
    return data

def loadModels(platform, media, country, max_r, dayStr):
    global model_cache

    # 如果缓存中没有，则从数据库加载
    sql = f'''
        select
            prophet_model,
            dnn_model,
            model_weights_base64
        from
            lastwar_predict_day1_revenue_by_cost__nerf_r_train
        where
            day = '{dayStr}'
            and platform = '{platform}'
            and media = '{media}'
            and country = '{country}'
            and max_r = {max_r}
        '''
    print(sql)
    models_df = execSql(sql)
    if models_df.empty:
        print("No models found for the given conditions.")
        return None
    # 取出第一个模型
    row = models_df.iloc[0]
    prophetModel = model_from_json(row['prophet_model'])

    dnnModel = json.loads(row['dnn_model'])
    dummy_weights = dnnModel.get_weights()
    model_weights_shapes = [w.shape for w in dummy_weights]

    model_weights_base64 = json.loads(row['model_weights_base64'])
    model_weights_binary = [base64.b64decode(w) for w in model_weights_base64]
    model_weights = [np.frombuffer(w, dtype=np.float32).reshape(shape) for w, shape in zip(model_weights_binary, model_weights_shapes)]

    dnnModel.set_weights(model_weights)
    dnnModel.compile(optimizer='RMSprop', loss='mean_squared_error')

    return prophetModel, dnnModel

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

    # 只获得today的就可以
    N = 0
    nDaysAgo = today - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')
    
    historical_data = getHistoricalData(nDaysAgoStr, dayStr)

    df = historical_data

    # 初步分组，不分max_r，因为模型只涉及付费用户数，与付费金额无关
    groupData = df.groupby(['platform','media','country','max_r'])

    retDf = pd.DataFrame()
    for (platform, media, country,max_r), group in groupData:
        print(f"platform: {platform}, media: {media}, country: {country}, max_r: {max_r}")
        
        if platform == 'ios' and media != 'ALL':
            print(f"Skip media: {media} for ios")
            continue
        
        group = group.sort_values(by='install_day').reset_index(drop=True)
        dayCost = group[group['install_day'] == dayStr]['cost'].sum()

        for cost_change_ratio in [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3]:
            cost = dayCost * (1 + cost_change_ratio)

            inputDf = pd.DataFrame({
                'ds': [pd.to_datetime(dayStr, format='%Y%m%d')],
                'cost': [cost],
            })

            prophetModel, dnnModel = loadModels(platform, media, country, max_r, currentMondayStr)
            
            if prophetModel is None or dnnModel is None:
                print(f"未加载到模型: {platform}, {media}, {country}, {max_r}, {currentMondayStr}")
                continue

            forecast = prophetModel.predict(inputDf)
            inputDf = inputDf.merge(forecast[['ds', 'weekly']], on='ds', how='left')

            # 准备DNN模型的输入数据
            dnn_input = inputDf[['cost', 'weekly']].values

            # 使用DNN模型进行预测
            predictedRevenue = dnnModel.predict(dnn_input)

            ret = pd.DataFrame({
                'platform': [platform],
                'media': [media],
                'country': [country],
                'max_r': [max_r],
                'cost_change_ratio': [cost_change_ratio],
                'yesterday_cost': [dayCost],
                'cost': [cost],
                'predicted_revenue': [predictedRevenue]
            })

            retDf = pd.concat([retDf, ret], ignore_index=True)


    # 聚合预测结果
    allRet = retDf.groupby(['platform', 'country', 'media', 'cost_change_ratio','max_r']).agg({
        'yesterday_cost': 'mean',
        'cost': 'mean',
        'predicted_revenue': 'sum'
    }).reset_index()
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
    predictions_to_write = allRet[['app', 'media', 'country', 'type','cost', 'max_r','predicted_revenue', 'predicted_roi','is_best','nerf_ratio']]
    
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
