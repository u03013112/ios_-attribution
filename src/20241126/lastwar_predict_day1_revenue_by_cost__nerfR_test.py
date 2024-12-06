# 改为逐天预测，每天的预测结果都会写入表格
import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json
from tensorflow.keras.models import Sequential, model_from_json as tf_model_from_json
from sklearn.preprocessing import StandardScaler

import json
import base64

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
        dayStr = '20240902'  # 本地测试时的日期，可自行修改

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
            Column(name='max_r', type='double', comment='maximum revenue'),
            Column(name='actual_revenue', type='double', comment='actual revenue'),
            Column(name='predicted_revenue', type='double', comment='predicted revenue')
        ]
        partitions = [
            Partition(name='day', type='string', comment='prediction date, like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_test'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_test'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')


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
            model_weights_base64,
            scaler_params
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
        return None,None,None
    # 取出第一个模型
    row = models_df.iloc[0]
    prophetModel = model_from_json(row['prophet_model'])

    dnnModel = tf_model_from_json(row['dnn_model'])
    dummy_weights = dnnModel.get_weights()
    model_weights_shapes = [w.shape for w in dummy_weights]

    model_weights_base64 = json.loads(row['model_weights_base64'])
    model_weights_binary = [base64.b64decode(w) for w in model_weights_base64]
    model_weights = [np.frombuffer(w, dtype=np.float32).reshape(shape) for w, shape in zip(model_weights_binary, model_weights_shapes)]

    dnnModel.set_weights(model_weights)
    dnnModel.compile(optimizer='RMSprop', loss='mean_squared_error')

    # 解析 scaler_params 并创建 StandardScaler 对象
    scaler_params = json.loads(row['scaler_params'])
    scaler_X = StandardScaler()
    scaler_X.mean_ = np.array(scaler_params['mean'])
    scaler_X.scale_ = np.array(scaler_params['scale'])
    scaler_X.var_ = scaler_X.scale_ ** 2  # 计算方差

    return prophetModel, dnnModel, scaler_X

def writeVerificationResultsToTable(df, dayStr):
    print('try to write verification results to table:')
    print(df.head(5))
    if len(df) == 0:
        print('No data to write.')
        return
    if 'o' in globals():
        table_name = 'lastwar_predict_day1_revenue_by_cost__nerf_r_test'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeVerificationResultsToTable failed, o is not defined')
        print(dayStr)
        print(df)

def main():
    global dayStr

    currentDay = pd.to_datetime(dayStr, format='%Y%m%d')
    
    currentMonDay = currentDay - pd.Timedelta(days=currentDay.dayofweek)
    currentMonDayStr = currentMonDay.strftime('%Y%m%d')

    N = 1
    nDaysAgo = currentDay - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')
    
    historical_data = getHistoricalData(nDaysAgoStr, dayStr)
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    groupData = historical_data.groupby(['platform','media','country','max_r'])

    retDf = pd.DataFrame()

    for (platform, media, country, max_r), group_data0 in groupData:
        # for test
        if platform != 'android' or media != 'ALL' or country != 'ALL' or max_r != 1e10:
            print('For test !!!')
            print(f"Skip platform: {platform}, media: {media}, country: {country}, max_r: {max_r}")
            continue

        print(f"platform: {platform}, media: {media}, country: {country}, max_r: {max_r}")
        if platform == 'ios' and media != 'ALL':
            print(f"Skip media: {media} for ios")
            continue
        
        group_dataCopy = group_data0.copy()
        group_dataCopy = group_dataCopy.sort_values('install_day', ascending=True)
        group_dataCopy.rename(columns={
            'install_day':'ds',
            'revenue_1d':'y'
        }, inplace=True)
        group_dataCopy = group_dataCopy[group_dataCopy['ds'] == dayStr].reset_index(drop=True)

        print('进入预测的数据：')
        print(group_dataCopy)

        # 过滤掉无效数据
        checkDf = group_dataCopy[['ds','cost']]
        # 去掉输入列中NaN和inf
        checkDf = checkDf.replace([np.inf, -np.inf], np.nan)
        checkDf = checkDf.dropna()
        if checkDf.empty:
            print('No valid data for prediction.')
            continue

        
        prophetModel, dnnModel, scaler_X = loadModels(platform, media, country, max_r, currentMonDayStr)
        if prophetModel is None or dnnModel is None:
            print('No models found for the given conditions.')
            continue
        forecast = prophetModel.predict(group_dataCopy)
        print('Prophet预测结果：')
        print(forecast[['ds', 'weekly']])
        group_dataCopy = group_dataCopy.merge(forecast[['ds', 'weekly']], on='ds', how='left')

        # 准备DNN模型的输入数据
        dnn_input = group_dataCopy[['cost', 'weekly']].values
        dnn_input_scaled = scaler_X.transform(dnn_input)


        print('DNN模型的输入数据：')
        print(dnn_input)
        print('DNN模型的标准化输入数据：')
        print(dnn_input_scaled)

        # 使用DNN模型进行预测
        predicted_revenue = dnnModel.predict(dnn_input_scaled)

        # 将预测结果添加到数据框中
        group_dataCopy['predicted_revenue'] = predicted_revenue
        group_dataCopy.rename(columns={
            'ds': 'install_day',
            'y': 'actual_revenue'
        }, inplace=True)

        # 准备结果数据框
        group_dataCopy['app'] = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        retDf0 = group_dataCopy[['install_day', 'app', 'platform', 'media', 'country', 'max_r', 'actual_revenue', 'predicted_revenue']]
        
        print('Verification results:')
        print(retDf0)

        retDf = retDf.append(retDf0)

    writeVerificationResultsToTable(retDf, dayStr)

if __name__ == "__main__":
    global dayStr

    init()
    createTable()
    deletePartition(dayStr)

    main()
