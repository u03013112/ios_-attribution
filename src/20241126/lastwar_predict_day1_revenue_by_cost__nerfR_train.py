import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
from datetime import datetime, timedelta
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import StandardScaler


import json
import base64

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
        dayStr = '20240902'

    print('dayStr:', dayStr)

def createTable():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='platform', type='string', comment=''),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='max_r', type='double', comment=''),
            Column(name='prophet_model', type='string', comment=''),
            Column(name='dnn_model', type='string', comment=''),
            Column(name='model_weights_base64', type='string', comment=''),
            Column(name='scaler_params', type='string', comment='')
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_revenue_by_cost__nerf_r_train', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_revenue_by_cost__nerf_r_train')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if len(df) == 0:
        print('No data to write.')
        return
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_revenue_by_cost__nerf_r_train')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)
        df.to_csv('/src/data/lastwar_predict_day1_revenue_by_cost__nerf_r_train.csv', index=False)

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

def train_model(train_df):
    """
    训练 Prophet 模型和 DNN 模型。
    """
    # 创建和训练Prophet模型
    prophet_model = Prophet(weekly_seasonality=True)
    prophet_model.add_regressor('cost')

    train_df2 = train_df[['ds', 'y', 'cost']]
    # 去掉输入列中NaN和inf
    train_df2 = train_df2.replace([np.inf, -np.inf], np.nan).dropna()

    if len(train_df2) < 30:
        print("训练数据不足（少于30条），跳过训练。")
        return None, None, None

    prophet_model.fit(train_df2)
    
    # 打印模型训练日志
    print("Prophet Model Training Completed")

    # 提取训练数据的季节性成分
    train_forecast = prophet_model.predict(train_df2)
    train_df2 = train_df2.merge(train_forecast[['ds', 'weekly']], on='ds')

    # print('train_df2:')
    # print(train_df2[['ds', 'y', 'cost', 'weekly']])
    # 标准化
    scaler_X = StandardScaler()
    train_df2[['cost', 'weekly']] = scaler_X.fit_transform(train_df2[['cost', 'weekly']])

    # 准备DNN的训练数据
    X_train = train_df2[['cost', 'weekly']]
    y_train = train_df2['y']
    # print('X_train:')
    # print(X_train)

    # print('y_train:')
    # print(y_train)

    # 构建DNN模型
    dnn_model = Sequential()
    dnn_model.add(Dense(64, input_dim=X_train.shape[1], activation='relu'))
    dnn_model.add(Dense(32, activation='relu'))
    dnn_model.add(Dense(1, activation='linear'))

    # 编译模型
    dnn_model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    # 添加 Early Stopping 回调
    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

    # 训练模型
    dnn_model.fit(X_train, y_train, epochs=5000, batch_size=4, verbose=0, validation_split=0.2, callbacks=[early_stopping])

    # 提取标准化参数
    scaler_params = {
        'mean': scaler_X.mean_.tolist(),
        'scale': scaler_X.scale_.tolist()
    }

    return prophet_model, dnn_model, scaler_params

def main():
    global dayStr

    # 找到本周的周一
    monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    mondayStr = monday.strftime('%Y%m%d')

    print(f"本周一： {mondayStr}")

    # 向前推8周
    start_date = monday - pd.Timedelta(days=60)
    startDateStr = start_date.strftime('%Y%m%d')

    lastSunday = monday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    print(f'向前推60天：{startDateStr}~{lastSundayStr}')

    # 获取当前平台的历史数据
    historical_data = getHistoricalData(startDateStr, lastSundayStr)
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')
    historical_data = historical_data.sort_values(by=['install_day'])
    
    historical_data.rename(columns={'install_day':'ds','revenue_1d':'y'}, inplace=True)
    
    groupData = historical_data.groupby(['platform','media','country','max_r'])

    modelDf = pd.DataFrame()

    for (platform, media, country, max_r), group_data0 in groupData:

        # # for test
        # if platform != 'android' or media != 'ALL' or country != 'ALL' or max_r != 1e10:
        #     print('For test !!!')
        #     print(f"Skip platform: {platform}, media: {media}, country: {country}, max_r: {max_r}")
        #     continue

        print(f"platform: {platform}, media: {media}, country: {country}, max_r: {max_r}")
        if platform == 'ios' and media != 'ALL':
            print('ios的media不是ALL，跳过')
            continue

        prophet_model, dnn_model, scaler_params = train_model(group_data0)
        if prophet_model is None or dnn_model is None:
            continue

        # 将 DNN 模型的权重保存为字符串
        model_weights = dnn_model.get_weights()
        
        # 将模型权重转换为二进制数据
        model_weights_binary = [w.tobytes() for w in model_weights]
        # 将二进制数据进行 Base64 编码
        model_weights_base64 = [base64.b64encode(w).decode('utf-8') for w in model_weights_binary]
        # 将 Base64 编码的数组转换为单个字符串
        model_weights_base64_str = json.dumps(model_weights_base64)

        # 将标准化参数转换为 JSON 字符串
        scaler_params_json = json.dumps(scaler_params)

        modelDf = modelDf.append({
            'app': 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147',
            'platform': platform,
            'media': media,
            'country': country,
            'max_r': max_r,
            'prophet_model': model_to_json(prophet_model),
            'dnn_model': dnn_model.to_json(),
            'model_weights_base64': model_weights_base64_str,
            'scaler_params': scaler_params_json  # 新增列
        }, ignore_index=True)

    # 写入表格
    writeTable(modelDf, mondayStr)

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

    main()