import pandas as pd
import numpy as np
import os
import sys
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.preprocessing import StandardScaler
from keras.callbacks import EarlyStopping
from keras.models import Sequential
from keras.layers import Dense
from prophet import Prophet

import json
import base64
from prophet.serialize import model_to_json, model_from_json
from tensorflow.keras.models import model_from_json as tf_model_from_json

sys.path.append('/src')
from src.maxCompute import execSql

def getData():
    filename = '/src/data/20241126_data.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
SELECT
    install_day,
    media,
    country,
    cost,
    pu_1d as pu,
    revenue_1d as revenue,
    actual_arppu
FROM
    lastwar_predict_day1_pu_pct_by_cost_pct__nerfr_historical_data2
WHERE
    day BETWEEN '20240601' AND '20241031'
    and platform = 'android'
    and group_name = 'g1__all'
    and max_r = 10000000000
;
        '''
        print("执行的SQL语句如下：\n")
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

import tensorflow as tf
import random

# 设置随机种子
def set_random_seed(seed_value=42):
    np.random.seed(seed_value)
    tf.random.set_seed(seed_value)
    random.seed(seed_value)

def calculate_mape(group, train_start_date, train_end_date, test_start_date, test_end_date):
    set_random_seed()
    train_data = group[(group['install_day'] >= train_start_date) & (group['install_day'] <= train_end_date)]
    test_data = group[(group['install_day'] >= test_start_date) & (group['install_day'] <= test_end_date)]
    
    if len(train_data) == 0 or len(test_data) == 0:
        return pd.DataFrame(columns=['install_day', 'mape'])
    
    # 准备训练数据
    train_data = train_data[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
    
    # 准备测试数据
    test_data = test_data[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})
    
    # 初始化并训练Prophet模型
    model = Prophet(weekly_seasonality=True)
    model.add_regressor('cost')
    model.fit(train_data)
    
    # 提取训练数据的季节性成分
    train_forecast = model.predict(train_data)
    train_data = train_data.merge(train_forecast[['ds', 'weekly']], on='ds')

    # 进行预测
    test_forecast = model.predict(test_data)
    print(test_forecast[['ds','weekly']])

    # 提取测试数据的季节性成分
    test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
    
    # 过滤掉 NaN 和 inf
    train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
    test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()
    print('train_data:')
    print(train_data[['ds', 'y', 'cost', 'weekly']])

    # 标准化
    scaler_X = StandardScaler()
    train_data[['cost', 'weekly']] = scaler_X.fit_transform(train_data[['cost', 'weekly']])
    test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])

    # 准备DNN的训练数据
    X_train = train_data[['cost', 'weekly']]
    y_train = train_data['y']
    print('X_train:')
    print(X_train)
    print('y_train:')
    print(y_train)
    
    # 准备DNN的测试数据
    X_test = test_data[['cost', 'weekly']]
    y_test = test_data['revenue']
    
    # 构建DNN模型
    dnn_model = Sequential()
    dnn_model.add(Dense(64, input_dim=X_test.shape[1], activation='relu'))
    dnn_model.add(Dense(32, activation='relu'))
    dnn_model.add(Dense(1, activation='linear'))
    
    # 编译模型
    dnn_model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
    
    # 添加 Early Stopping 回调
    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    
    # 训练模型
    dnn_model.fit(X_train, y_train, epochs=5000, batch_size=4, verbose=0, validation_split=0.2, callbacks=[early_stopping])
    
    # 进行预测
    y_pred = dnn_model.predict(X_test).flatten()
    
    result = test_data[['ds']].copy()
    result['y'] = test_data['revenue']
    result['y_pred'] = y_pred
    result['mape'] = np.abs((result['y'] - result['y_pred']) / result['y'])
    result.rename(columns={'ds': 'install_day'}, inplace=True)
    
    return result, model, dnn_model, scaler_X

def save_model_to_csv(prophet_model, dnn_model, scaler_X, filename):
    # 序列化 Prophet 模型
    prophet_model_json = model_to_json(prophet_model)
    
    # 序列化 DNN 模型
    dnn_model_json = dnn_model.to_json()
    model_weights = dnn_model.get_weights()
    model_weights_binary = [w.tobytes() for w in model_weights]
    model_weights_base64 = [base64.b64encode(w).decode('utf-8') for w in model_weights_binary]
    
    # 序列化 StandardScaler 参数
    scaler_params = {
        'mean': scaler_X.mean_.tolist(),
        'scale': scaler_X.scale_.tolist()
    }
    
    # 创建 DataFrame
    model_data = {
        'platform': ['android'],
        'media': ['ALL'],
        'country': ['ALL'],
        'max_r': [10000000000],
        'prophet_model': [prophet_model_json],
        'dnn_model': [dnn_model_json],
        'model_weights_base64': [json.dumps(model_weights_base64)],
        'scaler_params': [json.dumps(scaler_params)]
    }
    
    model_df = pd.DataFrame(model_data)
    model_df.to_csv(filename, index=False)

def prophetDnnTest3():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 固定训练和测试时间
    train_start_date = pd.Timestamp('2024-07-04')
    train_end_date = pd.Timestamp('2024-09-01')
    test_start_date = pd.Timestamp('2024-09-02')
    test_end_date = pd.Timestamp('2024-09-02')
    
    results = []
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        if (media, country) not in [('ALL', 'ALL')]:
            continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        
        period_result, prophet_model, dnn_model, scaler_X = calculate_mape(group, train_start_date, train_end_date, test_start_date, test_end_date)
        print(f'Media: {media}, Country: {country}, Train Start Date: {train_start_date}, Train End Date: {train_end_date}, Test Date: {test_start_date}')
        print(period_result)

        results.append(period_result)
        
        # 保存模型到 CSV 文件
        save_model_to_csv(prophet_model, dnn_model, scaler_X, '/src/data/20241126_prophet_dnn_test2_tmp.csv')
    
        

    if results:
        combined_results = pd.concat(results)
        overall_mape = combined_results['mape'].mean()
        
        print(f'Overall MAPE: {overall_mape:.4f}')
    
    combined_results.to_csv('/src/data/20241126_prophet_dnn_test3.csv', index=False)
    return combined_results

def loadModels(platform, media, country, max_r, dayStr):
    filename = '/src/data/lastwar_predict_day1_revenue_by_cost__nerf_r_train.csv'
    models_df = pd.read_csv(filename)
    
    # 过滤出符合条件的模型
    row = models_df[(models_df['platform'] == platform) & 
                    (models_df['media'] == media) & 
                    (models_df['country'] == country) & 
                    (models_df['max_r'] == max_r)].iloc[0]
    
    # 加载 Prophet 模型
    prophet_model = model_from_json(row['prophet_model'])
    
    # 加载 DNN 模型
    dnn_model = tf_model_from_json(row['dnn_model'])
    dummy_weights = dnn_model.get_weights()
    model_weights_shapes = [w.shape for w in dummy_weights]

    model_weights_base64 = json.loads(row['model_weights_base64'])
    model_weights_binary = [base64.b64decode(w) for w in model_weights_base64]
    model_weights = [np.frombuffer(w, dtype=np.float32).reshape(shape) for w, shape in zip(model_weights_binary, model_weights_shapes)]

    dnn_model.set_weights(model_weights)
    dnn_model.compile(optimizer='RMSprop', loss='mean_squared_error')

    # 解析 scaler_params 并创建 StandardScaler 对象
    scaler_params = json.loads(row['scaler_params'])
    scaler_X = StandardScaler()
    scaler_X.mean_ = np.array(scaler_params['mean'])
    scaler_X.scale_ = np.array(scaler_params['scale'])
    scaler_X.var_ = scaler_X.scale_ ** 2  # 计算方差

    return prophet_model, dnn_model, scaler_X

def calculate_mape_with_loaded_models(group, prophet_model, dnn_model, scaler_X, test_start_date, test_end_date):
    test_data = group[(group['install_day'] >= test_start_date) & (group['install_day'] <= test_end_date)]
    
    if len(test_data) == 0:
        return pd.DataFrame(columns=['install_day', 'mape'])
    
    # 准备测试数据
    test_data = test_data[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})
    
    # 进行预测
    test_forecast = prophet_model.predict(test_data)
    print(test_forecast[['ds','weekly']])

    # 提取测试数据的季节性成分
    test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
    
    # 过滤掉 NaN 和 inf
    test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()
    
    # 标准化
    test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])
    print('test_data')
    print(test_data)
    
    # 准备DNN的测试数据
    X_test = test_data[['cost', 'weekly']]
    y_test = test_data['revenue']
    
    # 进行预测
    y_pred = dnn_model.predict(X_test).flatten()
    
    result = test_data[['ds']].copy()
    result['y'] = test_data['revenue']
    result['y_pred'] = y_pred
    result['mape'] = np.abs((result['y'] - result['y_pred']) / result['y'])
    result.rename(columns={'ds': 'install_day'}, inplace=True)
    
    return result

def test2():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 固定测试时间
    test_start_date = pd.Timestamp('2024-09-02')
    test_end_date = pd.Timestamp('2024-09-02')
    
    results = []
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        if (media, country) not in [('ALL', 'ALL')]:
            continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        
        # 加载模型
        prophet_model, dnn_model, scaler_X = loadModels('android', media, country, 10000000000, '2024-09-02')
        
        if prophet_model is None or dnn_model is None or scaler_X is None:
            print(f'No models found for Media: {media}, Country: {country}')
            continue
        
        period_result = calculate_mape_with_loaded_models(group, prophet_model, dnn_model, scaler_X, test_start_date, test_end_date)
        print(f'Media: {media}, Country: {country}, Test Date: {test_start_date}')
        print(period_result)

        results.append(period_result)
    
    if results:
        combined_results = pd.concat(results)
        overall_mape = combined_results['mape'].mean()
        
        print(f'Overall MAPE: {overall_mape:.4f}')
    
    combined_results.to_csv('/src/data/20241126_prophet_dnn_test2.csv', index=False)
    return combined_results

if __name__ == '__main__':
    # 运行测试
    # prophetDnnTest3()
    test2()
