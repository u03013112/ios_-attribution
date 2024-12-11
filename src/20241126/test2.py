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
    # set_random_seed()  # 设置随机种子

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

    # 提取测试数据的季节性成分
    test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
    
    # 过滤掉 NaN 和 inf
    train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
    test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()
    
    # 标准化
    scaler_X = StandardScaler()
    train_data[['cost', 'weekly']] = scaler_X.fit_transform(train_data[['cost', 'weekly']])
    test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])
    
    # 将训练数据按日期排序
    train_data = train_data.sort_values(by='ds')
    
    # 取最后一周的数据作为验证集
    val_data = train_data[-7:]
    train_data = train_data[:-7]
    
    print('train_data:')
    print(train_data)

    print('val_data:')
    print(val_data)

    X_train = train_data[['cost', 'weekly']]
    y_train = train_data['y']
    
    X_val = val_data[['cost', 'weekly']]
    y_val = val_data['y']
    
    X_test = test_data[['cost', 'weekly']]
    y_test = test_data['revenue']
    
    best_mape = float('inf')
    best_y_pred = None
    
    for _ in range(1):  # 简单循环3次，选择最优结果
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
        history = dnn_model.fit(X_train, y_train, epochs=5000, batch_size=4, verbose=0, validation_data=(X_val, y_val), callbacks=[early_stopping])
        
        # 进行预测
        y_pred = dnn_model.predict(X_test).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        if mape < best_mape:
            best_mape = mape
            best_y_pred = y_pred
    
    result = test_data[['ds']].copy()
    result['y'] = test_data['revenue']
    result['y_pred'] = best_y_pred
    result['mape'] = np.abs((result['y'] - result['y_pred']) / result['y'])
    result.rename(columns={'ds': 'install_day'}, inplace=True)
    
    return result

# import matplotlib.pyplot as plt
# def calculate_mape(group, train_start_date, train_end_date, test_start_date, test_end_date):
#     set_random_seed()  # 设置随机种子

#     train_data = group[(group['install_day'] >= train_start_date) & (group['install_day'] <= train_end_date)]
#     test_data = group[(group['install_day'] >= test_start_date) & (group['install_day'] <= test_end_date)]
    
#     if len(train_data) == 0 or len(test_data) == 0:
#         return pd.DataFrame(columns=['install_day', 'mape'])
    
#     # 准备训练数据
#     train_data = train_data[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
    
#     # 准备测试数据
#     test_data = test_data[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})
    
#     # 初始化并训练Prophet模型
#     model = Prophet(weekly_seasonality=True)
#     model.add_regressor('cost')
#     model.fit(train_data)
    
#     # 提取训练数据的季节性成分
#     train_forecast = model.predict(train_data)
#     train_data = train_data.merge(train_forecast[['ds', 'weekly']], on='ds')

#     # 进行预测
#     test_forecast = model.predict(test_data)

#     # 提取测试数据的季节性成分
#     test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
    
#     # 过滤掉 NaN 和 inf
#     train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
#     test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()
    
#     # 标准化
#     scaler_X = StandardScaler()
#     train_data[['cost', 'weekly']] = scaler_X.fit_transform(train_data[['cost', 'weekly']])
#     test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])
    
#     # 准备DNN的训练数据
#     X_train = train_data[['cost', 'weekly']]
#     y_train = train_data['y']
    
#     # 准备DNN的测试数据
#     X_test = test_data[['cost', 'weekly']]
#     y_test = test_data['revenue']
    
#     best_mape = float('inf')
#     best_y_pred = None
    
#     # set_random_seed()
#     for i in range(1):  # 简单循环3次，选择最优结果

#         # 构建DNN模型
#         dnn_model = Sequential()
#         dnn_model.add(Dense(64, input_dim=X_test.shape[1], activation='relu'))
#         dnn_model.add(Dense(32, activation='relu'))
#         dnn_model.add(Dense(1, activation='linear'))
        
#         # 编译模型
#         dnn_model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
        
#         # 记录每100个epochs的loss和mape
#         epochs = 1000
#         batch_size = 4
#         history = {'epoch': [], 'train_loss': [], 'val_loss': [], 'test_loss': [], 'train_mape': [], 'val_mape': [], 'test_mape': []}
        
#         for epoch in range(0, epochs, 20):
#             hist = dnn_model.fit(X_train, y_train, epochs=20, batch_size=batch_size, verbose=0, validation_split=0.2)
            
#             # 计算训练集和验证集的loss和mape
#             train_loss = hist.history['loss'][-1]
#             val_loss = hist.history['val_loss'][-1]
#             train_mape = hist.history['mape'][-1]
#             val_mape = hist.history['val_mape'][-1]
            
#             # 计算测试集的loss和mape
#             test_loss, test_mape = dnn_model.evaluate(X_test, y_test, verbose=0)
            
#             if train_loss > 1e7 :
#                 train_loss = 1e7
#             if val_loss > 1e7 :
#                 val_loss = 1e7
#             if test_loss > 1e7 :
#                 test_loss = 1e7

#             # if train_mape > 30 :
#             #     train_mape = 30
#             # if val_mape > 30 :
#             #     val_mape = 30
#             # if test_mape > 30 :
#             #     test_mape = 30

#             # 记录loss和mape
#             history['epoch'].append(epoch + 100)
#             history['train_loss'].append(train_loss)
#             history['val_loss'].append(val_loss)
#             history['test_loss'].append(test_loss)
#             history['train_mape'].append(train_mape)
#             history['val_mape'].append(val_mape)
#             history['test_mape'].append(test_mape)
        
#         # 进行预测
#         y_pred = dnn_model.predict(X_test).flatten()
        
#         # 计算MAPE
#         mape = mean_absolute_percentage_error(y_test, y_pred)
        
#         if mape < best_mape:
#             best_mape = mape
#             best_y_pred = y_pred
        
#         historyDf = pd.DataFrame(history)
#         historyDf.to_csv(f'/src/data/20241126_prophet_dnn_history{i+1}_{test_start_date}_{test_end_date}.csv', index=False)

#         # # 画图并保存
#         plt.figure()
#         plt.plot(history['epoch'], history['train_loss'], label='Train Loss')
#         plt.plot(history['epoch'], history['val_loss'], label='Validation Loss')
#         plt.plot(history['epoch'], history['test_loss'], label='Test Loss')
#         plt.axvline(x=history['epoch'][np.argmin(history['val_loss'])], color='r', linestyle='--', label='Early Stopping Point')
#         plt.xlabel('Epochs')
#         plt.ylabel('Loss')
#         plt.legend()
#         plt.title(f'Training and Validation Loss (Run {i+1})')
#         plt.savefig(f'/src/data/20241126_prophet_dnn_loss_pic{i+1}_{test_start_date}_{test_end_date}.png')
#         plt.close()
        
#         # 画MAPE图并保存
#         plt.figure()
#         plt.plot(history['epoch'], history['train_mape'], label='Train MAPE')
#         plt.plot(history['epoch'], history['val_mape'], label='Validation MAPE')
#         plt.plot(history['epoch'], history['test_mape'], label='Test MAPE')
#         plt.axvline(x=history['epoch'][np.argmin(history['val_loss'])], color='r', linestyle='--', label='Early Stopping Point')
#         plt.xlabel('Epochs')
#         plt.ylabel('MAPE')
#         plt.legend()
#         plt.title(f'Training and Validation MAPE (Run {i+1})')
#         plt.savefig(f'/src/data/20241126_prophet_dnn_mape_pic{i+1}_{test_start_date}_{test_end_date}.png')
#         plt.close()
    
#     result = test_data[['ds']].copy()
#     result['y'] = test_data['revenue']
#     result['y_pred'] = best_y_pred
#     result['mape'] = np.abs((result['y'] - result['y_pred']) / result['y'])
#     result.rename(columns={'ds': 'install_day'}, inplace=True)
    
#     return result

def prophetDnnTest3():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 尝试的训练周期和预测周期列表
    try_periods = [
        # {'N': 30, 'M': 7}, 
        # {'N': 60, 'M': 7},
        {'N': 90, 'M': 7},
        # {'N': 120, 'M': 7},
        # {'N': 180, 'M': 7},
        # {'N': 210, 'M': 7},
    ]
    
    results = []
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL')]:
        if media != 'ALL' and country != 'ALL':
            continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        
        for period in try_periods:
            N = period['N']
            M = period['M']
            
            if len(group) < N + M:  # 如果过滤后数据行数不足N+M行，则跳过
                print(f'Skipping Media: {media}, Country: {country}, N: {N}, M: {M} due to insufficient data after filtering.')
                continue
            
            # 分段测试
            # test_periods = pd.date_range(start='2024-09-01', end='2024-10-31', freq=f'{M}D')
            test_periods = pd.date_range(start='2024-09-02', end='2024-10-31', freq=f'{M}D')

            period_results = []
            for start_date in test_periods:
                end_date = start_date + pd.Timedelta(days=M-1)
                if end_date > pd.Timestamp('2024-10-31'):
                    break
                
                train_end_date = start_date - pd.Timedelta(days=1)
                train_start_date = train_end_date - pd.Timedelta(days=N-1)
                
                period_result = calculate_mape(group, train_start_date, train_end_date, start_date, end_date)
                print(f'Media: {media}, Country: {country}, N: {N}, M: {M}, Start Date: {start_date}, End Date: {end_date}, Train Start Date: {train_start_date}, Train End Date: {train_end_date}')
                print(period_result)

                period_results.append(period_result)
            
            if period_results:
                combined_results = pd.concat(period_results)
                overall_mape = combined_results['mape'].mean()
                
                results.append({
                    'media': media,
                    'country': country,
                    'N': N,
                    'M': M,
                    'mape': overall_mape
                })
                print(f'Media: {media}, Country: {country}, N: {N}, M: {M}, MAPE: {overall_mape:.4f}')

    results_df = pd.DataFrame(results)
    print(results_df)

    results_df.to_csv('/src/data/20241126_prophet_dnn_test2.csv', index=False)
    return results_df

if __name__ == '__main__':
    # 运行测试
    prophetDnnTest3()
