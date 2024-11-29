import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

from tensorflow import keras
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.preprocessing import StandardScaler


from keras.callbacks import EarlyStopping
from keras.initializers import Zeros

from keras.models import Sequential, Model
from keras.layers import Dense, Input, Add

from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error

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

def prophetDnnTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue

    results = []
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL')]:
        #     continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

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
        
        # train_data 过滤 掉 NaN和inf
        train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
        test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()

        # 标准化
        scaler_X = StandardScaler()
        train_data[['cost', 'weekly']] = scaler_X.fit_transform(train_data[['cost', 'weekly']])
        test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])

        # 准备DNN的训练数据
        X_train = train_data[['cost', 'weekly']]
        y_train = train_data['y']
        
        # 准备DNN的测试数据
        X_test = test_data[['cost', 'weekly']]
        y_test = test_data['revenue']
        
        best_mape = float('inf')
        best_y_pred = None
        
        for _ in range(3):  # 简单循环3次，选择最优结果
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
            dnn_model.fit(X_train, y_train, epochs=5000, batch_size=4, verbose=0, validation_data=(X_test, y_test), callbacks=[early_stopping])
            
            # 进行预测
            y_pred = dnn_model.predict(X_test).flatten()
            
            # 计算MAPE
            mape = mean_absolute_percentage_error(y_test, y_pred)
            
            if mape < best_mape:
                best_mape = mape
                best_y_pred = y_pred
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {best_mape:.4f}')
        results.append({
            'media': media,
            'country': country,
            'mape': best_mape
        })

    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df


def prophetDnnTest2():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue

    results = []
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL')]:
        #     continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

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
        
        # train_data 过滤 掉 NaN和inf
        train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
        test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()

        # 标准化
        scaler_X = StandardScaler()
        train_data[['cost', 'weekly']] = scaler_X.fit_transform(train_data[['cost', 'weekly']])
        test_data[['cost', 'weekly']] = scaler_X.transform(test_data[['cost', 'weekly']])

        # 准备DNN的训练数据
        X_train = train_data[['cost', 'weekly']]
        y_train = train_data['y']
        
        # 准备DNN的测试数据
        X_test = test_data[['cost', 'weekly']]
        y_test = test_data['revenue']
        
        best_mape = float('inf')
        best_y_pred = None
        
        for _ in range(3):  # 简单循环3次，选择最优结果
            # 构建DNN模型
            input_cost = Input(shape=(1,), name='cost')
            input_weekly = Input(shape=(1,), name='weekly')
            
            x = Dense(64, activation='relu')(input_cost)
            x = Dense(32, activation='relu')(x)
            x = Dense(1, activation='linear')(x)
            
            output = Add()([x, input_weekly])
            
            dnn_model = Model(inputs=[input_cost, input_weekly], outputs=output)
            
            # 编译模型
            dnn_model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
            
            # keras.utils.plot_model(dnn_model, '/src/data/20241128_model1.jpg', show_shapes=True)
            # return

            # 添加 Early Stopping 回调
            early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
            
            # 训练模型
            dnn_model.fit([X_train['cost'], X_train['weekly']], y_train, epochs=5000, batch_size=4, verbose=0, validation_data=([X_test['cost'], X_test['weekly']], y_test), callbacks=[early_stopping])
            
            # 进行预测
            y_pred = dnn_model.predict([X_test['cost'], X_test['weekly']]).flatten()
            
            # 计算MAPE
            mape = mean_absolute_percentage_error(y_test, y_pred)
            
            if mape < best_mape:
                best_mape = mape
                best_y_pred = y_pred
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {best_mape:.4f}')
        results.append({
            'media': media,
            'country': country,
            'mape': best_mape
        })

    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df


def weeklyCorrTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue

    results = []
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        if (media, country) not in [('APPLOVIN', 'ALL')]:
            continue
        
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

        # 初始化并训练Prophet模型
        model = Prophet()
        model.add_regressor('cost',standardize=False)
        model.fit(train_data)
        
        # 提取训练数据的季节性成分
        train_forecast = model.predict(train_data)
        train_data = train_data.merge(train_forecast[['ds', 'weekly']], on='ds')
        
        print(train_forecast)
        # 进行预测
        test_forecast = model.predict(test_data)

        # 提取测试数据的季节性成分
        test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
        
        # train_data 过滤 掉 NaN和inf
        train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
        test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()

        # 计算相关系数
        corr_train1 = np.corrcoef(train_data['cost'], train_data['y'])[0, 1]
        corr_test1 = np.corrcoef(test_data['cost'], test_data['revenue'])[0, 1]
        corr_train2 = np.corrcoef(train_data['cost'], train_data['y'] - train_data['weekly'])[0, 1]
        corr_test2 = np.corrcoef(test_data['cost'], test_data['revenue'] - test_data['weekly'])[0, 1]
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, Corr_train1: {corr_train1:.4f}, Corr_test1: {corr_test1:.4f}, Corr_train2: {corr_train2:.4f}, Corr_test2: {corr_test2:.4f}')
        results.append({
            'media': media,
            'country': country,
            'corr_train1': corr_train1,
            'corr_test1': corr_test1,
            'corr_train2': corr_train2,
            'corr_test2': corr_test2
        })

    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df


if __name__ == '__main__':
    # corrDf = weeklyCorrTest()
    # corrDf.to_csv('/src/data/20241126_weekly_corr.csv', index=False)
    mapeDf = prophetDnnTest()
    # mapeDf.to_csv('/src/data/20241126_prophet_dnn_mape.csv', index=False)
    # mape2Df = prophetDnnTest2()
    # mape2Df.to_csv('/src/data/20241126_prophet_dnn_mape2.csv', index=False)
    