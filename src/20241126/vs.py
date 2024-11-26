import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

from sklearn.metrics import mean_absolute_percentage_error
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam

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
    day BETWEEN '20240801' AND '20241031'
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

def prophetTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue


    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
            continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 初始化并训练Prophet模型
        model = Prophet()
        model.add_regressor('cost')
        model.fit(train_data)
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

        # 进行预测
        forecast = model.predict(test_data)

        retDf = pd.merge(test_data, forecast[['ds', 'yhat']], on='ds', how='left')
        retDf['mape'] = np.abs((retDf['revenue'] - retDf['yhat']) / retDf['revenue'])
        retDf.to_csv(f'/src/data/20241126_prophet_{media}_{country}.csv', index=False)

        # 计算MAPE
        # mape = mean_absolute_percentage_error(retDf['revenue'], retDf['yhat'])
        mape = retDf['mape'].mean()
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)

def dnnTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
            continue

        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        X_train = group[['cost']].values
        y_train = group['revenue'].values
        
        # 标准化
        scaler_X = StandardScaler()
        scaler_y = StandardScaler()
        X_train_scaled = scaler_X.fit_transform(X_train)
        y_train_scaled = scaler_y.fit_transform(y_train.reshape(-1, 1)).flatten()
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        # model.compile(optimizer=Adam(learning_rate=0.001), loss='mean_squared_error')
        model.compile(optimizer='RMSprop', loss='mean_squared_error',metrics=['mape'])
        
        # 训练模型
        model.fit(X_train_scaled, y_train_scaled, epochs=100, batch_size=4, verbose=1)
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_group = test_group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(test_group) < 1:  # 如果测试数据行数不足1行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient test data after filtering.')
            continue
        
        X_test = test_group[['cost']].values
        y_test = test_group['revenue'].values
        
        # 标准化测试数据
        X_test_scaled = scaler_X.transform(X_test)
        y_test_scaled = scaler_y.transform(y_test.reshape(-1, 1)).flatten()
        
        # 进行预测
        y_pred_scaled = model.predict(X_test_scaled).flatten()
        y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)

if __name__ == '__main__':
    # prophetTest()
    dnnTest()