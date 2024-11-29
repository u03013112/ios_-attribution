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
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.initializers import Zeros
from statsmodels.tsa.seasonal import seasonal_decompose

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

def getCorr():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
        #     continue
        
        print('\n>>', media, country)
        # 只关注cost和revenue的相关性
        train_group = train_df[(train_df['media'] == media) & (train_df['country'] == country)]
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        
        if not train_group.empty and not test_group.empty:
            trainCorr = train_group.corr()
            testCorr = test_group.corr()
            ret[(media, country)] = (trainCorr['cost']['revenue'], testCorr['cost']['revenue'])
        else:
            ret[(media, country)] = (np.nan, np.nan)

    print(ret)
    return ret

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
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
        #     continue
        
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
    return ret

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
        # if (media, country) not in [('ALL', 'ALL')]:
        #     continue

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
        X_train_scaled = scaler_X.fit_transform(X_train)
        
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
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(64, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
        
        # 添加 Early Stopping 回调
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=5000, batch_size=4, verbose=0, validation_data=(X_test_scaled, y_test), callbacks=[early_stopping])
        
        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def getWeeklySeasonalStrength():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        print('\n>>', media, country)
        
        # 只关注cost和revenue的相关性
        train_group = train_df[(train_df['media'] == media) & (train_df['country'] == country)]
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        
        if train_group.empty or test_group.empty:
            continue
        
        # 设置日期列为索引
        train_group.set_index('install_day', inplace=True)
        test_group.set_index('install_day', inplace=True)
        
        # 计算训练集的每周季节性强度
        try:
            train_group['roi'] = train_group['revenue'] / train_group['cost']
            roi_weekly_result_train = seasonal_decompose(train_group['roi'], model='additive', period=7)
            roi_weekly_seasonal_strength_train = np.var(roi_weekly_result_train.seasonal) / (np.var(roi_weekly_result_train.seasonal) + np.var(roi_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing ROI for {media}, {country} (train): {e}')
            roi_weekly_seasonal_strength_train = None
        
        try:
            revenue_weekly_result_train = seasonal_decompose(train_group['revenue'], model='additive', period=7)
            revenue_weekly_seasonal_strength_train = np.var(revenue_weekly_result_train.seasonal) / (np.var(revenue_weekly_result_train.seasonal) + np.var(revenue_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing Revenue for {media}, {country} (train): {e}')
            revenue_weekly_seasonal_strength_train = None
        
        # 计算测试集的每周季节性强度
        try:
            test_group['roi'] = test_group['revenue'] / test_group['cost']
            roi_weekly_result_test = seasonal_decompose(test_group['roi'], model='additive', period=7)
            roi_weekly_seasonal_strength_test = np.var(roi_weekly_result_test.seasonal) / (np.var(roi_weekly_result_test.seasonal) + np.var(roi_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing ROI for {media}, {country} (test): {e}')
            roi_weekly_seasonal_strength_test = None
        
        try:
            revenue_weekly_result_test = seasonal_decompose(test_group['revenue'], model='additive', period=7)
            revenue_weekly_seasonal_strength_test = np.var(revenue_weekly_result_test.seasonal) / (np.var(revenue_weekly_result_test.seasonal) + np.var(revenue_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing Revenue for {media}, {country} (test): {e}')
            revenue_weekly_seasonal_strength_test = None
        
        ret[(media, country)] = (roi_weekly_seasonal_strength_train, revenue_weekly_seasonal_strength_train, roi_weekly_seasonal_strength_test, revenue_weekly_seasonal_strength_test)
        
        # 打印结果，处理 None 值
        roi_wss_train_str = f'{roi_weekly_seasonal_strength_train:.4f}' if roi_weekly_seasonal_strength_train is not None else 'N/A'
        revenue_wss_train_str = f'{revenue_weekly_seasonal_strength_train:.4f}' if revenue_weekly_seasonal_strength_train is not None else 'N/A'
        roi_wss_test_str = f'{roi_weekly_seasonal_strength_test:.4f}' if roi_weekly_seasonal_strength_test is not None else 'N/A'
        revenue_wss_test_str = f'{revenue_weekly_seasonal_strength_test:.4f}' if revenue_weekly_seasonal_strength_test is not None else 'N/A'
        
        print(f'ROI Weekly Seasonal Strength for {media}, {country} (train): {roi_wss_train_str}')
        print(f'Revenue Weekly Seasonal Strength for {media}, {country} (train): {revenue_wss_train_str}')
        print(f'ROI Weekly Seasonal Strength for {media}, {country} (test): {roi_wss_test_str}')
        print(f'Revenue Weekly Seasonal Strength for {media}, {country} (test): {revenue_wss_test_str}')
    
    return ret

def prophetDnnTest():
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
        # if (media, country) not in [('ALL', 'ALL')]:
        #     continue

        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 初始化并训练Prophet模型
        model = Prophet(weekly_seasonality=True)
        model.add_regressor('cost')
        model.fit(train_data)
        
        # 提取训练数据的季节性成分
        train_forecast = model.predict(train_data)
        train_data = train_data.merge(train_forecast[['ds', 'weekly']], on='ds')
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

        # 进行预测
        test_forecast = model.predict(test_data)

        # 提取测试数据的季节性成分
        test_data = test_data.merge(test_forecast[['ds', 'weekly']], on='ds')
        
        # train_data 过滤 掉 NaN和inf
        train_data = train_data.replace([np.inf, -np.inf], np.nan).dropna()
        test_data = test_data.replace([np.inf, -np.inf], np.nan).dropna()

        # 准备DNN的训练数据
        X_train = train_data[['cost', 'weekly']]
        y_train = train_data['y']
        
        # 标准化
        scaler_X = StandardScaler()
        X_train_scaled = scaler_X.fit_transform(X_train)
        
        # 准备DNN的测试数据
        X_test = test_data[['cost', 'weekly']]
        y_test = test_data['revenue']
        
        # 标准化测试数据
        X_test_scaled = scaler_X.transform(X_test)
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(64, input_dim=X_test_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
        
        # 添加 Early Stopping 回调
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=5000, batch_size=4, verbose=0, validation_data=(X_test_scaled, y_test), callbacks=[early_stopping])
        
        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def report():
    prophetDnnRet = prophetDnnTest()
    corrRet = getCorr()
    prophetRet = prophetTest()
    dnnRet = dnnTest()
    weeklySeasonalStrength = getWeeklySeasonalStrength()

    # 生成报告
    report_data = []
    for key in corrRet.keys():
        media, country = key
        corr_train, corr_test = corrRet[key]
        prophet_mape = prophetRet.get(key, np.nan)
        dnn_mape = dnnRet.get(key, np.nan)
        prophet_dnn_mape = prophetDnnRet.get(key, np.nan)
        roi_wss_train, revenue_wss_train, roi_wss_test, revenue_wss_test = weeklySeasonalStrength.get(key, (np.nan, np.nan, np.nan, np.nan))
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape, prophet_dnn_mape, roi_wss_train, revenue_wss_train, roi_wss_test, revenue_wss_test])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape', 'prophet_dnn_mape', 'roi_wss_train', 'revenue_wss_train', 'roi_wss_test', 'revenue_wss_test'])
    report_df.to_csv('/src/data/20241126report.csv', index=False)
    print("Report generated and saved to /src/data/20241126report.csv")
def getCorrPu():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        print('\n>>', media, country)
        # 只关注cost和pu的相关性
        train_group = train_df[(train_df['media'] == media) & (train_df['country'] == country)]
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        
        if not train_group.empty and not test_group.empty:
            trainCorr = train_group.corr()
            testCorr = test_group.corr()
            ret[(media, country)] = (trainCorr['cost']['pu'], testCorr['cost']['pu'])
        else:
            ret[(media, country)] = (np.nan, np.nan)

    print(ret)
    return ret

def prophetPuTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出pu

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'pu']].rename(columns={'install_day': 'ds', 'pu': 'y'})
        
        # 初始化并训练Prophet模型
        model = Prophet()
        model.add_regressor('cost')
        model.fit(train_data)
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'pu']].rename(columns={'install_day': 'ds'})

        # 进行预测
        forecast = model.predict(test_data)

        retDf = pd.merge(test_data, forecast[['ds', 'yhat']], on='ds', how='left')
        retDf['mape'] = np.abs((retDf['pu'] - retDf['yhat']) / retDf['pu'])
        retDf.to_csv(f'/src/data/20241126_prophet_pu_{media}_{country}.csv', index=False)

        # 计算MAPE
        mape = retDf['mape'].mean()
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def dnnPuTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出pu

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # 过滤掉包含 NaN 或无穷大值的行
        group = group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(group) < 10:  # 如果过滤后数据行数不足10行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data after filtering.')
            continue
        
        # 准备训练数据
        X_train = group[['cost']].values
        y_train = group['pu'].values
        
        # 标准化
        scaler_X = StandardScaler()
        X_train_scaled = scaler_X.fit_transform(X_train)
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_group = test_group.replace([np.inf, -np.inf], np.nan).dropna()
        if len(test_group) < 1:  # 如果测试数据行数不足1行，则跳过
            print(f'Skipping Media: {media}, Country: {country} due to insufficient test data after filtering.')
            continue

        X_test = test_group[['cost']].values
        y_test = test_group['pu'].values
        
        # 标准化测试数据
        X_test_scaled = scaler_X.transform(X_test)
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mape'])
        
        # 添加 Early Stopping 回调
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=5000, batch_size=4, verbose=0, validation_data=(X_test_scaled, y_test), callbacks=[early_stopping])
        
        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def getWeeklySeasonalStrengthPuCpup():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 计算 cpup
    df['cpup'] = df['cost'] / df['pu']
    
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]

    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        print('\n>>', media, country)
        
        # 只关注cost和pu的相关性
        train_group = train_df[(df['media'] == media) & (df['country'] == country)]
        test_group = test_df[(df['media'] == media) & (df['country'] == country)]
        
        if train_group.empty or test_group.empty:
            continue
        
        # 设置日期列为索引
        train_group.set_index('install_day', inplace=True)
        test_group.set_index('install_day', inplace=True)
        
        # 计算训练集的每周季节性强度
        try:
            pu_weekly_result_train = seasonal_decompose(train_group['pu'], model='additive', period=7)
            pu_weekly_seasonal_strength_train = np.var(pu_weekly_result_train.seasonal) / (np.var(pu_weekly_result_train.seasonal) + np.var(pu_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing PU for {media}, {country} (train): {e}')
            pu_weekly_seasonal_strength_train = None
        
        try:
            cpup_weekly_result_train = seasonal_decompose(train_group['cpup'], model='additive', period=7)
            cpup_weekly_seasonal_strength_train = np.var(cpup_weekly_result_train.seasonal) / (np.var(cpup_weekly_result_train.seasonal) + np.var(cpup_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing CPUP for {media}, {country} (train): {e}')
            cpup_weekly_seasonal_strength_train = None
        
        # 计算测试集的每周季节性强度
        try:
            pu_weekly_result_test = seasonal_decompose(test_group['pu'], model='additive', period=7)
            pu_weekly_seasonal_strength_test = np.var(pu_weekly_result_test.seasonal) / (np.var(pu_weekly_result_test.seasonal) + np.var(pu_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing PU for {media}, {country} (test): {e}')
            pu_weekly_seasonal_strength_test = None
        
        try:
            cpup_weekly_result_test = seasonal_decompose(test_group['cpup'], model='additive', period=7)
            cpup_weekly_seasonal_strength_test = np.var(cpup_weekly_result_test.seasonal) / (np.var(cpup_weekly_result_test.seasonal) + np.var(cpup_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing CPUP for {media}, {country} (test): {e}')
            cpup_weekly_seasonal_strength_test = None
        
        ret[(media, country)] = (pu_weekly_seasonal_strength_train, pu_weekly_seasonal_strength_test, cpup_weekly_seasonal_strength_train, cpup_weekly_seasonal_strength_test)
        
        # 打印结果，处理 None 值
        pu_wss_train_str = f'{pu_weekly_seasonal_strength_train:.4f}' if pu_weekly_seasonal_strength_train is not None else 'N/A'
        pu_wss_test_str = f'{pu_weekly_seasonal_strength_test:.4f}' if pu_weekly_seasonal_strength_test is not None else 'N/A'
        cpup_wss_train_str = f'{cpup_weekly_seasonal_strength_train:.4f}' if cpup_weekly_seasonal_strength_train is not None else 'N/A'
        cpup_wss_test_str = f'{cpup_weekly_seasonal_strength_test:.4f}' if cpup_weekly_seasonal_strength_test is not None else 'N/A'
        
        print(f'PU Weekly Seasonal Strength for {media}, {country} (train): {pu_wss_train_str}')
        print(f'PU Weekly Seasonal Strength for {media}, {country} (test): {pu_wss_test_str}')
        print(f'CPUP Weekly Seasonal Strength for {media}, {country} (train): {cpup_wss_train_str}')
        print(f'CPUP Weekly Seasonal Strength for {media}, {country} (test): {cpup_wss_test_str}')
    
    return ret

def reportPu():
    weeklySeasonalStrengthPuCpup = getWeeklySeasonalStrengthPuCpup()
    corrRet = getCorrPu()
    prophetRet = prophetPuTest()
    dnnRet = dnnPuTest()

    # 生成报告
    report_data = []
    for key in corrRet.keys():
        media, country = key
        corr_train, corr_test = corrRet[key]
        prophet_mape = prophetRet.get(key, np.nan)
        dnn_mape = dnnRet.get(key, np.nan)
        pu_wss_train, pu_wss_test, cpup_wss_train, cpup_wss_test = weeklySeasonalStrengthPuCpup.get(key, (np.nan, np.nan, np.nan, np.nan))
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape, pu_wss_train, pu_wss_test, cpup_wss_train, cpup_wss_test])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape', 'pu_wss_train', 'pu_wss_test', 'cpup_wss_train', 'cpup_wss_test'])
    report_df.to_csv('/src/data/20241126report_pu.csv', index=False)
    print("Report generated and saved to /src/data/20241126report_pu.csv")

def calculate_pct_change(df, column):
    df = df.copy()
    df[f'{column}_pct'] = df[column].pct_change()
    df = df.dropna().reset_index(drop=True)
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    return df

def getCorrPuPct():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    ret = {}
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        print('\n>>', media, country)
        # 计算cost_pct和pu_pct
        group = calculate_pct_change(group, 'cost')
        group = calculate_pct_change(group, 'pu')

        # 重新划分训练集和测试集
        train_group = group[(group['install_day'] >= '2024-07-01') & (group['install_day'] <= '2024-08-31')]
        test_group = group[(group['install_day'] >= '2024-09-01') & (group['install_day'] <= '2024-09-30')]
        
        if not train_group.empty and not test_group.empty:
            trainCorr = train_group.corr()
            testCorr = test_group.corr()
            ret[(media, country)] = (trainCorr['cost_pct']['pu_pct'], testCorr['cost_pct']['pu_pct'])
        else:
            ret[(media, country)] = (np.nan, np.nan)

    print(ret)
    return ret

def prophetPuTestPct():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    ret = {}
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # 计算cost_pct和pu_pct
        group = calculate_pct_change(group, 'cost')
        group = calculate_pct_change(group, 'pu')
        
        # 重新划分训练集和测试集
        train_group = group[(group['install_day'] >= '2024-07-01') & (group['install_day'] <= '2024-08-31')]
        test_group = group[(group['install_day'] >= '2024-09-01') & (group['install_day'] <= '2024-09-30')]
        
        if train_group.empty or test_group.empty:
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data.')
            continue
        
        # 准备训练数据
        train_data = train_group[['install_day', 'cost_pct', 'pu_pct']].rename(columns={'install_day': 'ds', 'pu_pct': 'y'})
        
        # 初始化并训练Prophet模型
        model = Prophet()
        model.add_regressor('cost_pct')
        model.fit(train_data)
        
        # 准备测试数据
        test_data = test_group[['install_day', 'cost_pct', 'pu_pct']].rename(columns={'install_day': 'ds'})

        # 进行预测
        forecast = model.predict(test_data)

        retDf = pd.merge(test_data, forecast[['ds', 'yhat']], on='ds', how='left')
        retDf['mape'] = np.abs((1 + retDf['pu_pct']) - (1 + retDf['yhat'])) / (1 + retDf['pu_pct'])
        retDf.to_csv(f'/src/data/20241126_prophet_pu_pct_{media}_{country}.csv', index=False)

        # 计算MAPE
        mape = retDf['mape'].mean()
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def dnnPuTestPct():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)

    ret = {}
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # 计算cost_pct和pu_pct
        group = calculate_pct_change(group, 'cost')
        group = calculate_pct_change(group, 'pu')
        
        # 重新划分训练集和测试集
        train_group = group[(group['install_day'] >= '2024-07-01') & (group['install_day'] <= '2024-08-31')]
        test_group = group[(group['install_day'] >= '2024-09-01') & (group['install_day'] <= '2024-09-30')]
        
        if train_group.empty or test_group.empty:
            print(f'Skipping Media: {media}, Country: {country} due to insufficient data.')
            continue
        
        # 准备训练数据
        X_train = train_group[['cost_pct']].values
        y_train = train_group['pu_pct'].values
        
        # 标准化
        scaler_X = StandardScaler()
        X_train_scaled = scaler_X.fit_transform(X_train)
        
        # 准备测试数据
        X_test = test_group[['cost_pct']].values
        y_test = test_group['pu_pct'].values
        
        # 标准化测试数据
        X_test_scaled = scaler_X.transform(X_test)

        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mape'])
        
        # 添加 Early Stopping 回调
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=5000, batch_size=4, verbose=1, validation_data=(X_test_scaled, y_test), callbacks=[early_stopping])

        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(1 + y_test, 1 + y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def getWeeklySeasonalStrengthPuCpupPct():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)

    ret = {}
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # 计算cost_pct和pu_pct
        group = calculate_pct_change(group, 'cost')
        group = calculate_pct_change(group, 'pu')
        group['cpup_pct'] = group['cost_pct'] / group['pu_pct']

        # 重新划分训练集和测试集
        train_group = group[(group['install_day'] >= '2024-07-01') & (group['install_day'] <= '2024-08-31')]
        test_group = group[(group['install_day'] >= '2024-09-01') & (group['install_day'] <= '2024-09-30')]
        print(train_group)
        return
        if train_group.empty or test_group.empty:
            continue
        
        # 设置日期列为索引
        train_group.set_index('install_day', inplace=True)
        test_group.set_index('install_day', inplace=True)
        
        # 计算训练集的每周季节性强度
        try:
            pu_weekly_result_train = seasonal_decompose(train_group['pu_pct'], model='additive', period=7)
            pu_weekly_seasonal_strength_train = np.var(pu_weekly_result_train.seasonal) / (np.var(pu_weekly_result_train.seasonal) + np.var(pu_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing PU for {media}, {country} (train): {e}')
            pu_weekly_seasonal_strength_train = None
        
        try:
            cpup_weekly_result_train = seasonal_decompose(train_group['cpup_pct'], model='additive', period=7)
            cpup_weekly_seasonal_strength_train = np.var(cpup_weekly_result_train.seasonal) / (np.var(cpup_weekly_result_train.seasonal) + np.var(cpup_weekly_result_train.resid))
        except Exception as e:
            print(f'Error processing CPUP for {media}, {country} (train): {e}')
            cpup_weekly_seasonal_strength_train = None

        # 计算测试集的每周季节性强度
        try:
            pu_weekly_result_test = seasonal_decompose(test_group['pu_pct'], model='additive', period=7)
            pu_weekly_seasonal_strength_test = np.var(pu_weekly_result_test.seasonal) / (np.var(pu_weekly_result_test.seasonal) + np.var(pu_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing PU for {media}, {country} (test): {e}')
            pu_weekly_seasonal_strength_test = None
        
        try:
            cpup_weekly_result_test = seasonal_decompose(test_group['cpup_pct'], model='additive', period=7)
            cpup_weekly_seasonal_strength_test = np.var(cpup_weekly_result_test.seasonal) / (np.var(cpup_weekly_result_test.seasonal) + np.var(cpup_weekly_result_test.resid))
        except Exception as e:
            print(f'Error processing CPUP for {media}, {country} (test): {e}')
            cpup_weekly_seasonal_strength_test = None
        
        ret[(media, country)] = (pu_weekly_seasonal_strength_train, pu_weekly_seasonal_strength_test, cpup_weekly_seasonal_strength_train, cpup_weekly_seasonal_strength_test)
        
        # 打印结果，处理 None 值
        pu_wss_train_str = f'{pu_weekly_seasonal_strength_train:.4f}' if pu_weekly_seasonal_strength_train is not None else 'N/A'
        pu_wss_test_str = f'{pu_weekly_seasonal_strength_test:.4f}' if pu_weekly_seasonal_strength_test is not None else 'N/A'
        cpup_wss_train_str = f'{cpup_weekly_seasonal_strength_train:.4f}' if cpup_weekly_seasonal_strength_train is not None else 'N/A'
        cpup_wss_test_str = f'{cpup_weekly_seasonal_strength_test:.4f}' if cpup_weekly_seasonal_strength_test is not None else 'N/A'
        
        print(f'PU_pct Weekly Seasonal Strength for {media}, {country} (train): {pu_wss_train_str}')
        print(f'PU_pct Weekly Seasonal Strength for {media}, {country} (test): {pu_wss_test_str}')
        print(f'CPUP_pct Weekly Seasonal Strength for {media}, {country} (train): {cpup_wss_train_str}')
        print(f'CPUP_pct Weekly Seasonal Strength for {media}, {country} (test): {cpup_wss_test_str}')
    
    return ret
        

def reportPuPct():
    weeklySeasonalStrengthPuCpupPct = getWeeklySeasonalStrengthPuCpupPct()
    corrRet = getCorrPuPct()
    prophetRet = prophetPuTestPct()
    dnnRet = dnnPuTestPct()

    # 生成报告
    report_data = []
    for key in corrRet.keys():
        media, country = key
        corr_train, corr_test = corrRet[key]
        prophet_mape = prophetRet.get(key, np.nan)
        dnn_mape = dnnRet.get(key, np.nan)
        pu_wss_train, pu_wss_test, cpup_wss_train, cpup_wss_test = weeklySeasonalStrengthPuCpupPct.get(key, (np.nan, np.nan, np.nan, np.nan))
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape, pu_wss_train, pu_wss_test, cpup_wss_train, cpup_wss_test])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape', 'puPct_wss_train', 'puPct_wss_test', 'cpupPct_wss_train', 'cpupPct_wss_test'])
    report_df.to_csv('/src/data/20241126report_pu_pct.csv', index=False)
    print("Report generated and saved to /src/data/20241126report_pu_pct.csv")


if __name__ == '__main__':
    report()
    # reportPu()
    # reportPuPct()

    
    
    