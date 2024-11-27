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
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
        # if (media, country) not in [('ALL', 'ALL'),('ALL', 'T1')]:
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
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mape'])
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=500, batch_size=4, verbose=0)
        
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
    corrRet = getCorr()
    prophetRet = prophetTest()
    dnnRet = dnnTest()

    # 生成报告
    # 格式csv
    # 先做一个DataFrame,然后to_csv
    # 列 media,country,corr_train,corr_test,prophet_mape,dnn_mape
    # 生成报告
    report_data = []
    for key in corrRet.keys():
        media, country = key
        corr_train, corr_test = corrRet[key]
        prophet_mape = prophetRet.get(key, np.nan)
        dnn_mape = dnnRet.get(key, np.nan)
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape'])
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
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mape'])
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=500, batch_size=4, verbose=0)
        
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
        
        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(y_test, y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def reportPu():
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
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape'])
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
        
        # 构建DNN模型
        model = Sequential()
        model.add(Dense(32, input_dim=X_train_scaled.shape[1], activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1, activation='linear'))
        
        # 编译模型
        model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mape'])
        
        # 训练模型
        model.fit(X_train_scaled, y_train, epochs=500, batch_size=4, verbose=0)
        
        # 准备测试数据
        X_test = test_group[['cost_pct']].values
        y_test = test_group['pu_pct'].values
        
        # 标准化测试数据
        X_test_scaled = scaler_X.transform(X_test)
        
        # 进行预测
        y_pred = model.predict(X_test_scaled).flatten()
        
        # 计算MAPE
        mape = mean_absolute_percentage_error(1 + y_test, 1 + y_pred)
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)
    return ret

def reportPuPct():
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
        report_data.append([media, country, corr_train, corr_test, prophet_mape, dnn_mape])

    report_df = pd.DataFrame(report_data, columns=['media', 'country', 'corr_train', 'corr_test', 'prophet_mape', 'dnn_mape'])
    report_df.to_csv('/src/data/20241126report_pu_pct.csv', index=False)
    print("Report generated and saved to /src/data/20241126report_pu_pct.csv")


if __name__ == '__main__':
    report()
    reportPu()
    reportPuPct()
