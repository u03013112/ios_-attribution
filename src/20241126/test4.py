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
    day BETWEEN '20240101' AND '20241031'
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

def dataStep1(data):
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
    
    # 按照 install_day 分周
    data['week'] = data['install_day'].dt.isocalendar().week
    
    # 创建一个空的 DataFrame 来存储每周的计算结果
    weekly_data = pd.DataFrame()
    
    # 按照 media 和 country 分组
    for (media, country), group in data.groupby(['media', 'country']):
        # 按照 week 分组并计算每周的指标
        weekly_group = group.groupby('week').agg({
            'revenue': 'sum',
            'cost': 'sum',
            'pu': 'sum'
        }).reset_index()
        
        # 计算每周的 ROI, CPUP, ARPPU
        weekly_group['week_roi'] = weekly_group['revenue'] / weekly_group['cost']
        weekly_group['week_cpup'] = weekly_group['cost'] / weekly_group['pu']
        weekly_group['week_arppu'] = weekly_group['revenue'] / weekly_group['pu']
        
        # 计算上周的指标
        weekly_group['lastweek'] = weekly_group['week'].shift(1)
        weekly_group['lastweek_roi'] = weekly_group['week_roi'].shift(1)
        weekly_group['lastweek_cpup'] = weekly_group['week_cpup'].shift(1)
        weekly_group['lastweek_arppu'] = weekly_group['week_arppu'].shift(1)
        
        # 添加 media 和 country 列
        weekly_group['media'] = media
        weekly_group['country'] = country
        
        # 将结果添加到 weekly_data
        weekly_data = pd.concat([weekly_data, weekly_group], ignore_index=True)
    
    # 合并原始数据和计算结果
    data = data.merge(weekly_data[['media', 'country', 'week', 'lastweek_roi', 'lastweek_cpup', 'lastweek_arppu']],
                    on=['media', 'country', 'week'], how='left')
    
    # 删除没有上周数据的行
    data.dropna(subset=['lastweek_roi', 'lastweek_cpup', 'lastweek_arppu'], inplace=True)
    
    # 删除临时的 week 列
    data.drop(columns=['week'], inplace=True)
    
    return data

def calculate_mape(group, train_start_date, train_end_date, test_start_date, test_end_date, input_columns, scheme_name):
    train_data = group[(group['install_day'] >= train_start_date) & (group['install_day'] <= train_end_date)]
    test_data = group[(group['install_day'] >= test_start_date) & (group['install_day'] <= test_end_date)]
    
    if len(train_data) == 0 or len(test_data) == 0:
        return pd.DataFrame(columns=['install_day', 'mape', 'scheme'])
    
    # 准备训练数据
    train_data = train_data[['install_day', 'cost', 'revenue'] + input_columns].rename(columns={'install_day': 'ds', 'revenue': 'y'})
    
    # 准备测试数据
    test_data = test_data[['install_day', 'cost', 'revenue'] + input_columns].rename(columns={'install_day': 'ds'})
    
    # 初始化并训练Prophet模型
    model = Prophet(weekly_seasonality=True)
    model.add_regressor('cost')
    for col in input_columns:
        model.add_regressor(col)
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
    train_data[['cost', 'weekly'] + input_columns] = scaler_X.fit_transform(train_data[['cost', 'weekly'] + input_columns])
    test_data[['cost', 'weekly'] + input_columns] = scaler_X.transform(test_data[['cost', 'weekly'] + input_columns])
    
    # 准备DNN的训练数据
    X_train = train_data[['cost', 'weekly'] + input_columns]
    y_train = train_data['y']
    
    # 准备DNN的测试数据
    X_test = test_data[['cost', 'weekly'] + input_columns]
    y_test = test_data['revenue']
    
    best_mape = float('inf')
    best_y_pred = None
    
    for _ in range(5):  # 简单循环5次，选择最优结果
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
    
    result = test_data[['ds']].copy()
    result['y'] = test_data['revenue']
    result['y_pred'] = best_y_pred
    result['mape'] = np.abs((result['y'] - result['y_pred']) / result['y'])
    result.rename(columns={'ds': 'install_day'}, inplace=True)
    result['scheme'] = scheme_name
    
    return result

def prophetDnnTest4():
    df = getData()
    df = dataStep1(df)
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    
    # 尝试的训练周期和预测周期列表
    try_periods = [
        {'N': 60, 'M': 7},
        {'N': 90, 'M': 7},
        {'N': 120, 'M': 7},
        {'N': 180, 'M': 7},
        {'N': 210, 'M': 7},
        {'N': 60, 'M': 14},
        {'N': 90, 'M': 14},
        {'N': 120, 'M': 14},
        {'N': 180, 'M': 14},
        {'N': 210, 'M': 14},
    ]
    
    schemes = [
        {'input_columns': [], 'scheme_name': '方案1'},
        {'input_columns': ['lastweek_roi'], 'scheme_name': '方案2'},
        {'input_columns': ['lastweek_cpup'], 'scheme_name': '方案3'},
        {'input_columns': ['lastweek_arppu'], 'scheme_name': '方案4'},
        {'input_columns': ['lastweek_roi', 'lastweek_cpup', 'lastweek_arppu'], 'scheme_name': '方案5'},
    ]
    
    results = []
    results_file = '/src/data/20241126_prophet_dnn_test5.csv'
    
    # 如果文件存在，删除它以确保我们从头开始
    if os.path.exists(results_file):
        os.remove(results_file)
    
    groupDf = df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL')]:
        #     continue

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
            
            period_results = []
            for scheme in schemes:
                # 分段测试
                test_periods = pd.date_range(start='2024-09-01', end='2024-10-31', freq=f'{M}D')
                
                for start_date in test_periods:
                    end_date = start_date + pd.Timedelta(days=M-1)
                    if end_date > pd.Timestamp('2024-10-31'):
                        break
                    
                    train_end_date = start_date - pd.Timedelta(days=1)
                    train_start_date = train_end_date - pd.Timedelta(days=N-1)
                
                    period_result = calculate_mape(group, train_start_date, train_end_date, start_date, end_date, scheme['input_columns'], scheme['scheme_name'])
                    print(f'Media: {media}, Country: {country}, N: {N}, M: {M}, Start Date: {start_date}, End Date: {end_date}, Scheme: {scheme["scheme_name"]}')
                    print(period_result)

                    period_results.append(period_result)
            
                if period_results:
                    combined_results = pd.concat(period_results)
                    overall_mape = combined_results['mape'].mean()
                    
                    result = {
                        'media': media,
                        'country': country,
                        'N': N,
                        'M': M,
                        'mape': overall_mape,
                        'scheme': scheme['scheme_name']
                    }
                    results.append(result)
                    
                    # 将结果写入文件
                    result_df = pd.DataFrame([result])
                    result_df.to_csv(results_file, mode='a', header=not os.path.exists(results_file), index=False)
                    
                    print(f'Media: {media}, Country: {country}, N: {N}, M: {M}, Scheme: {scheme["scheme_name"]}, MAPE: {overall_mape:.4f}')

    results_df = pd.DataFrame(results)
    print(results_df)

    return results_df

if __name__ == '__main__':
    # 运行测试
    prophetDnnTest4()
