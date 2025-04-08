# 针对2024-10-16 之后进行训练
# 与e的不同之处主要在参数改变

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
from prophet.diagnostics import cross_validation, performance_metrics
    

def getData():
    df = pd.read_csv('lastwar_分服流水每天_20240101_20250217.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "服务器ID" -> "server_id", "S新支付.美元付费金额 - USD(每日汇率)总和" -> "revenue"
    df = df.rename(columns={
        '时间': 'day', 
        '服务器ID': 'server_id', 
        'S新支付.美元付费金额 - USD(每日汇率)总和': 'revenue'
    })

    # 将 服务器ID 为 空 的行删除
    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    # 将服务器ID转换为整数，无法转换的直接扔掉
    def convert_server_id(server_id):
        try:
            return int(server_id[3:])
        except:
            return np.nan

    df['server_id_int'] = df['server_id'].apply(convert_server_id)
    df = df.dropna(subset=['server_id_int'])
    df['server_id_int'] = df['server_id_int'].astype(int)

    # 服务器ID 只统计到 'APS1188' 服务器
    df = df[df['server_id_int'] <= 1188]

    # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    
    return df

def func1():
    df = getData()
    df = df[df['day'] >= '2024-10-16']

    df0 = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]

    df0 = df0[['day', 'revenue','server_id_int']]
    df0 = df0.rename(columns={'day':'ds','revenue':'y','server_id_int':'server_id'})
    df0['cap'] = 1e5
    df0['floor'] = 0

    # 拟合 Prophet 模型，捕捉通用季节性
    model0 = Prophet(
        growth='logistic',
        daily_seasonality=True, 
        weekly_seasonality=True, 
        yearly_seasonality=True,
        changepoint_prior_scale=0.05,  # 新增防过拟合参数
        seasonality_prior_scale=0.05,    # 新增防过拟合参数
        holidays_prior_scale=0.1,       # 新增防过拟合参数
    )
    model0.fit(df0)

    df_cv = cross_validation(
        model0,
        initial='90 days',   # 用3个月数据作为初始训练
        period='30 days',    # 每月扩展一次训练集
        horizon='30 days',   # 预测未来1个月
        parallel="processes"
    )
    df_p = performance_metrics(df_cv)
    print(f"CV Metrics:")
    # print(df_p[['horizon', 'rmse', 'mape']].head())
    print(df_p)

    # 提取通用季节性
    future = model0.make_future_dataframe(periods=0)
    future['cap'] = 1e5
    future['floor'] = 0
    forecast = model0.predict(future)
    forecast.to_csv("/src/data/20250220_forecast0.csv", index=False)
    
    seasonal = forecast[['ds', 'yearly', 'weekly', 'daily']]
    seasonal = seasonal.rename(columns={'yearly':'yearly0','weekly':'weekly0','daily':'daily0'})
    # 将seasonal中的NaN替换为0
    seasonal = seasonal.fillna(0)

    results = []

    for server_id in range(3, 37):
        # for test
        if server_id != 10:
            continue

        server_data = df0[df0['server_id'] == server_id].sort_values('ds').reset_index(drop=True)
        
        if server_data.empty:
            continue
        
        # 检查最近4周的收入和是否小于10美元
        if server_data['y'].tail(28).sum() < 10:
            continue
        
        # 将通用季节性加入到每个服务器的数据中
        server_data = server_data.merge(seasonal, on='ds', how='left')

        # 再次检查并填充 NaN 值
        server_data = server_data.fillna(0)
        server_data['cap'] = 1e5
        server_data['floor'] = 0

        # 训练单独的 Prophet 模型
        # model = Prophet(growth='logistic',yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)
        model = Prophet(growth='logistic')
        model.add_regressor('yearly0')
        model.add_regressor('weekly0')
        model.add_regressor('daily0')
        model.fit(server_data)
        
        # 预测未来
        future = model.make_future_dataframe(periods=90)
        future['cap'] = 1e5
        future['floor'] = 0
        future_seasonal = model0.predict(future)
        future_seasonal = future_seasonal[['ds', 'yearly', 'weekly', 'daily']]
        future_seasonal.rename(columns={'yearly':'yearly0','weekly':'weekly0','daily':'daily0'}, inplace=True)
        future = future.merge(future_seasonal, on='ds', how='left')
        forecast = model.predict(future)
        forecast.to_csv(f"/src/data/20250220_forecast_{server_id}.csv", index=False)
        
        # 计算去除季节性后的趋势
        server_data['yhat'] = model.predict(server_data)['yhat']
        server_data['trend'] = server_data['yhat'] - server_data['yearly0'] - server_data['weekly0'] - server_data['daily0']
        
        # 计算预测部分的趋势
        forecast['trend'] = forecast['yhat'] - forecast['yearly0'] - forecast['weekly0'] - forecast['daily0']
        
        # 保存结果
        results.append({
            'server_id': server_id,
            'forecast': forecast,
            'trend': server_data[['ds', 'trend']],
            'actual': server_data[['ds', 'y']],
            'forecast_trend': forecast[['ds', 'trend']]  # 新增的部分
        })
    
    return results

results = func1()
print("func1() done.")
print("results:")
print(results)

# 分析去除季节性后的趋势和实际收入
for result in results:
    server_id = result['server_id']
    trend = result['trend']
    actual = result['actual']
    forecast = result['forecast']
    forecast_trend = result['forecast_trend']  # 新增的部分
    
    plt.figure(figsize=(10, 6))
    # plt.plot(trend['ds'], trend['trend'], label='Trend (Seasonality Removed)')
    plt.plot(actual['ds'], actual['y'], label='Actual Revenue', alpha=0.6)
    
    # 预测部分
    plt.plot(forecast['ds'], forecast['yhat'], label='Forecasted Revenue', linestyle='--')
    plt.plot(forecast_trend['ds'], forecast_trend['trend'], label='Forecasted Trend (Seasonality Removed)', linestyle='--')  # 新增的部分
    
    # 添加竖线分隔当前数据和预测数据
    plt.axvline(x=actual['ds'].max(), color='g', linestyle='--', label='Prediction Start')
    
    plt.axhline(y=0, color='r', linestyle='--')
    plt.title(f'Trend and Actual Revenue for Server {server_id}')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.legend()
    plt.savefig(f"/src/data/20250220_trend_actual_{server_id}.png")
    print(f"Trend and Actual Revenue for Server {server_id} saved.")
    plt.close()
