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

    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    df = df.rename(columns={
        '时间': 'day', 
        '服务器ID': 'server_id', 
        'S新支付.美元付费金额 - USD(每日汇率)总和': 'revenue'
    })

    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    def convert_server_id(server_id):
        try:
            return int(server_id[3:])
        except:
            return np.nan

    df['server_id_int'] = df['server_id'].apply(convert_server_id)
    df = df.dropna(subset=['server_id_int'])
    df['server_id_int'] = df['server_id_int'].astype(int)

    df = df[df['server_id_int'] <= 1188]

    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    
    return df

def func1():
    df = getData()
    df = df[df['day'] >= '2024-10-16']

    df0 = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]
    df0 = df0[['day', 'revenue', 'server_id_int']]
    df0 = df0.rename(columns={'day': 'ds', 'revenue': 'y', 'server_id_int': 'server_id'})
    df0['cap'] = 1e5
    df0['floor'] = 0

    # 汇总数据
    aggregated_data = df0.groupby('ds').agg({'y': 'mean'}).reset_index()
    aggregated_data['cap'] = 1e5
    aggregated_data['floor'] = 0

    # 优化全局模型参数并训练模型
    changepoint_prior_scales = [0.1, 0.5, 0.8, 1.0]
    seasonality_prior_scales = [0.1, 0.5, 0.8, 1.0]
    best_params_global = None
    best_coverage_global = float('inf')
    best_model_global = None

    for cps in changepoint_prior_scales:
        for sps in seasonality_prior_scales:
            model = Prophet(
                growth='logistic',
                daily_seasonality=True, 
                weekly_seasonality=True, 
                yearly_seasonality=True,
                changepoint_prior_scale=cps,
                seasonality_prior_scale=sps
            )
            model.fit(aggregated_data)

            df_cv = cross_validation(
                model,
                initial='60 days',
                period='30 days',
                horizon='60 days',
                parallel="processes"
            )
            df_p = performance_metrics(df_cv)
            # print(df_p)
            
            coverage = df_p['coverage'].mean()

            if coverage < best_coverage_global:
                best_coverage_global = coverage
                best_params_global = (cps, sps)
                best_model_global = model

    print(f"Global best params: {best_params_global}, coverage: {best_coverage_global}")
    # 保存全局最优参数到CSV
    global_params_df = pd.DataFrame([{
        'type': 'global',
        'changepoint_prior_scale': best_params_global[0],
        'seasonality_prior_scale': best_params_global[1],
        'coverage': best_coverage_global
    }])
    global_params_df.to_csv('/src/data/20250220_e2_global_best_params.csv', index=False)
    future = best_model_global.make_future_dataframe(periods=0)
    future['cap'] = 1e5
    future['floor'] = 0
    forecast_global = best_model_global.predict(future)
    forecast_global.to_csv("/src/data/20250220_forecast0.csv", index=False)

    seasonal = forecast_global[['ds', 'yearly', 'weekly', 'daily']]
    seasonal = seasonal.rename(columns={'yearly': 'yearly0', 'weekly': 'weekly0', 'daily': 'daily0'})
    seasonal = seasonal.fillna(0)

    results = []

    for server_id in range(3, 37):
        # for test
        if server_id != 10:
            continue

        server_data = df0[df0['server_id'] == server_id].sort_values('ds').reset_index(drop=True)
        
        if server_data.empty or server_data['y'].tail(28).sum() < 10:
            continue
        
        server_data = server_data.merge(seasonal, on='ds', how='left')
        server_data = server_data.fillna(0)
        server_data['cap'] = 1e5
        server_data['floor'] = 0

        # 优化单独服务器模型参数并训练模型
        best_params_server = None
        best_coverage_server = float('inf')
        best_model_server = None

        changepoint_prior_scales2 = [0.03, 0.05, 0.08, 0.1]
        seasonality_prior_scales2 = [0.03, 0.05, 0.08, 0.1]
        for cps in changepoint_prior_scales2:
            for sps in seasonality_prior_scales2:
                model = Prophet(
                    growth='logistic',
                    changepoint_prior_scale=cps,
                    seasonality_prior_scale=sps
                )
                model.add_regressor('yearly0')
                model.add_regressor('weekly0')
                model.add_regressor('daily0')
                model.fit(server_data)

                df_cv = cross_validation(
                    model,
                    initial='60 days',
                    period='30 days',
                    horizon='60 days',
                    parallel="processes"
                )
                df_p = performance_metrics(df_cv)
                coverage = df_p['coverage'].mean()

                if coverage < best_coverage_server:
                    best_coverage_server = coverage
                    best_params_server = (cps, sps)
                    best_model_server = model

        print(f"Server {server_id} best params: {best_params_server}, coverage: {best_coverage_server}")
        # 保存服务器最优参数到CSV
        server_params_df = pd.DataFrame([{
            'type': f'server_{server_id}',
            'changepoint_prior_scale': best_params_server[0],
            'seasonality_prior_scale': best_params_server[1],
            'coverage': best_coverage_server
        }])
        server_params_df.to_csv(f'/src/data/20250220_e2_server_{server_id}_best_params.csv', index=False)
        future = best_model_server.make_future_dataframe(periods=60)
        future['cap'] = 1e5
        future['floor'] = 0
        future_seasonal = best_model_global.predict(future)
        future_seasonal = future_seasonal[['ds', 'yearly', 'weekly', 'daily']]
        future_seasonal.rename(columns={'yearly': 'yearly0', 'weekly': 'weekly0', 'daily': 'daily0'}, inplace=True)
        future = future.merge(future_seasonal, on='ds', how='left')
        forecast_server = best_model_server.predict(future)
        forecast_server.to_csv(f"/src/data/20250220_forecast_{server_id}.csv", index=False)
        
        server_data['yhat'] = best_model_server.predict(server_data)['yhat']
        server_data['trend'] = server_data['yhat'] - server_data['yearly0'] - server_data['weekly0'] - server_data['daily0']
        
        forecast_server['trend'] = forecast_server['yhat'] - forecast_server['yearly0'] - forecast_server['weekly0'] - forecast_server['daily0']
        
        results.append({
            'server_id': server_id,
            'forecast': forecast_server,
            'trend': server_data[['ds', 'trend']],
            'actual': server_data[['ds', 'y']],
            'forecast_trend': forecast_server[['ds', 'trend']]
        })
    
    return results

results = func1()
print("func1() done.")
input("按任意键继续...")
print("results:")
print(results)

# 分析去除季节性后的趋势和实际收入
for result in results:
    server_id = result['server_id']
    trend = result['trend']
    actual = result['actual']
    forecast = result['forecast']
    forecast_trend = result['forecast_trend']
    
    plt.figure(figsize=(10, 6))
    plt.plot(actual['ds'], actual['y'], label='Actual Revenue', alpha=0.6)
    plt.plot(forecast['ds'], forecast['yhat'], label='Forecasted Revenue', linestyle='--')
    plt.plot(forecast_trend['ds'], forecast_trend['trend'], label='Forecasted Trend (Seasonality Removed)', linestyle='--')
    plt.axvline(x=actual['ds'].max(), color='g', linestyle='--', label='Prediction Start')
    plt.axhline(y=0, color='r', linestyle='--')
    plt.title(f'Trend and Actual Revenue for Server {server_id}')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.legend()
    plt.savefig(f"/src/data/20250220_trend_actual_{server_id}.png")
    print(f"Trend and Actual Revenue for Server {server_id} saved.")
    plt.close()
