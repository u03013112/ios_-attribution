import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from neuralprophet import NeuralProphet
from sklearn.metrics import r2_score
import torch

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

def custom_loss(y_true, y_pred):
    # 添加一个惩罚项，确保预测值大于0
    penalty = torch.sum(torch.abs(y_pred[y_pred < 0])) * 100000  # 增加惩罚力度
    return torch.mean((y_true - y_pred) ** 2) + penalty

def func1():
    df = getData()

    # 按服务器分组并计算ewm
    df = df.sort_values(by=['server_id_int', 'day'])
    # df['revenue'] = df.groupby('server_id_int')['revenue'].transform(lambda x: x.ewm(span=7, adjust=False).mean())

    # 从一个周一开始
    df = df[df['day'] >= '2024-10-21']

    df0 = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]

    df0 = df0[['day', 'revenue','server_id_int']]
    df0 = df0.rename(columns={'day':'ds','revenue':'y','server_id_int':'server_id'})

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
        
        # 训练 NeuralProphet 模型
        model = NeuralProphet(
            n_lags=28,
            n_forecasts=28,
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            learning_rate=0.005, 
            loss_func=custom_loss
        )
        
        # 只保留 'ds' 和 'y' 列
        server_data_for_model = server_data[['ds', 'y']]
        
        trainDf = server_data_for_model[server_data_for_model['ds'] < '2024-12-30']
        testDf = server_data_for_model[server_data_for_model['ds'] >= '2024-12-30']

        metrics = model.fit(trainDf, freq='D')
        
        # # 预测未来
        # future = model.make_future_dataframe(server_data_for_model, periods=28, n_historic_predictions=True)
        # forecast = model.predict(future)
        # forecast.to_csv(f"/src/data/20250220_forecast_{server_id}.csv", index=False)
        
        # # 计算 R² 值
        # y_true = server_data['y'].values
        # y_pred = []
        # y_pred_dates = []


        # 预测1月份的数据，预测从周一开始，所以稍微调整一下 2024-12-30~2025-01-26
        future = model.make_future_dataframe(trainDf, periods=28, n_historic_predictions=False)
        print(f"future:")
        print(future)

        forecast = model.predict(future)
        print(f"forecast:")
        print(forecast)

        future.to_csv(f"/src/data/20250220_debug_future_{server_id}.csv", index=False)
        forecast.to_csv(f"/src/data/20250220_debug_forecast_{server_id}.csv", index=False)

        y_pred = []
        y_pred_dates = []

        for j in range(28):
            y_pred.append(forecast.iloc[28+j][f'yhat{j+1}'])
            y_pred_dates.append(forecast.iloc[28+j]['ds'])


        y_true = testDf[(testDf['ds'] <= '2025-01-26')]['y'].values
        y_pred = np.array(y_pred).astype(np.float64)
        print('y_true:')
        print(y_true)
        print('y_pred:')
        print(y_pred)

        # 计算 R² 值
        r2 = r2_score(y_true, y_pred)
        
        print(f"r2: {r2}")
        # 计算mape
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        print(f"mape: {mape}")

        totalMape = np.abs((y_true.sum() - y_pred.sum()) / y_true.sum()) * 100
        print(f"totalMape: {totalMape}")

        # 画图
        plt.figure(figsize=(10, 6))
        plt.plot(testDf['ds'], testDf['y'], label='Actual Revenue', alpha=0.6)
        plt.plot(y_pred_dates, y_pred, label='Predicted Revenue', linestyle='--')
        plt.title(f'Actual and Forecasted Revenue for Server {server_id} (R²={r2:.2f})')
        plt.xlabel('Date')
        plt.ylabel('Revenue')
        plt.legend()
        plt.savefig(f"/src/data/20250220_actual_forecast_{server_id}.png")
        plt.close()
        print(f"Actual and Forecasted Revenue for Server {server_id} saved.")





#         # 分段预测
#         for i in range(0, len(server_data) - 28, 28):
#             segment = server_data_for_model.iloc[:i+28]
#             future_segment = model.make_future_dataframe(segment, periods=28, n_historic_predictions=False)
#             forecast_segment = model.predict(future_segment)
            
#             for j in range(28):
#                 y_pred.append(forecast_segment.iloc[28+j][f'yhat{j+1}'])
#                 y_pred_dates.append(forecast_segment.iloc[28+j]['ds'])

#         # 将 y_pred 转换为浮点数类型
#         y_pred = np.array(y_pred).astype(np.float64)
        
#         # 检查并处理 NaN 和无穷大值
#         if np.any(np.isnan(y_pred)) or np.any(np.isinf(y_pred)):
#             y_pred = np.nan_to_num(y_pred, nan=0.0, posinf=0.0, neginf=0.0)
        
#         # 确保 y_true 和 y_pred 的长度一致
#         min_length = min(len(y_true), len(y_pred))
#         y_true = y_true[:min_length]
#         y_pred = y_pred[:min_length]
#         y_pred_dates = y_pred_dates[:min_length]  # 确保 y_pred_dates 的长度一致
        
#         r2 = r2_score(y_true, y_pred)
#         # print('y_pred:')
#         # print(y_pred)

#         # 保存结果
#         results.append({
#             'server_id': server_id,
#             'forecast': forecast,
#             'actual': server_data[['ds', 'y']],
#             'y_pred': y_pred,
#             'y_pred_dates': y_pred_dates,
#             'r2': r2
#         })
    
#     return results

results = func1()
print("func1() done.")
# # print("results:")
# # print(results)

# # 分析实际收入和预测收入
# for result in results:
#     server_id = result['server_id']
#     actual = result['actual']
#     forecast = result['forecast']
#     y_pred = result['y_pred']
#     y_pred_dates = result['y_pred_dates']
#     r2 = result['r2']
    
#     plt.figure(figsize=(10, 6))
#     plt.plot(actual['ds'], actual['y'], label='Actual Revenue', alpha=0.6)
    
#     # 预测部分
#     plt.plot(forecast['ds'], forecast['yhat1'], label='Forecasted Revenue', linestyle='--')
    
#     # 预测的28天结果
#     plt.plot(y_pred_dates, y_pred, label='Forecasted Revenue (28 days)', linestyle='-.')
    
#     # 添加竖线分隔当前数据和预测数据
#     plt.axvline(x=actual['ds'].max(), color='g', linestyle='--', label='Prediction Start')
    
#     plt.axhline(y=0, color='r', linestyle='--')
#     plt.title(f'Actual and Forecasted Revenue for Server {server_id} (R²={r2:.2f})')
#     plt.xlabel('Date')
#     plt.ylabel('Revenue')
#     plt.legend()
#     plt.savefig(f"/src/data/20250220_actual_forecast_{server_id}.png")
#     print(f"Actual and Forecasted Revenue for Server {server_id} saved.")
#     plt.close()
