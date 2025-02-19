# 使用累计付费金额
# 预测累计付费金额
# 然后再通过差得到每日付费金额

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score

def getData():
    df = pd.read_csv('lastwar_分服流水每天_20240101_20250217.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "服务器ID" -> "server_id", "S新支付.美元付费金额 - USD(每日汇率)总和" -> "revenue","S新支付.触发用户数" -> "pay_users","S登录.触发用户数" -> "login_users"
    df = df.rename(columns={
        '时间': 'day', 
        '服务器ID': 'server_id', 
        'S新支付.美元付费金额 - USD(每日汇率)总和': 'revenue', 
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
    server_10 = df[df['server_id_int'] == 10].sort_values('day').reset_index(drop=True)
    
    # 使用累计付费金额
    server_10['cum_revenue'] = server_10['revenue'].cumsum()
    
    # 准备数据以适应 Prophet 模型
    prophet_df = server_10[['day', 'cum_revenue']].rename(columns={'day': 'ds', 'cum_revenue': 'y'})
    
    # 拟合 Prophet 模型
    model = Prophet()
    model.fit(prophet_df)
    
    # 生成未来的日期数据
    future = model.make_future_dataframe(periods=30)  # 预测未来30天
    forecast = model.predict(future)
    
    # 计算 R2 分数
    y_true = prophet_df['y']
    y_pred = forecast['yhat'][:len(y_true)]
    r2 = r2_score(y_true, y_pred)
    print(f'R2 Score: {r2}')
    
    # 绘制累计付费金额的预测结果
    plt.figure(figsize=(10, 6))
    plt.plot(server_10['day'], server_10['cum_revenue'], label='Actual Cumulative Revenue')
    plt.plot(forecast['ds'], forecast['yhat'], label='Predicted Cumulative Revenue')
    plt.fill_between(forecast['ds'], forecast['yhat_lower'], forecast['yhat_upper'], color='gray', alpha=0.2)
    plt.legend()
    plt.title('Cumulative Revenue Prediction')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Revenue')
    plt.savefig('/src/data/20250219_prophet1_cum_revenue.png')
    plt.close()
    
    # 计算每日收入的预测结果
    forecast['daily_revenue'] = forecast['yhat'].diff()
    forecast['daily_revenue_lower'] = forecast['yhat_lower'].diff()
    forecast['daily_revenue_upper'] = forecast['yhat_upper'].diff()
    
    # 去除第一个 NaN 值
    forecast = forecast.dropna(subset=['daily_revenue'])
    
    # 计算每日收入的 R2 分数
    y_true = server_10['revenue']
    y_pred = forecast['daily_revenue'][:len(y_true)]
    r2 = r2_score(y_true, y_pred)
    print(f'Daily Revenue R2 Score: {r2}')

    # 绘制每日收入的预测结果
    plt.figure(figsize=(10, 6))
    plt.plot(server_10['day'], server_10['revenue'], label='Actual Daily Revenue')
    plt.plot(forecast['ds'], forecast['daily_revenue'], label='Predicted Daily Revenue')
    plt.fill_between(forecast['ds'], forecast['daily_revenue_lower'], forecast['daily_revenue_upper'], color='gray', alpha=0.2)
    plt.legend()
    plt.title('Daily Revenue Prediction')
    plt.xlabel('Date')
    plt.ylabel('Daily Revenue')
    plt.savefig('/src/data/20250219_prophet1_daily_revenue.png')
    plt.close()

if __name__ == '__main__':
    func1()