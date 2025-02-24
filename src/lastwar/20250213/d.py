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
    df = df[df['day'] >= '2024-10-16']
    results = []

    for server_id in range(3, 37):
        server_data = df[df['server_id_int'] == server_id].sort_values('day').reset_index(drop=True)
        
        if server_data.empty:
            continue
        
        # 检查最近4周的收入和是否小于10美元
        if server_data['revenue'].tail(28).sum() < 10:
            continue
        
        # 使用累计付费金额
        server_data['cum_revenue'] = server_data['revenue'].cumsum()
        
        # 准备数据以适应 Prophet 模型
        prophet_df = server_data[['day', 'cum_revenue']].rename(columns={'day': 'ds', 'cum_revenue': 'y'})
        
        # 拟合 Prophet 模型
        model = Prophet()
        model.fit(prophet_df)
        
        # 生成未来的日期数据
        future = model.make_future_dataframe(periods=90)  # 预测未来30天
        forecast = model.predict(future)
        
        # 计算 R2 分数
        y_true = prophet_df['y']
        y_pred = forecast['yhat'][:len(y_true)]
        r2 = r2_score(y_true, y_pred)
        
        # 计算每日收入的预测结果
        forecast['daily_revenue'] = forecast['yhat'].diff()
        forecast['daily_revenue_lower'] = forecast['yhat_lower'].diff()
        forecast['daily_revenue_upper'] = forecast['yhat_upper'].diff()
        
        # 去除第一个 NaN 值
        forecast = forecast.dropna(subset=['daily_revenue'])

        # 计算每日收入中，已有数据最后4周的最大收入金额与最小收入金额，计算差值作为置信宽度
        # 后面判断是否低于10美元和20美元时使用 预测收入金额 - 置信宽度 、 预测收入金额 + 置信宽度 与 10/20 美元的大小关系
        # 得出低于10美元和20美元的日期的区间，最小日期，最大与日期
        last_28_days = server_data['revenue'].tail(28)
        min_revenue = last_28_days.min()
        max_revenue = last_28_days.max()
        confidence_width = (max_revenue - min_revenue) / 4
        forecast['daily_revenue_lower'] = forecast['daily_revenue'] - confidence_width
        forecast['daily_revenue_upper'] = forecast['daily_revenue'] + confidence_width
        
        # 计算每日收入的 R2 分数
        y_true_daily = server_data['revenue']
        y_pred_daily = forecast['daily_revenue'][:len(y_true_daily)]
        r2_daily = r2_score(y_true_daily, y_pred_daily)
        
        # 找到每日收入低于10美元和20美元的日期（从预测数据开始）
        forecast_future = forecast[len(y_true_daily):]
        below_10_min = forecast_future[forecast_future['daily_revenue'] - confidence_width < 10]['ds'].min()
        below_10_max = forecast_future[forecast_future['daily_revenue'] + confidence_width < 10]['ds'].max()
        below_20_min = forecast_future[forecast_future['daily_revenue'] - confidence_width < 20]['ds'].min()
        below_20_max = forecast_future[forecast_future['daily_revenue'] + confidence_width < 20]['ds'].max()
        
        # 记录结果
        results.append({
            'server_id': server_id,
            'below_10_usd_date_min': below_10_min,
            'below_10_usd_date_max': below_10_max,
            'below_20_usd_date_min': below_20_min,
            'below_20_usd_date_max': below_20_max,
            'r2': r2,
            'r2_daily': r2_daily
        })
        
        # 绘制累计付费金额的预测结果
        plt.figure(figsize=(10, 6))
        plt.plot(server_data['day'], server_data['cum_revenue'], label='Actual Cumulative Revenue')
        plt.plot(forecast['ds'], forecast['yhat'], label='Predicted Cumulative Revenue')
        plt.fill_between(forecast['ds'], forecast['yhat_lower'], forecast['yhat_upper'], color='gray', alpha=0.2)
        plt.axvline(x=server_data['day'].max(), color='r', linestyle='--', label='End of Actual Data')
        plt.legend()
        plt.title(f'Cumulative Revenue Prediction for Server {server_id}')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Revenue')
        plt.savefig(f'/src/data/20250219_prophet1_cum_revenue_server_{server_id}.png')
        plt.close()
        
        # 绘制每日收入的预测结果
        plt.figure(figsize=(10, 6))
        plt.plot(server_data['day'], server_data['revenue'], label='Actual Daily Revenue')
        plt.plot(forecast['ds'], forecast['daily_revenue'], label='Predicted Daily Revenue')
        plt.fill_between(forecast['ds'], forecast['daily_revenue_lower'], forecast['daily_revenue_upper'], color='gray', alpha=0.2)
        plt.axvline(x=server_data['day'].max(), color='r', linestyle='--', label='End of Actual Data')
        plt.axhline(y=10, color='g', linestyle='--', label='10 USD')
        plt.axhline(y=20, color='b', linestyle='--', label='20 USD')
        plt.legend()
        plt.title(f'Daily Revenue Prediction for Server {server_id}')
        plt.xlabel('Date')
        plt.ylabel('Daily Revenue')
        plt.savefig(f'/src/data/20250219_prophet1_daily_revenue_server_{server_id}.png')
        plt.close()

    # 保存结果到CSV文件
    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250219_prophet1_results.csv', index=False)

def func2():
    df = getData()
    df = df[df['day'] >= '2024-10-16']
    results = []

    # 将2025-01-01之后的数据作为测试集
    train_df = df[df['day'] < '2025-01-01']
    test_df = df[df['day'] >= '2025-01-01']

    for server_id in range(3, 37):
        # # for test
        # if server_id != 10:
        #     continue

        server_train_data = train_df[train_df['server_id_int'] == server_id].sort_values('day').reset_index(drop=True)
        server_test_data = test_df[test_df['server_id_int'] == server_id].sort_values('day').reset_index(drop=True)
        
        if server_train_data.empty or server_test_data.empty:
            continue
        
        # 检查最近4周的收入和是否小于10美元
        if server_train_data['revenue'].tail(28).sum() < 10:
            continue
        
        # 使用累计付费金额
        server_train_data['cum_revenue'] = server_train_data['revenue'].cumsum()
        
        # 准备数据以适应 Prophet 模型
        prophet_df = server_train_data[['day', 'cum_revenue']].rename(columns={'day': 'ds', 'cum_revenue': 'y'})
        
        # 拟合 Prophet 模型
        model = Prophet()
        model.fit(prophet_df)
        
        # 生成未来的日期数据
        future = model.make_future_dataframe(periods=len(server_test_data))
        forecast = model.predict(future)
        
        # 计算每日收入的预测结果
        forecast['daily_revenue'] = forecast['yhat'].diff()
        forecast = forecast.dropna(subset=['daily_revenue'])
        
        # 提取测试集预测结果
        test_forecast = forecast[forecast['ds'] >= '2025-01-01']
        
        # 计算 MAPE
        y_true_daily = server_test_data['revenue'].values
        y_pred_daily = test_forecast['daily_revenue'].values[:len(y_true_daily)]
        mape_daily = mean_absolute_percentage_error(y_true_daily, y_pred_daily)
        
        # 按周计算 MAPE
        server_test_data['week'] = server_test_data['day'].dt.isocalendar().week
        weekly_true = server_test_data.groupby('week')['revenue'].sum()
        weekly_pred = test_forecast.groupby(test_forecast['ds'].dt.isocalendar().week)['daily_revenue'].sum()
        mape_weekly = mean_absolute_percentage_error(weekly_true, weekly_pred[:len(weekly_true)])
        
        # 按月计算 MAPE
        server_test_data['month'] = server_test_data['day'].dt.month
        monthly_true = server_test_data.groupby('month')['revenue'].sum()
        monthly_pred = test_forecast.groupby(test_forecast['ds'].dt.month)['daily_revenue'].sum()
        mape_monthly = mean_absolute_percentage_error(monthly_true, monthly_pred[:len(monthly_true)])
        
        # 记录结果
        results.append({
            'server_id': server_id,
            'mape_daily': mape_daily,
            'mape_weekly': mape_weekly,
            'mape_monthly': mape_monthly
        })
        
        # 绘制每日收入的预测结果
        plt.figure(figsize=(10, 6))
        plt.plot(server_train_data['day'], server_train_data['revenue'], label='Actual Daily Revenue (Train)')
        plt.plot(server_test_data['day'], server_test_data['revenue'], label='Actual Daily Revenue (Test)')
        plt.plot(forecast['ds'], forecast['daily_revenue'], label='Predicted Daily Revenue')
        plt.fill_between(forecast['ds'], forecast['daily_revenue'].min(), forecast['daily_revenue'].max(), where=(forecast['ds'] >= '2025-01-01'), color='gray', alpha=0.2)
        plt.axvline(x=pd.to_datetime('2025-01-01'), color='r', linestyle='--', label='Start of Test Data')
        plt.legend()
        plt.title(f'Daily Revenue Prediction for Server {server_id}')
        plt.xlabel('Date')
        plt.ylabel('Daily Revenue')
        plt.savefig(f'/src/data/20250219_prophet2_daily_revenue_server_{server_id}.png')
        plt.close()
    
    # 保存结果到CSV文件
    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250219_prophet2_results.csv', index=False)

def func3():
    df = getData()
    df = df[df['day'] >= '2024-10-20']  # 确保从2024-10-20（周日）开始
    results = []

    # 将2025-01-05之后的数据作为测试集
    train_df = df[df['day'] < '2025-01-05']
    test_df = df[df['day'] >= '2025-01-05']

    for server_id in range(3, 37):
        # for test
        if server_id != 10:
            continue

        server_train_data = train_df[train_df['server_id_int'] == server_id].sort_values('day').reset_index(drop=True)
        server_test_data = test_df[test_df['server_id_int'] == server_id].sort_values('day').reset_index(drop=True)
        
        if server_train_data.empty or server_test_data.empty:
            continue
        
        # 按周汇总收入，以周日为基准
        server_train_data['week'] = server_train_data['day'].dt.to_period('W').apply(lambda r: r.start_time)
        weekly_train_data = server_train_data.groupby('week')['revenue'].sum().reset_index()
        
        # 检查最近4周的收入和是否小于10美元
        if weekly_train_data['revenue'].tail(4).sum() < 10:
            continue
        
        # 使用累计付费金额
        weekly_train_data['cum_revenue'] = weekly_train_data['revenue'].cumsum()
        
        # 准备数据以适应 Prophet 模型
        prophet_df = weekly_train_data[['week', 'cum_revenue']].rename(columns={'week': 'ds', 'cum_revenue': 'y'})
        
        # 拟合 Prophet 模型
        model = Prophet()
        model.fit(prophet_df)
        
        # 生成未来的日期数据
        future = model.make_future_dataframe(periods=len(server_test_data['day'].dt.to_period('U').unique()), freq='U')
        forecast = model.predict(future)
        
        # 计算每周收入的预测结果
        forecast['weekly_revenue'] = forecast['yhat'].diff()
        forecast = forecast.dropna(subset=['weekly_revenue'])
        
        # 提取测试集预测结果
        test_forecast = forecast[forecast['ds'] >= '2025-01-05']
        
        # 计算 MAPE
        server_test_data['week'] = server_test_data['day'].dt.to_period('U').apply(lambda r: r.start_time)
        weekly_true = server_test_data.groupby('week')['revenue'].sum()
        weekly_pred = test_forecast.set_index('ds')['weekly_revenue']
        
        # 确保 weekly_true 和 weekly_pred 的索引对齐
        weekly_true, weekly_pred = weekly_true.align(weekly_pred, join='inner')
        
        # 检查是否有足够的数据进行 MAPE 计算
        if len(weekly_true) == 0 or len(weekly_pred) == 0:
            continue
        
        mape_weekly = mean_absolute_percentage_error(weekly_true, weekly_pred)
        
        # 按月计算 MAPE
        server_test_data['month'] = server_test_data['day'].dt.month
        monthly_true = server_test_data.groupby('month')['revenue'].sum()
        monthly_pred = test_forecast.set_index('ds').resample('M')['weekly_revenue'].sum()
        
        # 确保 monthly_true 和 monthly_pred 的索引对齐
        monthly_true, monthly_pred = monthly_true.align(monthly_pred, join='inner')
        
        # 检查是否有足够的数据进行 MAPE 计算
        if len(monthly_true) == 0 or len(monthly_pred) == 0:
            continue
        
        mape_monthly = mean_absolute_percentage_error(monthly_true, monthly_pred)
        
        # 记录结果
        results.append({
            'server_id': server_id,
            'mape_weekly': mape_weekly,
            'mape_monthly': mape_monthly
        })
        
        # 绘制每周收入的预测结果
        plt.figure(figsize=(10, 6))
        plt.plot(weekly_train_data['week'], weekly_train_data['revenue'], label='Actual Weekly Revenue (Train)')
        plt.plot(weekly_true.index, weekly_true.values, label='Actual Weekly Revenue (Test)')
        plt.plot(forecast['ds'], forecast['weekly_revenue'], label='Predicted Weekly Revenue')
        plt.fill_between(forecast['ds'], forecast['weekly_revenue'].min(), forecast['weekly_revenue'].max(), where=(forecast['ds'] >= '2025-01-05'), color='gray', alpha=0.2)
        plt.axvline(x=pd.to_datetime('2025-01-05'), color='r', linestyle='--', label='Start of Test Data')
        plt.legend()
        plt.title(f'Weekly Revenue Prediction for Server {server_id}')
        plt.xlabel('Date')
        plt.ylabel('Weekly Revenue')
        plt.savefig(f'/src/data/20250219_prophet3_weekly_revenue_server_{server_id}.png')
        plt.close()
    
    # 保存结果到CSV文件
    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250219_prophet3_results.csv', index=False)


if __name__ == '__main__':
    # func1()
    # func2()

    func3()