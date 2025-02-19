# 每天版本

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


# 使用 Prophet 进行时间序列预测
def prophet_forecast(df, column, periods):
    df_prophet = df[['day', column]].rename(columns={'day': 'ds', column: 'y'})
    # model = Prophet(seasonality_mode='multiplicative',weekly_seasonality=True,yearly_seasonality=True)
    # model = Prophet(weekly_seasonality=True,yearly_seasonality=True)
    model = Prophet(seasonality_mode='multiplicative',yearly_seasonality=True)
    model.fit(df_prophet)
    
    future = model.make_future_dataframe(periods=periods, freq='W')
    # future['ds'] = future['ds'] + pd.DateOffset(days=1)

    forecast = model.predict(future)
    forecast.to_csv('/src/data/20250213_prophet1_forecast.csv', index=False)
    
    return model, forecast

# 评估回归模型
def evaluate_regression_model(y_true, y_pred):
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return mse, rmse, mape, r2

def plot_forecast(server_id, train_df, forecast_df, target_revenues, endday):
    plt.figure(figsize=(10, 6))
    plt.plot(train_df['ds'], train_df['y'], label='Actual Revenue')
    plt.plot(forecast_df['ds'], forecast_df['yhat'], label='Predicted Revenue')
    plt.fill_between(forecast_df['ds'], forecast_df['yhat_lower'], forecast_df['yhat_upper'], color='gray', alpha=0.2, label='Prediction Interval')
    
    for target_revenue in target_revenues:
        plt.axhline(y=target_revenue, color='r', linestyle='--', label=f'Target Revenue {target_revenue}')
    
    plt.axvline(x=endday, color='g', linestyle='--', label='End of Actual Data')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.title(f'Server {server_id} Revenue Forecast')
    plt.legend()
    plt.savefig(f'/src/data/20250213_prophet1_{server_id}.png')
    print(f'save file /src/data/20250213_prophet1_{server_id}.png')
    plt.close()

def prophet1(target_revenues=[70, 140], future_periods=30, start_date='2024-11-01'):
    df = getData()

    # 只计算10,13,17 三个服务器
    # df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]
    df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df[['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})

        # 训练 Prophet 模型并进行预测
        model = Prophet(seasonality_mode='multiplicative',yearly_seasonality=True,weekly_seasonality=True,daily_seasonality=True)
        model.fit(train_df)
        
        # 对训练数据进行预测
        forecast = model.predict(train_df[['ds']])
        
        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
        future_forecast = model.predict(future_forecast)

        # 拼接训练集和预测集
        forecast_df = pd.concat([forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

        for target_revenue in target_revenues:
            # 求解未来时间
            future_trend = future_forecast['yhat'].values
            future_trend_lower = future_forecast['yhat_lower'].values
            future_trend_upper = future_forecast['yhat_upper'].values

            target_date = None
            target_date_lower = None
            target_date_upper = None

            for i, revenue in enumerate(future_trend):
                if revenue <= target_revenue:
                    target_date = future_forecast['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_lower):
                if revenue <= target_revenue:
                    target_date_lower = future_forecast['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_upper):
                if revenue <= target_revenue:
                    target_date_upper = future_forecast['ds'].iloc[i]
                    break

            results.append({
                'server_id': server_id,
                'target_revenue': target_revenue,
                'target_date': target_date,
                'target_date_lower': target_date_lower,
                'target_date_upper': target_date_upper,
                'mse': mse,
                'rmse': rmse,
                'mape': mape,
                'r2': r2
            })

        # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
        forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}.csv', index=False)

        # 绘制图表
        plot_forecast(server_id, train_df, forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophet1Floor(target_revenues=[70, 140], future_periods=90, start_date='2024-11-01'):
    df = getData()
    # for test
    df = df[df['day'] >= '2024-09-01']

    # 只计算10,13,17 三个服务器
    # df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]
    df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df[['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})

        # 设置cap和floor列，确保预测值尽量大于0
        train_df['cap'] = train_df['y'].max() * 1.2  # 设置一个合理的上限
        train_df['floor'] = 0  # 设置下限为0

        # 训练 Prophet 模型并进行预测
        model = Prophet(growth='logistic', seasonality_mode='multiplicative', yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)
        model.fit(train_df)
        
        # 对训练数据进行预测
        forecast = model.predict(train_df[['ds', 'cap', 'floor']])
        
        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
        future_forecast['cap'] = train_df['cap'].iloc[0]  # 使用相同的cap值
        future_forecast['floor'] = 0  # 使用相同的floor值
        future_forecast = model.predict(future_forecast)

        # 拼接训练集和预测集
        forecast_df = pd.concat([forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

        for target_revenue in target_revenues:
            # 求解未来时间
            future_trend = future_forecast['yhat'].values
            future_trend_lower = future_forecast['yhat_lower'].values
            future_trend_upper = future_forecast['yhat_upper'].values

            target_date = None
            target_date_lower = None
            target_date_upper = None

            for i, revenue in enumerate(future_trend):
                if revenue <= target_revenue:
                    target_date = future_forecast['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_lower):
                if revenue <= target_revenue:
                    target_date_lower = future_forecast['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_upper):
                if revenue <= target_revenue:
                    target_date_upper = future_forecast['ds'].iloc[i]
                    break

            results.append({
                'server_id': server_id,
                'target_revenue': target_revenue,
                'target_date': target_date,
                'target_date_lower': target_date_lower,
                'target_date_upper': target_date_upper,
                'mse': mse,
                'rmse': rmse,
                'mape': mape,
                'r2': r2
            })

        # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
        forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}.csv', index=False)
        print(f'save file /src/data/20250213_prophet1_results_{server_id}.csv')

        # 绘制图表
        plot_forecast(server_id, train_df, forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophet1FloorL(target_revenues=[10, 20], future_periods=90):
    df = getData()
    
    # # 按服务器分组并计算ewm
    # df = df.sort_values(by=['server_id_int', 'day'])
    # df['revenue'] = df.groupby('server_id_int')['revenue'].transform(lambda x: x.ewm(span=7, adjust=False).mean())

    # 生成从2024-01-01到2024-12-01的自然月的start_date列表
    start_dates = pd.date_range(start='2024-01-01', end='2024-12-01', freq='MS').strftime('%Y-%m-%d').tolist()

    # 只计算10,13,17 三个服务器
    # df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]
    df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    # 计算最后4周的收入最大值与最小值的差值，作为预测的宽度
    last4weeksDf = df[df['day'] >= endday - pd.DateOffset(weeks=4)]
    max_revenue = last4weeksDf['revenue'].max()
    min_revenue = last4weeksDf['revenue'].min()
    width = max_revenue - min_revenue

    for start_date in start_dates:
        for server_id in servers:
            server_df = df[df['server_id_int'] == server_id]
            print(f'服务器 {server_id} 的数据量：{len(server_df)}')
            print(server_df.head())

            # 使用全部数据进行训练
            train_df = server_df[['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})
            train_df = train_df[train_df['ds'] >= start_date]

            # 设置cap和floor列，确保预测值尽量大于0
            train_df['cap'] = train_df['y'].max() * 1.2  # 设置一个合理的上限
            train_df['floor'] = 0  # 设置下限为0

            # 训练 Prophet 模型并进行预测
            model = Prophet(growth='logistic', seasonality_mode='multiplicative', yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True,interval_width=0.25)
            model.fit(train_df)
            
            # 对训练数据进行预测
            forecast = model.predict(train_df[['ds', 'cap', 'floor']])
            
            # 评估模型性能
            mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

            print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

            # 预测未来趋势，从整个数据集之后开始
            future_forecast = model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
            future_forecast['cap'] = train_df['cap'].iloc[0]  # 使用相同的cap值
            future_forecast['floor'] = 0  # 使用相同的floor值
            future_forecast = model.predict(future_forecast)

            # 拼接训练集和预测集
            forecast_df = pd.concat([forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

            future_forecast['yhat_lower'] = future_forecast['yhat'] - width
            future_forecast['yhat_upper'] = future_forecast['yhat'] + width

            # 计算新的 yhat_lower 和 yhat_upper
            forecast_df['yhat_lower'] = forecast_df['yhat'] - width
            forecast_df['yhat_upper'] = forecast_df['yhat'] + width

            for target_revenue in target_revenues:
                # 求解未来时间
                future_trend = future_forecast['yhat'].values
                future_trend_lower = future_forecast['yhat_lower'].values
                future_trend_upper = future_forecast['yhat_upper'].values

                target_date = None
                target_date_lower = None
                target_date_upper = None

                for i, revenue in enumerate(future_trend):
                    if revenue <= target_revenue:
                        target_date = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_lower):
                    if revenue <= target_revenue:
                        target_date_lower = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_upper):
                    if revenue <= target_revenue:
                        target_date_upper = future_forecast['ds'].iloc[i]
                        break

                results.append({
                    'server_id': server_id,
                    'start_date': start_date,
                    'target_revenue': target_revenue,
                    'target_date': target_date,
                    'target_date_lower': target_date_lower,
                    'target_date_upper': target_date_upper,
                    'mse': mse,
                    'rmse': rmse,
                    'mape': mape,
                    'r2': r2
                })

            # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}_{start_date}.csv', index=False)
            print(f'save file /src/data/20250213_prophet1_results_{server_id}_{start_date}.csv')

            # 绘制图表
            plot_forecast(f'{server_id}_{start_date}', train_df, forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophet1FloorL20250219(target_revenues=[10, 20], future_periods=90):
    df = getData()
    
    # 按服务器分组并计算ewm
    df = df.sort_values(by=['server_id_int', 'day'])
    df['revenue'] = df.groupby('server_id_int')['revenue'].transform(lambda x: x.ewm(span=7, adjust=False).mean())

    # 生成从2024-07-01到2024-09-01的自然月的start_date列表
    start_dates = ['2024-07-01', '2024-08-01', '2024-09-01']

    # 只计算3~36的服务器
    df = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]
    # df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    print(f'最近4周的收入和：{server_revenue}')
    active_servers = server_revenue[server_revenue > 10].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    print(f'服务器ID：{servers}')
    results = []

    for server_id in servers:
        # 计算最后4周的收入最大值与最小值的差值，作为预测的宽度
        last4weeksDf = df[(df['server_id_int'] == server_id)&(df['day'] >= startDay)]
        max_revenue = last4weeksDf['revenue'].max()
        min_revenue = last4weeksDf['revenue'].min()
        width = (max_revenue - min_revenue) * 0.5

        for start_date in start_dates:
            server_df = df[df['server_id_int'] == server_id]
            print(f'服务器 {server_id} 的数据量：{len(server_df)}')
            print(server_df.head())

            # 使用全部数据进行训练
            train_df = server_df[['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})
            train_df = train_df[train_df['ds'] >= start_date]

            # 设置cap和floor列，确保预测值尽量大于0
            train_df['cap'] = train_df['y'].max() * 1.2  # 设置一个合理的上限
            train_df['floor'] = 0  # 设置下限为0

            # 训练 Prophet 模型并进行预测
            model = Prophet(growth='logistic', seasonality_mode='multiplicative', yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True, interval_width=0.25)
            model.fit(train_df)
            
            # 对训练数据进行预测
            forecast = model.predict(train_df[['ds', 'cap', 'floor']])
            
            # 评估模型性能
            mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

            print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

            # 预测未来趋势，从整个数据集之后开始
            future_forecast = model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
            future_forecast['cap'] = train_df['cap'].iloc[0]  # 使用相同的cap值
            future_forecast['floor'] = 0  # 使用相同的floor值
            future_forecast = model.predict(future_forecast)

            # 拼接训练集和预测集
            forecast_df = pd.concat([forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

            future_forecast['yhat_lower'] = future_forecast['yhat'] - width
            future_forecast['yhat_upper'] = future_forecast['yhat'] + width

            # 计算新的 yhat_lower 和 yhat_upper
            forecast_df['yhat_lower'] = forecast_df['yhat'] - width
            forecast_df['yhat_upper'] = forecast_df['yhat'] + width

            for target_revenue in target_revenues:
                # 求解未来时间
                future_trend = future_forecast['yhat'].values
                future_trend_lower = future_forecast['yhat_lower'].values
                future_trend_upper = future_forecast['yhat_upper'].values

                target_date = None
                target_date_lower = None
                target_date_upper = None

                for i, revenue in enumerate(future_trend):
                    if revenue <= target_revenue:
                        target_date = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_lower):
                    if revenue <= target_revenue:
                        target_date_lower = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_upper):
                    if revenue <= target_revenue:
                        target_date_upper = future_forecast['ds'].iloc[i]
                        break

                # 如果为空，设置为特定的大日期
                if target_date is None:
                    target_date = pd.Timestamp('2030-01-01')
                if target_date_lower is None:
                    target_date_lower = pd.Timestamp('2030-01-01')
                if target_date_upper is None:
                    target_date_upper = pd.Timestamp('2030-01-01')

                results.append({
                    'server_id': server_id,
                    'start_date': start_date,
                    'target_revenue': target_revenue,
                    'target_date': target_date,
                    'target_date_lower': target_date_lower,
                    'target_date_upper': target_date_upper,
                    'mse': mse,
                    'rmse': rmse,
                    'mape': mape,
                    'r2': r2
                })

            # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}_{start_date}.csv', index=False)
            print(f'save file /src/data/20250213_prophet1_results_{server_id}_{start_date}.csv')

            # 绘制图表
            plot_forecast(f'{server_id}_{start_date}', train_df, forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    print(results_df)
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)

    # 计算每个服务器的最小和最大日期范围
    final_results = []
    for target_revenue in target_revenues:
        for server_id in servers:
            target_dates = [res['target_date'] for res in results if res['server_id'] == server_id and res['target_revenue'] == target_revenue]
            target_dates_lower = [res['target_date_lower'] for res in results if res['server_id'] == server_id and res['target_revenue'] == target_revenue]
            target_dates_upper = [res['target_date_upper'] for res in results if res['server_id'] == server_id and res['target_revenue'] == target_revenue]

            # 计算最小日期时同时考虑 target_dates 和 target_dates_lower
            all_min_dates = target_dates + target_dates_lower
            min_date = min(all_min_dates) if all(all_min_dates) else None

            # 计算最大日期时同时考虑 target_dates 和 target_dates_upper
            all_max_dates = target_dates + target_dates_upper
            max_date = max(all_max_dates) if all(all_max_dates) else None

            # 将特定的大日期重新改为空
            if min_date == pd.Timestamp('2030-01-01'):
                min_date = None
            if max_date == pd.Timestamp('2030-01-01'):
                max_date = None

            final_results.append({
                'server_id': server_id,
                'target_revenue': target_revenue,
                'min_date': min_date,
                'max_date': max_date
            })

    # 输出结果
    final_results_df = pd.DataFrame(final_results)
    # 按照服务器ID和目标收入排序
    final_results_df = final_results_df.sort_values(['server_id', 'target_revenue'], ascending=True)

    print(final_results_df)

    # 保存结果
    final_results_df.to_csv(f'/src/data/20250213_prophet1_results20250219.csv', index=False)
    return final_results_df

def prophet2FloorL(target_revenues=[70, 140], future_periods=180):
    df = getData()
    
    # 生成从2024-01-01到2024-12-01的自然月的start_date列表
    start_dates = pd.date_range(start='2024-01-01', end='2024-12-01', freq='MS').strftime('%Y-%m-%d').tolist()


    # 只计算10,13,17 三个服务器
    df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]
    # df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    # 特殊事件
    special_events = {
        '预热': ['2024-04-15','2024-08-05','2024-12-30','2025-04-14','2025-07-28'],
        '结算': ['2024-06-10','2024-09-30','2025-02-24','2025-06-16'],
        '休赛': ['2024-06-17','2024-06-24','2024-07-22','2024-07-29','2024-10-07','2024-10-21','2024-11-18','2024-11-25','2024-12-23','2025-03-10','2025-03-17','2025-03-24','2025-03-31','2025-04-07','2025-06-30','2025-07-07','2025-07-14','2025-07-21'],
        '战区对决': ['2024-07-01','2024-07-08','2024-07-15','2024-10-28','2024-11-04','2024-11-11','2024-12-02','2024-12-09','2024-12-16'],
        '移民':['2024-10-14','2025-03-03','2025-06-23']
    }
    # 特殊事件2
    special_events2 = {
        '军备加获取途径跳转': ['2025-02-06'],
        '战斗优化上线': ['2025-01-15'],
        '新版推荐加盟与入盟邀请': ['2024-12-26'],
        '尸潮攻城开启打点换成s_zombie_rush_search_start': ['2024-12-26'],
        '尸潮预约': ['2024-12-11'],
        '沙漠2.0全服开放': ['2024-11-25'],
        '冬日战场中小R匹配加速': ['2024-11-25'],
        '冬日开放范围增加': ['2024-11-19'],
        '冬日增加主界面入口': ['2024-11-04'],
        '修复冬日匹配的严重bug': ['2024-10-29'],
        '左右门新手关全量': ['2024-10-16'],
        '3-36第一次移民': ['2024-10-15'],
        '新手移民全服开放': ['2024-09-23'],
        '前线突围开全服': ['2024-09-18'],
        '新手流程AB全量': ['2024-09-18'],
        '新手不热更全量': ['2024-09-18'],
        '新版不热更上线': ['2024-09-11'],
        '前线突围开所有新服': ['2024-09-04'],
        '优化了关卡配置，增加了小怪的死亡随机动作': ['2024-08-28'],
        '新手不热更全量': ['2024-08-22'],
        '得分提醒上线': ['2024-08-22'],
        '优化关卡表现效果（受击效果、子弹效果等）': ['2024-08-20'],
        '上线前线突围': ['2024-08-20'],
        '新手不热更开启阿语': ['2024-08-13'],
        '大富翁关卡前30关迭代全量': ['2024-08-08'],
        '冬日战场MVP功能': ['2024-08-03'],
        '修改前七关怪物模型配置': ['2024-08-01'],
        '更新了侵权丧尸': ['2024-07-30'],
        '运动节活动，城堡特效，旧城堡返场': ['2024-07-29'],
        '请求灭火消息': ['2024-07-25'],
        '关卡内性能优化': ['2024-07-25'],
        '大富翁前七关关卡AB全量': ['2024-07-22'],
        '末日军团改为打1次，大幅上调任务奖励': ['2024-07-05'],
        '新手关打门关卡全量': ['2024-07-02'],
        '雷达引导全量': ['2024-07-02'],
        '大富翁地块事件、鼓励师全量': ['2024-07-02'],
        '木桶表现效果全量': ['2024-06-25'],
        '21-100赛季S1结束': ['2024-06-24'],
        '冠军对决3-20': ['2024-06-17'],
        '冬季风暴战场上线': ['2024-06-08'],
        '全量新手流程': ['2024-06-08'],
        '战斗音效': ['2024-06-04'],
        '尸潮攻城打点': ['2024-05-15'],
        '21-100开赛季S1': ['2024-05-06'],
        '加了救完妹子后弹窗': ['2024-04-08'],
        '激活vip得分从30→40，发出城际贸易货车得分从10→30，取消偷取他人隐密任务的品质要求': ['2024-04-08'],
        '修复新攻城的全军突击功能': ['2024-03-31'],
        '新攻城上线': ['2024-03-28'],
        '付费4刀弹评分': ['2024-03-20'],
        '挖宝锦鲤上线': ['2024-03-20'],
        '雷达一键完成': ['2024-03-15'],
        '游荡boss每小时上限40→80，优化搜索规则': ['2024-03-13'],
        '新增游荡boss雷达任务': ['2024-03-07'],
        '5星好评提前到升旗结束': ['2024-03-06'],
        '无人机芯片上线': ['2024-03-01'],
        '个人货车科技': ['2024-01-18'],
        '联盟对决联赛第二届，联赛开启时，排名17-32的联盟进热身赛，对决开启时，1-32都会进本服/跨服热身赛': ['2024-01-15'],
        '5星好评打点': ['2024-01-10'],
        '新年活动上线': ['2023-12-29'],
        '圣诞节系列活动上线，会叠加新手累充': ['2023-12-22'],
        '游荡boss': ['2023-12-21'],
        '联盟对决联赛第一届': ['2023-12-18'],
        '欧美服和亚服': ['2023-12-08'],
        '无人机功能上线 个人军备加排行榜': ['2023-12-06'],
        '感恩节活动结束': ['2023-11-28'],
        '同盟火车': ['2023-11-22'],
        '感恩节活动开启': ['2023-11-22'],
        '感恩节开启': ['2023-11-22'],
        '丧尸入侵': ['2023-11-14'],
        '万圣节活动结束': ['2023-11-05'],
        '万圣节活动开启': ['2023-10-30'],
        '战区对决 跨服个人货车': ['2023-10-26'],
        '同盟boss开放': ['2023-09-28'],
        '3v3积分赛': ['2023-09-26'],
        '沙漠风暴战场上线': ['2023-08-28'],
        '联盟对决上线': ['2023-08-24'],
        '个人货车玩法上线': ['2023-08-08'],
        '新王座战': ['2023-07-20'],
        '派遣任务': ['2023-06-28'],
        '将军试炼': ['2023-06-20'],
        '世界boss': ['2023-06-12'],
        '麦克斯韦试炼': ['2023-06-02']
    }
    # # 将special_events和special_events2合并
    # special_events = {**special_events, **special_events2}

    # 创建特殊事件的DataFrame
    holidays = pd.DataFrame()
    for event_name, event_dates in special_events.items():
        event_df = pd.DataFrame({
            'holiday': event_name,
            'ds': pd.to_datetime(event_dates),
            'lower_window': -7,
            'upper_window': 7,
            # 'lower_window': 0,
            # 'upper_window': 1
        })
        holidays = pd.concat([holidays, event_df])
    
    # 计算最后4周的收入最大值与最小值的差值，作为预测的宽度
    last4weeksDf = df[df['day'] >= endday - pd.DateOffset(weeks=4)]
    max_revenue = last4weeksDf['revenue'].max()
    min_revenue = last4weeksDf['revenue'].min()
    width = max_revenue - min_revenue

    for start_date in start_dates:
        for server_id in servers:
            server_df = df[df['server_id_int'] == server_id]
            print(f'服务器 {server_id} 的数据量：{len(server_df)}')
            print(server_df.head())

            # 使用全部数据进行训练
            train_df = server_df[['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})
            train_df = train_df[train_df['ds'] >= start_date]

            # 设置cap和floor列，确保预测值尽量大于0
            train_df['cap'] = train_df['y'].max() * 1.2  # 设置一个合理的上限
            train_df['floor'] = 0  # 设置下限为0

            # 训练 Prophet 模型并进行预测
            model = Prophet(growth='logistic', seasonality_mode='multiplicative', yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True,interval_width=0.25, holidays=holidays)
            model.fit(train_df)
            
            # 对训练数据进行预测
            forecast = model.predict(train_df[['ds', 'cap', 'floor']])
            
            # 评估模型性能
            mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

            print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

            # 预测未来趋势，从整个数据集之后开始
            future_forecast = model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
            future_forecast['cap'] = train_df['cap'].iloc[0]  # 使用相同的cap值
            future_forecast['floor'] = 0  # 使用相同的floor值
            future_forecast = model.predict(future_forecast)

            # 拼接训练集和预测集
            forecast_df = pd.concat([forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

            future_forecast['yhat_lower'] = future_forecast['yhat'] - width
            future_forecast['yhat_upper'] = future_forecast['yhat'] + width

            # 计算新的 yhat_lower 和 yhat_upper
            forecast_df['yhat_lower'] = forecast_df['yhat'] - width
            forecast_df['yhat_upper'] = forecast_df['yhat'] + width


            for target_revenue in target_revenues:
                # 求解未来时间
                future_trend = future_forecast['yhat'].values
                future_trend_lower = future_forecast['yhat_lower'].values
                future_trend_upper = future_forecast['yhat_upper'].values

                target_date = None
                target_date_lower = None
                target_date_upper = None

                for i, revenue in enumerate(future_trend):
                    if revenue <= target_revenue:
                        target_date = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_lower):
                    if revenue <= target_revenue:
                        target_date_lower = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_upper):
                    if revenue <= target_revenue:
                        target_date_upper = future_forecast['ds'].iloc[i]
                        break

                results.append({
                    'server_id': server_id,
                    'start_date': start_date,
                    'target_revenue': target_revenue,
                    'target_date': target_date,
                    'target_date_lower': target_date_lower,
                    'target_date_upper': target_date_upper,
                    'mse': mse,
                    'rmse': rmse,
                    'mape': mape,
                    'r2': r2
                })

            # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}_{start_date}.csv', index=False)
            print(f'save file /src/data/20250213_prophet1_results_{server_id}_{start_date}.csv')

            # 绘制图表
            plot_forecast(f'{server_id}_{start_date}', train_df, forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophet3(target_revenues=[70, 140], future_periods=180):
    df = getData()
    
    # 按服务器分组并计算ewm
    df = df.sort_values(by=['server_id_int', 'day'])
    df['revenue'] = df.groupby('server_id_int')['revenue'].transform(lambda x: x.ewm(span=7, adjust=False).mean())
    
    # 生成从2024-01-01到2024-12-01的自然月的start_date列表
    start_dates = pd.date_range(start='2024-01-01', end='2024-12-01', freq='MS').strftime('%Y-%m-%d').tolist()
    start_dates = [
        '2024-01-01',
        '2024-04-01',
        '2024-07-01',
        '2024-10-01',

    ]

    # 只计算10,13,17 三个服务器
    # df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]
    df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    # 特殊事件
    special_events = {
        '预热': ['2024-04-15','2024-08-05','2024-12-30','2025-04-14','2025-07-28'],
        '结算': ['2024-06-10','2024-09-30','2025-02-24','2025-06-16'],
        '休赛': ['2024-06-17','2024-06-24','2024-07-22','2024-07-29','2024-10-07','2024-10-21','2024-11-18','2024-11-25','2024-12-23','2025-03-10','2025-03-17','2025-03-24','2025-03-31','2025-04-07','2025-06-30','2025-07-07','2025-07-14','2025-07-21'],
        '战区对决': ['2024-07-01','2024-07-08','2024-07-15','2024-10-28','2024-11-04','2024-11-11','2024-12-02','2024-12-09','2024-12-16'],
        '移民':['2024-10-14','2025-03-03','2025-06-23']
    }

    # 创建特殊事件的DataFrame
    holidays = pd.DataFrame()
    for event_name, event_dates in special_events.items():
        event_df = pd.DataFrame({
            'holiday': event_name,
            'ds': pd.to_datetime(event_dates),
            'lower_window': -7,
            'upper_window': 7,
        })
        holidays = pd.concat([holidays, event_df])
    
    best_results = []

    for server_id in servers:
        best_r2 = -float('inf')
        best_start_date = None
        best_forecast_df = None
        best_train_df = None
        best_model = None

        for start_date in start_dates:
            server_df = df[df['server_id_int'] == server_id]
            print(f'服务器 {server_id} 的数据量：{len(server_df)}')
            print(server_df.head())

            server_df = server_df.rename(columns={'day': 'ds', 'revenue': 'y'})
            
            # 设置cap和floor列，确保预测值尽量大于0
            server_df['cap'] = server_df['y'].max()  # 设置一个合理的上限
            server_df['floor'] = 0  # 设置下限为0

            train_test_split_date = '2025-02-01'

            # 使用2025-02-01之前的数据进行训练
            train_df = server_df[server_df['ds'] < train_test_split_date][['ds', 'y', 'cap', 'floor']]
            train_df = train_df[train_df['ds'] >= start_date]
            

            # 训练 Prophet 模型并进行预测
            model = Prophet(growth='logistic', seasonality_mode='multiplicative', yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True, interval_width=0.25, holidays=holidays)
            model.fit(train_df)
            
            # 对训练数据进行预测
            forecast = model.predict(train_df[['ds', 'cap', 'floor']])
            
            # 评估模型性能
            mse, rmse, mape, r2 = evaluate_regression_model(train_df['y'], forecast['yhat'])

            print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

            # 使用测试集数据计算R²
            test_df = server_df[(server_df['ds'] >= train_test_split_date)]
            print(f'服务器 {server_id} 的测试集数据量：{len(test_df)}')
            print(test_df.head())
            test_forecast = model.predict(test_df[['ds', 'cap', 'floor']])
            test_r2 = r2_score(test_df['y'], test_forecast['yhat'])

            print(f'服务器 {server_id} - 测试集 R²: {test_r2}')

            if test_r2 > best_r2:
                best_r2 = test_r2
                best_start_date = start_date
                best_forecast_df = forecast
                best_train_df = train_df
                best_model = model

        if best_forecast_df is not None and best_train_df is not None:
            # 预测未来趋势，从整个数据集之后开始
            future_forecast = best_model.make_future_dataframe(periods=future_periods, freq='D', include_history=False)
            future_forecast['cap'] = best_train_df['cap'].iloc[0]  # 使用相同的cap值
            future_forecast['floor'] = 0  # 使用相同的floor值
            future_forecast = best_model.predict(future_forecast)

            # 拼接训练集和预测集
            forecast_df = pd.concat([best_forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

            for target_revenue in target_revenues:
                # 求解未来时间
                future_trend = future_forecast['yhat'].values
                future_trend_lower = future_forecast['yhat_lower'].values
                future_trend_upper = future_forecast['yhat_upper'].values

                target_date = None
                target_date_lower = None
                target_date_upper = None

                for i, revenue in enumerate(future_trend):
                    if revenue <= target_revenue:
                        target_date = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_lower):
                    if revenue <= target_revenue:
                        target_date_lower = future_forecast['ds'].iloc[i]
                        break

                for i, revenue in enumerate(future_trend_upper):
                    if revenue <= target_revenue:
                        target_date_upper = future_forecast['ds'].iloc[i]
                        break

                results.append({
                    'server_id': server_id,
                    'start_date': best_start_date,
                    'target_revenue': target_revenue,
                    'target_date': target_date,
                    'target_date_lower': target_date_lower,
                    'target_date_upper': target_date_upper,
                    'mse': mse,
                    'rmse': rmse,
                    'mape': mape,
                    'r2': r2,
                    'test_r2': best_r2
                })

            # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(f'/src/data/20250213_prophet3_results_{server_id}_{best_start_date}.csv', index=False)
            print(f'save file /src/data/20250213_prophet3_results_{server_id}_{best_start_date}.csv')

            best_results.append({
                'server_id': server_id,
                'start_date': best_start_date,
                'r2': best_r2,
                'model': best_model
            })

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet3_results.csv', index=False)

    # 绘制最佳图表
    for result in best_results:
        server_id = result['server_id']
        start_date = result['start_date']
        model = result['model']
        forecast_df = pd.read_csv(f'/src/data/20250213_prophet3_results_{server_id}_{start_date}.csv')
        train_df = df[df['server_id_int'] == server_id][['day', 'revenue']].rename(columns={'day': 'ds', 'revenue': 'y'})
        train_df['ds'] = pd.to_datetime(train_df['ds'])
        forecast_df['ds'] = pd.to_datetime(forecast_df['ds'])

        train_df = train_df[train_df['ds'] >= start_date]
        # print('train_df:')
        # print(train_df.info())
        # print('forecast_df:')
        # print(forecast_df.info())
        plot_forecast(f'{server_id}_{start_date}', train_df, forecast_df, target_revenues, endday)

    return results_df

if __name__ == '__main__':
    # prophet1()
    # prophet1Floor()
    # prophet1FloorL()
    prophet1FloorL20250219()

    # prophet2FloorL()
    # prophet3()
    