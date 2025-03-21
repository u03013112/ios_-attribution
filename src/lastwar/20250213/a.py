# 服务器收入预测
# 1、找到目前日收入最低的服务器或若干服务器
# 2、预测这些服务器日收入到达X所需时间

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score


def getData():
    df = pd.read_csv('lastwar_分服流水_20240101_20250212.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "服务器ID" -> "server_id", "S新支付.美元付费金额 - USD(每日汇率)总和" -> "revenue","S新支付.触发用户数" -> "pay_users","S登录.触发用户数" -> "login_users"
    df = df.rename(columns={
        '时间': 'day', 
        '服务器ID': 'server_id', 
        'S新支付.美元付费金额 - USD(每日汇率)总和': 'revenue', 
        'S新支付.触发用户数': 'pay_users', 
        'S登录.触发用户数': 'login_users'
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
    df['pay_users'] = pd.to_numeric(df['pay_users'], errors='coerce').fillna(0).astype(int)
    df['login_users'] = pd.to_numeric(df['login_users'], errors='coerce').fillna(0).astype(int)

    return df

# 获取最近N周收入最低的10个服务器，与最近N周的平均收入
def getLowestRevenueServers(df, N):
    endday = df['day'].max()
    startDay = endday - pd.DateOffset(weeks=N)
    print(f'获取最近{N}周的数据，从{startDay}到{endday}')
    
    # 获取最近N周的数据
    recent = df[df['day'] >= startDay]

    # 按照"server_id"分组，计算每个服务器在最近N周的总收入
    server_revenue = recent.groupby('server_id')['revenue'].sum()

    # 只统计活跃服务器，即最近N周有收入的服务器
    active_servers = server_revenue[server_revenue > 0]

    # 获取最近N周收入最低的10个服务器
    lowest_revenue_servers = active_servers.nsmallest(10)

    # 计算每个服务器的平均收入
    avg_revenue = recent.groupby('server_id')['revenue'].mean()/N

    # 创建一个包含服务器ID和平均收入的DataFrame
    result_df = pd.DataFrame({
        'server_id': lowest_revenue_servers.index,
        'avg_revenue': avg_revenue[lowest_revenue_servers.index]
    }).reset_index(drop=True)

    return result_df

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

# 使用 Prophet 进行时间序列预测
def prophet_forecast2(df, column, periods, special_events):
    df_prophet = df[['day', column]].rename(columns={'day': 'ds', column: 'y'})
    
    # 创建特殊事件的DataFrame
    holidays = pd.DataFrame()
    for event_name, event_dates in special_events.items():
        event_df = pd.DataFrame({
            'holiday': event_name,
            'ds': pd.to_datetime(event_dates),
            # 'lower_window': -7,
            # 'upper_window': 14
            'lower_window': 0,
            'upper_window': 1
        })
        holidays = pd.concat([holidays, event_df])
    
    model = Prophet(seasonality_mode='multiplicative', weekly_seasonality=True, yearly_seasonality=True, holidays=holidays)
    model.fit(df_prophet)
    
    future = model.make_future_dataframe(periods=periods, freq='W')
    forecast = model.predict(future)
    forecast.to_csv('/src/data/20250213_prophet2_forecast.csv', index=False)    
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

def plot_forecast2(server_id, train_df, forecast_df, target_revenues, endday):
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
    plt.savefig(f'/src/data/20250213_prophet2_{server_id}.png')
    print(f'save file /src/data/20250213_prophet2_{server_id}.png')
    plt.close()

def prophet1(target_revenues=[70, 140], future_periods=8, start_date='2024-11-01'):
    df = getData()
    start_date = '2024-11-01'
    # for test ,只计算 2024-11-01 之后的数据
    df = df[df['day'] >= start_date]

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

    # 找到endday之后第一个周一，记作nextMonday
    nextMonday = endday + pd.DateOffset(days=(7 - endday.weekday()))
    print(f'最近数据的最后一天的下一个周一：{nextMonday}')

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())

        # 确保数据是按周的格式，并以周一为起始日
        server_df = server_df.set_index('day').resample('W', label='left').sum().reset_index()
        print(f'服务器 {server_id} 按周统计后的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df

        # 计算指定时间段内的收入最大值和最小值
        period_df = server_df[(server_df['day'] >= start_date) & (server_df['day'] <= endday)]
        max_revenue = period_df['revenue'].max()
        min_revenue = period_df['revenue'].min()
        width = (max_revenue - min_revenue) / 2

        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入最大值：{max_revenue}')
        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入最小值：{min_revenue}')
        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入波动范围：{width}')

        # 训练 Prophet 模型并进行预测
        model, forecast = prophet_forecast(train_df, 'revenue', periods=0)

        # 提取预测值
        forecast = forecast[:len(train_df)]

        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['revenue'], forecast['yhat'])

        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='W', include_history=False)
        future_forecast = future_forecast[future_forecast['ds'] > nextMonday]

        print(f'服务器 {server_id} 未来预测数据量：{len(future_forecast)}')
        print(future_forecast.head())
        future_forecast = model.predict(future_forecast)

        # 拼接训练集和预测集
        forecast_df = pd.concat([forecast[['ds', 'yhat']], future_forecast[['ds', 'yhat']]])

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
                'target_revenue': target_revenue,
                'target_date': target_date,
                'target_date_lower': target_date_lower,
                'target_date_upper': target_date_upper,
                'mse': mse,
                'rmse': rmse,
                'mape': mape,
                'r2': r2
            })

        # 增加一列 yhat_upper - yhat_lower
        forecast_df['yhat_diff'] = forecast_df['yhat_upper'] - forecast_df['yhat_lower']

        # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
        forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'yhat_diff']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}.csv', index=False)

        # 绘制图表
        plot_forecast(server_id, train_df.rename(columns={'day': 'ds', 'revenue': 'y'}), forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophetForecastLogistic(df, column, periods):
    df_prophet = df[['day', column]].rename(columns={'day': 'ds', column: 'y'})
    # model = Prophet(seasonality_mode='multiplicative',weekly_seasonality=True,yearly_seasonality=True)

    cap = 30000
    floor = 435.81

    df_prophet['cap'] = cap
    df_prophet['floor'] = floor

    model = Prophet(growth='logistic', seasonality_mode='multiplicative',weekly_seasonality=True, yearly_seasonality=True)
    model.fit(df_prophet)
    
    future = model.make_future_dataframe(periods=periods, freq='W')
    future['cap'] = cap
    future['floor'] = floor

    forecast = model.predict(future)
    
    return model, forecast



# growth='logistic'
# 目的是给预测值加上一个上下限，即预测值的波动范围，防止出现收入为负的情况
def prophet1Logistic(target_revenues=[70, 140], future_periods=28, start_date='2024-11-01'):
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

    # 找到endday之后第一个周一，记作nextMonday
    nextMonday = endday + pd.DateOffset(days=(7 - endday.weekday()))
    print(f'最近数据的最后一天的下一个周一：{nextMonday}')

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())

        # 确保数据是按周的格式，并以周一为起始日
        server_df = server_df.set_index('day').resample('W', label='left').sum().reset_index()
        print(f'服务器 {server_id} 按周统计后的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df

        # 计算指定时间段内的收入最大值和最小值
        period_df = server_df[(server_df['day'] >= start_date) & (server_df['day'] <= endday)]
        max_revenue = period_df['revenue'].max()
        min_revenue = period_df['revenue'].min()
        width = (max_revenue - min_revenue) / 2

        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入最大值：{max_revenue}')
        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入最小值：{min_revenue}')
        print(f'服务器 {server_id} 在 {start_date} 到 {endday} 之间的收入波动范围：{width}')

        # 训练 Prophet 模型并进行预测
        model, forecast = prophetForecastLogistic(train_df, 'revenue', periods=0)

        # 提取预测值
        forecast = forecast[:len(train_df)]

        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['revenue'], forecast['yhat'])

        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')

        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='W', include_history=False)
        future_forecast = future_forecast[future_forecast['ds'] > nextMonday]
        
        future_forecast['cap'] = 2562.07
        future_forecast['floor'] = 435.81

        print(f'服务器 {server_id} 未来预测数据量：{len(future_forecast)}')
        print(future_forecast.head())
        future_forecast = model.predict(future_forecast)

        # 拼接训练集和预测集
        forecast_df = pd.concat([forecast[['ds', 'yhat']], future_forecast[['ds', 'yhat']]])

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
                'target_revenue': target_revenue,
                'target_date': target_date,
                'target_date_lower': target_date_lower,
                'target_date_upper': target_date_upper,
                'mse': mse,
                'rmse': rmse,
                'mape': mape,
                'r2': r2
            })

        # 增加一列 yhat_upper - yhat_lower
        forecast_df['yhat_diff'] = forecast_df['yhat_upper'] - forecast_df['yhat_lower']

        # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
        forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'yhat_diff']].to_csv(f'/src/data/20250213_prophet1_results_{server_id}.csv', index=False)

        # 绘制图表
        plot_forecast(server_id, train_df.rename(columns={'day': 'ds', 'revenue': 'y'}), forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1_results.csv', index=False)
    return results_df

def prophet1New(target_revenues=[70, 140], future_periods=28, start_date='2024-11-01'):
    df = getData()

    # 只计算10,13,17 三个服务器
    df = df[(df['server_id_int'] == 10) | (df['server_id_int'] == 13) | (df['server_id_int'] == 17)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 找到endday之后第一个周一，记作nextMonday
    nextMonday = endday + pd.DateOffset(days=(7 - endday.weekday()))
    print(f'最近数据的最后一天的下一个周一：{nextMonday}')

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())

        # 确保数据是按周的格式，并以周一为起始日
        server_df = server_df.set_index('day').resample('W', label='left').sum().reset_index()
        print(f'服务器 {server_id} 按周统计后的数据量：{len(server_df)}')
        print(server_df.head())

        # 计算收入变化率
        server_df['revenue_pct'] = server_df['revenue'].pct_change().fillna(0)
        print(f'服务器 {server_id} 收入变化率：')
        print(server_df[['day', 'revenue', 'revenue_pct']].head())

        # 使用全部数据进行训练
        train_df = server_df[['day', 'revenue_pct']].rename(columns={'day': 'ds', 'revenue_pct': 'y'})

        # 训练 Prophet 模型并进行预测
        model = Prophet(weekly_seasonality=True, yearly_seasonality=True)
        model.fit(train_df)

        # 预测训练集
        train_forecast = model.predict(train_df[['ds']])
        train_df['yhat'] = train_forecast['yhat']

        # 将预测的变化率转换回收入金额
        initial_revenue = server_df['revenue'].iloc[0]
        train_df['revenue_hat'] = initial_revenue
        for i in range(1, len(train_df)):
            train_df.at[i, 'revenue_hat'] = train_df.at[i-1, 'revenue_hat'] * (1 + train_df.at[i, 'yhat'])

        # 计算 R²
        r2 = r2_score(server_df['revenue'].iloc[1:], train_df['revenue_hat'].iloc[1:])
        print(f'服务器 {server_id} 的 R²：{r2}')

        # 预测未来趋势，从整个数据集之后开始
        future = model.make_future_dataframe(periods=future_periods, freq='W')
        future = future[future['ds'] > nextMonday]
        forecast = model.predict(future)

        # 拼接训练集和预测集
        forecast_df = pd.concat([train_df[['ds', 'y', 'yhat']], forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]])

        # 计算预测的收入
        forecast_df['revenue'] = initial_revenue
        forecast_df['revenue_lower'] = initial_revenue
        forecast_df['revenue_upper'] = initial_revenue
        for i in range(1, len(forecast_df)):
            forecast_df.at[i, 'revenue'] = forecast_df.at[i-1, 'revenue'] * (1 + forecast_df.at[i, 'yhat'])
            forecast_df.at[i, 'revenue_lower'] = forecast_df.at[i-1, 'revenue_lower'] * (1 + forecast_df.at[i, 'yhat_lower'])
            forecast_df.at[i, 'revenue_upper'] = forecast_df.at[i-1, 'revenue_upper'] * (1 + forecast_df.at[i, 'yhat_upper'])

        for target_revenue in target_revenues:
            # 求解未来时间
            future_trend = forecast_df['revenue'].values
            future_trend_lower = forecast_df['revenue_lower'].values
            future_trend_upper = forecast_df['revenue_upper'].values

            target_date = None
            target_date_lower = None
            target_date_upper = None

            for i, revenue in enumerate(future_trend):
                if revenue <= target_revenue:
                    target_date = forecast_df['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_lower):
                if revenue <= target_revenue:
                    target_date_lower = forecast_df['ds'].iloc[i]
                    break

            for i, revenue in enumerate(future_trend_upper):
                if revenue <= target_revenue:
                    target_date_upper = forecast_df['ds'].iloc[i]
                    break

            results.append({
                'server_id': server_id,
                'target_revenue': target_revenue,
                'target_date': target_date,
                'target_date_lower': target_date_lower,
                'target_date_upper': target_date_upper,
                'r2': r2
            })

        # 保存预测部分的日期，yhat，yhat lower，yhat upper 到 CSV
        forecast_df[['ds', 'revenue', 'revenue_lower', 'revenue_upper']].to_csv(f'/src/data/20250213_prophet1New_results_{server_id}.csv', index=False)

        # 绘制图表
        plot_forecast(server_id, train_df.rename(columns={'ds': 'day', 'revenue_hat': 'revenue'}), forecast_df.rename(columns({'ds': 'day', 'yhat': 'revenue'})), target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date', 'target_revenue'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet1New_results.csv', index=False)
    return results_df


def prophet2(target_revenues=[70,140], future_periods=28):
    df = getData()

    # df = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]
    df = df[(df['server_id_int'] == 10)]

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

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
    # 将special_events和special_events2合并
    special_events = {**special_events, **special_events2}

    # 将特殊事件的日期调整到最近的周日
    for event_name, event_dates in special_events.items():
        special_events[event_name] = [pd.to_datetime(date) + pd.DateOffset(days=(6 - pd.to_datetime(date).weekday())) for date in event_dates]

    print(special_events)

    # 找到endday之后第一个周一，记作nextMonday
    nextMonday = endday + pd.DateOffset(days=(7 - endday.weekday()))
    print(f'最近数据的最后一天的下一个周一：{nextMonday}')

    # 按服务器ID分组
    servers = df['server_id_int'].unique()
    results = []

    for server_id in servers:
        server_df = df[df['server_id_int'] == server_id]
        print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        print(server_df.head())
        
        # 确保数据是按周的格式，并以周一为起始日
        server_df = server_df.set_index('day').resample('W', label='left').sum().reset_index()
        print(f'服务器 {server_id} 按周统计后的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df
        
        # 训练 Prophet 模型并进行预测
        model, forecast = prophet_forecast2(train_df, 'revenue', periods=0, special_events=special_events)
        
        # 提取预测值
        forecast = forecast[:len(train_df)]
        
        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['revenue'], forecast['yhat'])
        
        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')
        
        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='W', include_history=False)
        future_forecast = future_forecast[future_forecast['ds'] > nextMonday]
        
        print(f'服务器 {server_id} 未来预测数据量：{len(future_forecast)}')
        print(future_forecast.head())
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
        
        # 绘制图表
        plot_forecast2(server_id, train_df.rename(columns={'day': 'ds', 'revenue': 'y'}), forecast_df, target_revenues, endday)

    # 输出结果
    results_df = pd.DataFrame(results)
    # 按照目标日期排序，升序
    results_df = results_df.sort_values(['target_date','target_revenue','server_id'], ascending=True)

    print(results_df)

    # 保存结果
    results_df.to_csv(f'/src/data/20250213_prophet2_results.csv', index=False)
    return results_df

if __name__ == '__main__':
    # df = getData()
    # print(df.head())
    # print(df.info())
    # print(df.describe())
    # df0 = getLowestRevenueServers(df, 4)
    # print(df0)

    # prophet1()
    # prophet2()

    # prophet1Logistic()
    prophet1New()