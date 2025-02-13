# 服务器收入预测
# 1、找到目前日收入最低的服务器或若干服务器
# 2、预测这些服务器日收入到达X所需时间

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import STL
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

def PCA1(df):
    # 1. 只统计'APS3'~'APS36'这些服务器
    df = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]

    # 2. 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 3. 按照服务器ID升序排列后的前10名目前的收入金额画在一张图上
    top_servers = sorted(active_servers, key=lambda x: int(x[3:]))[:10]
    df_top = df[df['server_id'].isin(top_servers)]

    # 创建一个pivot table，行是日期，列是服务器ID，值是收入
    pivot_df = df_top.pivot(index='day', columns='server_id', values='revenue').fillna(0)

    # 绘制原始收入图
    plt.figure(figsize=(12, 8))
    plt.subplot(2, 1, 1)
    for server in top_servers:
        plt.plot(pivot_df.index, pivot_df[server], label=server)
    plt.title('Original Revenue')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.legend()

    # 4. 进行PCA，得到残差后，按照第3步的方式，再画一张图
    pca = PCA(n_components=10)
    principal_components = pca.fit_transform(pivot_df)
    reconstructed = pca.inverse_transform(principal_components)
    residuals = pivot_df - reconstructed

    # 绘制残差图
    plt.subplot(2, 1, 2)
    for server in top_servers:
        plt.plot(pivot_df.index, residuals[server], label=server)
    plt.title('Residuals (Unique Trends)')
    plt.xlabel('Date')
    plt.ylabel('Revenue Residuals')
    plt.legend()

    # 保存图像
    plt.tight_layout()
    plt.savefig('/src/data/20250213_pca1.png')
    print('图像已保存到 /src/data/20250213_pca1.png')

def STL_decomposition(df):
    # 1. 只统计'APS3'~'APS36'这些服务器
    df = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]

    # 2. 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    active_servers = server_revenue[server_revenue > 0].index
    df = df[df['server_id'].isin(active_servers)]

    # 3. 按照服务器ID升序排列后的前10名目前的收入金额画在一张图上
    top_servers = sorted(active_servers, key=lambda x: int(x[3:]))[:10]
    df_top = df[df['server_id'].isin(top_servers)]

    # 创建一个pivot table，行是日期，列是服务器ID，值是收入
    pivot_df = df_top.pivot(index='day', columns='server_id', values='revenue').fillna(0)

    # 绘制原始收入图
    plt.figure(figsize=(12, 8))
    plt.subplot(3, 1, 1)
    for server in top_servers:
        plt.plot(pivot_df.index, pivot_df[server], label=server)
    plt.title('Original Revenue')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.legend()

    # 4. 进行STL分解
    trend_components = {}
    for server in top_servers:
        stl = STL(pivot_df[server], seasonal=13)
        result = stl.fit()
        trend_components[server] = result.trend

    # 绘制趋势图
    plt.subplot(3, 1, 2)
    for server in top_servers:
        plt.plot(pivot_df.index, trend_components[server], label=server)
    plt.title('Trend Component')
    plt.xlabel('Date')
    plt.ylabel('Revenue Trend')
    plt.legend()

    # 绘制残差图
    plt.subplot(3, 1, 3)
    for server in top_servers:
        residuals = pivot_df[server] - trend_components[server]
        plt.plot(pivot_df.index, residuals, label=server)
    plt.title('Residuals')
    plt.xlabel('Date')
    plt.ylabel('Revenue Residuals')
    plt.legend()

    # 保存图像
    plt.tight_layout()
    plt.savefig('/src/data/20250213_stl.png')
    print('图像已保存到 /src/data/20250213_stl.png')

# 使用 Prophet 进行时间序列预测
def prophet_forecast(df, column, periods):
    df_prophet = df[['day', column]].rename(columns={'day': 'ds', column: 'y'})
    model = Prophet(weekly_seasonality=True)
    model.fit(df_prophet)
    
    future = model.make_future_dataframe(periods=periods, freq='W')
    forecast = model.predict(future)
    
    return model, forecast

# 评估回归模型
def evaluate_regression_model(y_true, y_pred):
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return mse, rmse, mape, r2

# 主流程封装到函数中
def prophet1(target_revenue=200, future_periods=30):
    df = getData()

    # 过滤服务器ID在10到14之间的数据
    df = df[(df['server_id_int'] >= 10) & (df['server_id_int'] <= 14)]

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
        
        # 确保数据是按周的格式
        server_df = server_df.set_index('day').resample('W-MON').sum().reset_index()
        print(f'服务器 {server_id} 按周统计后的数据量：{len(server_df)}')
        print(server_df.head())

        # 使用全部数据进行训练
        train_df = server_df
        
        # 训练 Prophet 模型并进行预测
        model, forecast = prophet_forecast(train_df, 'revenue', periods=0)
        
        # 提取预测值
        forecast = forecast[:len(train_df)]
        
        # 评估模型性能
        mse, rmse, mape, r2 = evaluate_regression_model(train_df['revenue'], forecast['yhat'])
        
        print(f'服务器 {server_id} - MSE: {mse}, RMSE: {rmse}, MAPE: {mape}, R²: {r2}')
        
        # 预测未来趋势，从整个数据集之后开始
        future_forecast = model.make_future_dataframe(periods=future_periods, freq='W', include_history=False)
        # 调整未来预测日期为周一
        future_forecast['ds'] = future_forecast['ds'] + pd.DateOffset(days=1)
        
        # future_forecast = future_forecast[future_forecast['ds'] > endday]
        print(f'服务器 {server_id} 未来预测数据量：{len(future_forecast)}')
        print(future_forecast.head())
        future_forecast = model.predict(future_forecast)
        
        # 求解未来时间
        future_trend = future_forecast['yhat'].values
        target_date = None
        for i, revenue in enumerate(future_trend):
            if revenue <= target_revenue:
                target_date = future_forecast['ds'].iloc[i]
                break
        
        results.append({
            'server_id': server_id,
            'target_date': target_date,
            'mse': mse,
            'rmse': rmse,
            'mape': mape,
            'r2': r2
        })

    # 输出结果
    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df


if __name__ == '__main__':
    # df = getData()
    # print(df.head())
    # print(df.info())
    # print(df.describe())
    # df0 = getLowestRevenueServers(df, 4)
    # print(df0)

    # PCA1(df)
    # STL_decomposition(df)

    prophet1()