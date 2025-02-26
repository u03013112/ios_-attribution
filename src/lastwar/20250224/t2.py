
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from prophet import Prophet

def getData():
    df = pd.read_csv('payusers_revenue_20241016_20250223.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "原服ID：数值格式" -> "server_id"
    df = df.rename(columns={
        '时间': 'day', 
        '原服ID：数值格式': 'server_id',
    })

    # 将 服务器ID 为 空 的行删除
    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)

    # print(df.head())
    # #         day  server_id  payusers  revenue
    # # 0 2024-10-16         22      46.0  1077.90
    # # 1 2024-10-17         22      57.0  1706.28
    # # 2 2024-10-18         22      46.0  1062.88
    # # 3 2024-10-19         22      59.0  1181.60
    # # 4 2024-10-20         22      49.0   945.98

    return df

def getData2():
    df = pd.read_csv('payusers_revenue_20240101_20250223.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "原服ID：数值格式" -> "server_id"
    df = df.rename(columns={
        '时间': 'day', 
        '原服ID：数值格式': 'server_id',
    })

    # 将 服务器ID 为 空 的行删除
    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)

    # print(df.head())
    # #         day  server_id  payusers  revenue
    # # 0 2024-10-16         22      46.0  1077.90
    # # 1 2024-10-17         22      57.0  1706.28
    # # 2 2024-10-18         22      46.0  1062.88
    # # 3 2024-10-19         22      59.0  1181.60
    # # 4 2024-10-20         22      49.0   945.98

    return df

# 
def getData3():
    df = pd.read_csv('payusers_revenue_20240101_20250225_without_gm.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "原服ID：数值格式" -> "server_id"
    df = df.rename(columns={
        '时间': 'day', 
        '原服ID：数值格式': 'server_id',
    })

    # 将 服务器ID 为 空 的行删除
    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)

    # print(df.head())
    # #         day  server_id  payusers  revenue
    # # 0 2024-10-16         22      46.0  1077.90
    # # 1 2024-10-17         22      57.0  1706.28
    # # 2 2024-10-18         22      46.0  1062.88
    # # 3 2024-10-19         22      59.0  1181.60
    # # 4 2024-10-20         22      49.0   945.98

    return df



def prophet1FloorL(target_revenues=[10, 20], future_periods=90):
    # df = getData2()
    df = getData3()

    df['day'] = pd.to_datetime(df['day'])

    # 按服务器分组并计算ewm
    df = df.sort_values(by=['server_id', 'day'])
    # df['revenue'] = df.groupby('server_id')['revenue'].transform(lambda x: x.ewm(span=7, adjust=False).mean())
    df['revenue'] = df.groupby('server_id')['revenue'].transform(lambda x: x.ewm(span=14, adjust=False).mean())

    # df = df[df['day'] >= '2024-10-16']
    df = df[df['day'] >= '2024-01-01']

    # 只计算3~36的服务器
    df = df[(df['server_id'] >= 3) & (df['server_id'] <= 36)]
    
    # for test
    # df = df[(df['server_id'] == 10)]
    df = df.groupby(['day']).sum().reset_index()
    df['server_id'] = 0

    # 过滤掉最近4周收入和为0的服务器
    endday = df['day'].max()
    # print(f'最近数据的最后一天：{endday}')
    startDay = endday - pd.DateOffset(weeks=4)
    recent = df[df['day'] >= startDay]
    server_revenue = recent.groupby('server_id')['revenue'].sum()
    # print(f'最近4周的收入和：{server_revenue}')
    active_servers = server_revenue[server_revenue > 10].index
    df = df[df['server_id'].isin(active_servers)]

    # 按服务器ID分组
    servers = df['server_id'].unique()
    # print(f'服务器ID：{servers}')

    for server_id in servers:
        server_df = df[df['server_id'] == server_id].copy()
        # print(f'服务器 {server_id} 的数据量：{len(server_df)}')
        # print(server_df.head())

        # 2025-01-01 作为训练集与测试集的分割
        start_date = '2025-01-01'
        server_df['cap'] = 1000
        server_df['floor'] = 0
        server_df.rename(columns={'day': 'ds', 'revenue': 'y'}, inplace=True)
        server_df = server_df[['ds', 'y', 'cap', 'floor']]

        train_df = server_df[server_df['ds'] < start_date]
        test_df = server_df[server_df['ds'] >= start_date]

        # 训练 Prophet 模型并进行预测
        model = Prophet()
        model.fit(train_df)
        
        train_forecast = model.predict(train_df[['ds', 'cap', 'floor']])
        test_forecast = model.predict(test_df[['ds', 'cap', 'floor']])

        # 合并训练集和测试集数据
        full_df = pd.concat([train_df, test_df])
        model = Prophet()
        model.fit(full_df)

        # 预测未来 future_periods 天的数据
        future = model.make_future_dataframe(periods=future_periods)
        future['cap'] = 1000
        future['floor'] = 0
        full_forecast = model.predict(future)

        # 计算train、test的MAPE
        train_df_new = train_df[['ds', 'y']].merge(train_forecast[['ds', 'yhat']], on='ds')
        train_df_new['mape'] = np.abs(train_df_new['y'] - train_df_new['yhat']) / train_df_new['y']
        train_df_new.to_csv(f'/src/data/20250224_prophet1_train_{server_id}.csv', index=False)
        train_mape = np.mean(train_df_new['mape'])

        test_df_new = test_df[['ds', 'y']].merge(test_forecast[['ds', 'yhat']], on='ds')
        test_df_new['mape'] = np.abs(test_df_new['y'] - test_df_new['yhat']) / test_df_new['y']
        test_df_new.to_csv(f'/src/data/20250224_prophet1_test_{server_id}.csv', index=False)
        test_mape = np.mean(test_df_new['mape'])

        # 按周做汇总，然后再计算train、test的MAPE
        train_df_new['week'] = train_df_new['ds'].dt.week
        train_df_new_week = train_df_new.groupby('week').agg({'y': 'sum', 'yhat': 'sum'}).reset_index()
        train_df_new_week['mape'] = np.abs(train_df_new_week['y'] - train_df_new_week['yhat']) / train_df_new_week['y']
        train_df_new_week.to_csv(f'/src/data/20250224_prophet1_train_week_{server_id}.csv', index=False)
        train_mape_week = np.mean(train_df_new_week['mape'])

        test_df_new['week'] = test_df_new['ds'].dt.week
        test_df_new_week = test_df_new.groupby('week').agg({'y': 'sum', 'yhat': 'sum'}).reset_index()
        test_df_new_week['mape'] = np.abs(test_df_new_week['y'] - test_df_new_week['yhat']) / test_df_new_week['y']
        test_df_new_week.to_csv(f'/src/data/20250224_prophet1_test_week_{server_id}.csv', index=False)
        test_mape_week = np.mean(test_df_new_week['mape'])

        # 按月做汇总，然后再计算train、test的MAPE
        train_df_new['month'] = train_df_new['ds'].dt.month
        train_df_new_month = train_df_new.groupby('month').agg({'y': 'sum', 'yhat': 'sum'}).reset_index()
        train_df_new_month['mape'] = np.abs(train_df_new_month['y'] - train_df_new_month['yhat']) / train_df_new_month['y']
        train_df_new_month.to_csv(f'/src/data/20250224_prophet1_train_month_{server_id}.csv', index=False)
        train_mape_month = np.mean(train_df_new_month['mape'])

        test_df_new['month'] = test_df_new['ds'].dt.month
        test_df_new_month = test_df_new.groupby('month').agg({'y': 'sum', 'yhat': 'sum'}).reset_index()
        test_df_new_month['mape'] = np.abs(test_df_new_month['y'] - test_df_new_month['yhat']) / test_df_new_month['y']
        test_df_new_month.to_csv(f'/src/data/20250224_prophet1_test_month_{server_id}.csv', index=False)
        test_mape_month = np.mean(test_df_new_month['mape'])

        # 保存结果到 CSV 文件
        result_df = full_forecast[['ds', 'yhat']].merge(server_df[['ds', 'y']], on='ds', how='left')
        result_df.rename(columns={'y': 'actual_revenue', 'yhat': 'predicted_revenue'}, inplace=True)
        result_df['initial_predicted_revenue'] = result_df['predicted_revenue']
        result_df.loc[result_df['ds'] >= start_date, 'initial_predicted_revenue'] = np.nan

        # 将训练集和测试集的预测结果合并到 initial_predicted_revenue 列中
        initial_predicted = pd.concat([train_forecast[['ds', 'yhat']], test_forecast[['ds', 'yhat']]])
        initial_predicted.rename(columns={'yhat': 'initial_predicted_revenue'}, inplace=True)
        result_df = result_df.merge(initial_predicted, on='ds', how='left')

        result_df.rename(columns={
            'initial_predicted_revenue_y': 'predict1',
            'predicted_revenue': 'predict2',
        }, inplace=True)
        result_df = result_df[['ds', 'actual_revenue', 'predict1','predict2']]
        result_df.to_csv(f'/src/data/20250224_prophet1_result_{server_id}.csv', index=False)

        print('server', server_id)
        print('train mape:', train_mape)
        print('test mape:', test_mape)
        print('train mape week:', train_mape_week)
        print('test mape week:', test_mape_week)
        print('train mape month:', train_mape_month)
        print('test mape month:', test_mape_month)

        # 画图
        plt.figure(figsize=(18, 6))
        plt.plot(train_df_new['ds'], train_df_new['y'], label='Actual Revenue')
        plt.plot(train_df_new['ds'], train_df_new['yhat'], label='Predicted Revenue')

        plt.plot(test_df_new['ds'], test_df_new['y'], label='Actual Revenue', alpha=0.6)
        plt.plot(test_df_new['ds'], test_df_new['yhat'], label='Predicted Revenue', alpha=0.6)

        plt.plot(full_forecast['ds'], full_forecast['yhat'], label='Future Predicted Revenue', linestyle='--')

        # 添加竖线分割训练集和测试集，以及测试集和预测集
        plt.axvline(x=pd.to_datetime(start_date), color='r', linestyle='--', label='Train/Test Split')
        plt.axvline(x=pd.to_datetime(test_df['ds'].max()), color='g', linestyle='--', label='Test/Future Split')

        plt.xlabel('Date')
        plt.ylabel('Revenue')
        plt.title(f'Server {server_id} Revenue Forecast')
        plt.legend()
        plt.savefig(f'/src/data/20250224_prophet1_{server_id}.png')
        print(f'save file /src/data/20250224_prophet1_{server_id}.png')
        plt.close()


if __name__ == '__main__':
    prophet1FloorL()
            

