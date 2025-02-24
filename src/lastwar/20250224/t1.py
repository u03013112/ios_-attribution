import roi_arpu_cpu_algorithm
import pandas as pd
import numpy as np
from sklearn import metrics

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

def mainWeek():
    df = getData()

    # 筛选服务器ID在3到36之间的数据
    df0 = df[(df['server_id'] >= 3) & (df['server_id'] <= 36)]

    # 存储每个服务器的最佳方案和MAPE
    best_models = {}

    for server_id in range(3, 37):
        # for test
        if server_id != 10:
            continue

        # 筛选当前服务器的数据
        server_data = df0[df0['server_id'] == server_id].sort_values('day').reset_index(drop=True)
        
        # 如果当前服务器没有数据，跳过
        if server_data.empty:
            continue
        
        # 检查最近4周的收入和是否小于10美元，如果小于则跳过
        if server_data['revenue'].tail(28).sum() < 10:
            continue
    
        # 计算按周汇总数据
        server_data['week'] = server_data['day'].dt.to_period('W')
        weekly_data = server_data.groupby('week').agg({'revenue': 'sum', 'payusers': 'sum'}).reset_index()
        weekly_data['arppu'] = weekly_data['revenue'] / weekly_data['payusers']

        # 去除不完整周的数据（比如半周的）
        weekly_data = weekly_data[weekly_data['payusers'] > 0]

        # 如果数据不足，跳过
        if len(weekly_data) < 2:
            continue

        # 准备数据
        x = np.arange(1,len(weekly_data)+1)  # 时间序列（周数）
        y_payusers = weekly_data['payusers'].values  # 目标值（payusers）
        y_arppu = weekly_data['arppu'].values  # 目标值（arppu）
        y_revenue = weekly_data['revenue'].values  # 目标值（revenue）

        # 初始化最佳MAPE和方案名
        best_payusers_mape = float('inf')
        best_payusers_model_name = None
        best_arppu_mape = float('inf')
        best_arppu_model_name = None

        print('models:', roi_arpu_cpu_algorithm.MODELS)

        # 遍历所有模型，拟合payusers并计算MAPE
        for model_name, model_func in roi_arpu_cpu_algorithm.MODELS.items():
            try:
                print(model_name)
                # 获取模型
                model = roi_arpu_cpu_algorithm.get_model(model_name, [x], y_payusers)
                # 预测
                y_pred, _ = model([x])
                # 计算MAPE
                mape = metrics.mean_absolute_percentage_error(y_payusers, y_pred)
                print('mape:', mape)
                # 更新最佳方案
                if mape < best_payusers_mape:
                    best_payusers_mape = mape
                    best_payusers_model_name = model_name
            except Exception as e:
                print(f"Error fitting payusers with model {model_name} for server {server_id}: {e}")

        # 遍历所有模型，拟合arppu并计算MAPE
        for model_name, model_func in roi_arpu_cpu_algorithm.MODELS.items():
            try:
                # print(model_name)
                # print('x:')
                # print(x)
                # print('y_arppu:')
                # print(y_arppu)
                # 获取模型
                model = roi_arpu_cpu_algorithm.get_model(model_name, [x], y_arppu)
                # 预测
                y_pred, _ = model([x])
                # 计算MAPE
                mape = metrics.mean_absolute_percentage_error(y_arppu, y_pred)
                # 更新最佳方案
                if mape < best_arppu_mape:
                    best_arppu_mape = mape
                    best_arppu_model_name = model_name
            except Exception as e:
                print(f"Error fitting arppu with model {model_name} for server {server_id}: {e}")

        # 记录最佳方案
        best_models[server_id] = {
            'best_payusers_model': best_payusers_model_name,
            'best_payusers_mape': best_payusers_mape,
            'best_arppu_model': best_arppu_model_name,
            'best_arppu_mape': best_arppu_mape
        }

        # 按照最佳方案预测payusers和arppu，并计算收入的MAPE
        if best_payusers_model_name and best_arppu_model_name:
            try:
                # 获取最佳payusers模型
                best_payusers_model = roi_arpu_cpu_algorithm.get_model(best_payusers_model_name, [x], y_payusers)
                # 预测payusers
                y_payusers_pred, _ = best_payusers_model([x])

                # 获取最佳arppu模型
                best_arppu_model = roi_arpu_cpu_algorithm.get_model(best_arppu_model_name, [x], y_arppu)
                # 预测arppu
                y_arppu_pred, _ = best_arppu_model([x])

                # 计算收入的预测值
                y_revenue_pred = y_payusers_pred * y_arppu_pred

                # 计算收入的MAPE
                revenue_mape = metrics.mean_absolute_percentage_error(y_revenue, y_revenue_pred)
                # 记录收入的MAPE
                best_models[server_id]['revenue_mape'] = revenue_mape
            except Exception as e:
                print(f"Error calculating revenue MAPE for server {server_id}: {e}")

    # 输出每个服务器的最佳方案和MAPE
    for server_id, result in best_models.items():
        print(f"Server {server_id}: "
              f"Best Payusers Model = {result['best_payusers_model']}, Payusers MAPE = {result['best_payusers_mape']:.4f}, "
              f"Best ARPPU Model = {result['best_arppu_model']}, ARPPU MAPE = {result['best_arppu_mape']:.4f}, "
              f"Revenue MAPE = {result.get('revenue_mape', 'N/A')}")

if __name__ == '__main__':
    mainWeek()
