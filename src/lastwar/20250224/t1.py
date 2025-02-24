import roi_arpu_cpu_algorithm
import pandas as pd
import numpy as np
from sklearn import metrics
import matplotlib.pyplot as plt


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
        print(f"Processing server {server_id}...")
        # # for test
        # if server_id != 10:
        #     continue

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

        # 划分训练集和测试集（前80%训练，后20%测试）
        split_index = int(len(weekly_data) * 0.8)
        train_data = weekly_data.iloc[:split_index]
        test_data = weekly_data.iloc[split_index:]
        # print('train data len:', len(train_data))
        # print('test data len:', len(test_data))

        # 准备训练集和测试集数据
        x_train = np.arange(1, len(train_data) + 1)  # 训练集时间序列（周数）
        y_payusers_train = train_data['payusers'].values  # 训练集目标值（payusers）
        y_arppu_train = train_data['arppu'].values  # 训练集目标值（arppu）

        x_test = np.arange(len(train_data) + 1, len(weekly_data) + 1)  # 测试集时间序列（周数）
        y_payusers_test = test_data['payusers'].values  # 测试集目标值（payusers）
        y_arppu_test = test_data['arppu'].values  # 测试集目标值（arppu）

        # 初始化最佳MAPE和方案名
        best_payusers_mape = float('inf')
        best_payusers_model_name = None
        best_arppu_mape = float('inf')
        best_arppu_model_name = None

        # 存储最佳方案的预测值和真实值
        best_payusers_predictions_train = None
        best_payusers_predictions_test = None
        best_arppu_predictions_train = None
        best_arppu_predictions_test = None

        # 遍历所有模型，拟合payusers并计算测试集MAPE
        for model_name, model_func in roi_arpu_cpu_algorithm.MODELS.items():
            try:
                # 获取模型
                model = roi_arpu_cpu_algorithm.get_model(model_name, [x_train], y_payusers_train)
                # 预测训练集和测试集
                y_pred_train, _ = model([x_train])
                y_pred_test, _ = model([x_test])
                # 计算测试集MAPE
                mape_test = metrics.mean_absolute_percentage_error(y_payusers_test, y_pred_test)
                # 计算训练集MAPE
                mape_train = metrics.mean_absolute_percentage_error(y_payusers_train, y_pred_train)
                # 更新最佳方案
                if mape_test < best_payusers_mape:
                    best_payusers_mape = mape_test
                    best_payusers_model_name = model_name
                    # 记录训练集和测试集预测值
                    best_payusers_predictions_train = y_pred_train
                    best_payusers_predictions_test = y_pred_test
            except Exception as e:
                print(f"Error fitting payusers with model {model_name} for server {server_id}: {e}")

        # 遍历所有模型，拟合arppu并计算测试集MAPE
        for model_name, model_func in roi_arpu_cpu_algorithm.MODELS.items():
            try:
                # 获取模型
                model = roi_arpu_cpu_algorithm.get_model(model_name, [x_train], y_arppu_train)
                # 预测训练集和测试集
                y_pred_train, _ = model([x_train])
                y_pred_test, _ = model([x_test])
                # 计算测试集MAPE
                mape_test = metrics.mean_absolute_percentage_error(y_arppu_test, y_pred_test)
                # 计算训练集MAPE
                mape_train = metrics.mean_absolute_percentage_error(y_arppu_train, y_pred_train)
                # 更新最佳方案
                if mape_test < best_arppu_mape:
                    best_arppu_mape = mape_test
                    best_arppu_model_name = model_name
                    # 记录训练集和测试集预测值
                    best_arppu_predictions_train = y_pred_train
                    best_arppu_predictions_test = y_pred_test
            except Exception as e:
                print(f"Error fitting arppu with model {model_name} for server {server_id}: {e}")

        # 记录最佳方案
        best_models[server_id] = {
            'best_payusers_model': best_payusers_model_name,
            'best_payusers_mape_train': mape_train,
            'best_payusers_mape_test': best_payusers_mape,
            'best_arppu_model': best_arppu_model_name,
            'best_arppu_mape_train': mape_train,
            'best_arppu_mape_test': best_arppu_mape,
            'best_payusers_predictions_train': best_payusers_predictions_train,
            'best_payusers_predictions_test': best_payusers_predictions_test,
            'best_arppu_predictions_train': best_arppu_predictions_train,
            'best_arppu_predictions_test': best_arppu_predictions_test,
            'train_data': train_data,
            'test_data': test_data
        }

        # 按照最佳方案预测payusers和arppu，并计算收入的MAPE
        if best_payusers_model_name and best_arppu_model_name:
            try:
                # 计算收入的预测值
                y_revenue_pred_train = best_payusers_predictions_train * best_arppu_predictions_train
                y_revenue_pred_test = best_payusers_predictions_test * best_arppu_predictions_test

                # 计算收入的MAPE
                revenue_mape_train = metrics.mean_absolute_percentage_error(train_data['revenue'].values, y_revenue_pred_train)
                revenue_mape_test = metrics.mean_absolute_percentage_error(test_data['revenue'].values, y_revenue_pred_test)
                # 记录收入的MAPE
                best_models[server_id]['revenue_mape_train'] = revenue_mape_train
                best_models[server_id]['revenue_mape_test'] = revenue_mape_test

                # 将每周时间、真实结果、预测结果保存到CSV
                weekly_data['payusers_pred'] = np.concatenate([best_payusers_predictions_train, best_payusers_predictions_test])
                weekly_data['arppu_pred'] = np.concatenate([best_arppu_predictions_train, best_arppu_predictions_test])
                weekly_data['revenue_pred'] = weekly_data['payusers_pred'] * weekly_data['arppu_pred']
                weekly_data.to_csv(f"/src/data/20250224_week_df_{server_id}.csv", index=False)

                # 画图
                plt.figure(figsize=(10, 12))
                plt.subplot(3, 1, 1)
                plt.plot(weekly_data['week'].astype(str), weekly_data['payusers'], label='True Payusers')
                plt.plot(weekly_data['week'].astype(str), weekly_data['payusers_pred'], label='Predicted Payusers')
                plt.axvline(x=split_index, color='r', linestyle='--', label='Train/Test Split')
                plt.title(f'Server {server_id} - Payusers')
                plt.legend()

                plt.subplot(3, 1, 2)
                plt.plot(weekly_data['week'].astype(str), weekly_data['arppu'], label='True ARPPU')
                plt.plot(weekly_data['week'].astype(str), weekly_data['arppu_pred'], label='Predicted ARPPU')
                plt.axvline(x=split_index, color='r', linestyle='--', label='Train/Test Split')
                plt.title(f'Server {server_id} - ARPPU')
                plt.legend()

                plt.subplot(3, 1, 3)
                plt.plot(weekly_data['week'].astype(str), weekly_data['revenue'], label='True Revenue')
                plt.plot(weekly_data['week'].astype(str), weekly_data['revenue_pred'], label='Predicted Revenue')
                plt.axvline(x=split_index, color='r', linestyle='--', label='Train/Test Split')
                plt.title(f'Server {server_id} - Revenue')
                plt.legend()

                plt.tight_layout()
                plt.savefig(f"/src/data/20250224_week_df_{server_id}.png")
                plt.close()
            except Exception as e:
                print(f"Error saving results for server {server_id}: {e}")

    # 输出最终结果
    results_df = pd.DataFrame(columns=[
        '服务器id', 'Best Payusers Model Name', 'Payusers train mape', 'Payusers test mape',
        'Best arppu Model Name', 'arppu train mape', 'arppu test mape', 'revenue train mape', 'revenue test mape'
    ])
    for server_id, result in best_models.items():
        results_df = results_df.append({
            '服务器id': server_id,
            'Best Payusers Model Name': result['best_payusers_model'],
            'Payusers train mape': result['best_payusers_mape_train'],
            'Payusers test mape': result['best_payusers_mape_test'],
            'Best arppu Model Name': result['best_arppu_model'],
            'arppu train mape': result['best_arppu_mape_train'],
            'arppu test mape': result['best_arppu_mape_test'],
            'revenue train mape': result.get('revenue_mape_train', 'N/A'),
            'revenue test mape': result.get('revenue_mape_test', 'N/A')
        }, ignore_index=True)
    results_df.to_csv("/src/data/20250224_final_results.csv", index=False)


if __name__ == '__main__':
    mainWeek()
