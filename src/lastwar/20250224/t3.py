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


def mainMonth():
    df = getData()
    df['day'] = pd.to_datetime(df['day'])
    df['month'] = df['day'] - pd.offsets.MonthBegin(1)
    # 只获取2024年11月到2025年1月 3个月的数据
    df = df[(df['month'] >= '2024-11-01') & (df['month'] <= '2025-01-31')]

    # print(df.head())

    monthDf = df.groupby(['server_id','month']).sum().reset_index()
    monthDf['arppu'] = monthDf['revenue'] / monthDf['payusers']

    serverIdList = []

    for server_id in range(3, 37):
        # # for test
        # if server_id != 10:
        #     continue

        # 筛选当前服务器的数据
        server_data = monthDf[monthDf['server_id'] == server_id].sort_values('month').reset_index(drop=True)
        
        # 如果当前服务器没有数据，跳过
        if server_data.empty:
            continue
        
        # 检查最近4周的收入和是否小于10美元，如果小于则跳过
        if server_data['revenue'].tail(28).sum() < 10:
            continue

        serverIdList.append(server_id)
    
    monthDf = monthDf[monthDf['server_id'].isin(serverIdList)]
    monthDf = monthDf.sort_values(['server_id','month']).reset_index()
    # print(monthDf)

    # 按照server汇总成行数据，列变为 server_id, 11月arppu, 12月arppu, 1月arppu, 11月revenue, 12月revenue, 1月revenue, 11月payusers, 12月payusers, 1月payusers
    serverDf = pd.DataFrame()
    for server_id in serverIdList:
        server_data = monthDf[monthDf['server_id'] == server_id]
        serverDf = serverDf.append({
            'server_id': server_id,
            '11月arppu': server_data[server_data['month'] == '2024-11-01']['arppu'].values[0],
            '12月arppu': server_data[server_data['month'] == '2024-12-01']['arppu'].values[0],
            '1月arppu': server_data[server_data['month'] == '2025-01-01']['arppu'].values[0],
            '11月revenue': server_data[server_data['month'] == '2024-11-01']['revenue'].values[0],
            '12月revenue': server_data[server_data['month'] == '2024-12-01']['revenue'].values[0],
            '1月revenue': server_data[server_data['month'] == '2025-01-01']['revenue'].values[0],
            '11月payusers': server_data[server_data['month'] == '2024-11-01']['payusers'].values[0],
            '12月payusers': server_data[server_data['month'] == '2024-12-01']['payusers'].values[0],
            '1月payusers': server_data[server_data['month'] == '2025-01-01']['payusers'].values[0],
        }, ignore_index=True)

    print(serverDf)

    payUserX = serverDf[['11月payusers','12月payusers']]
    payUserY = serverDf[['1月payusers']]
    
    arppuX = serverDf[['11月arppu','12月arppu']]
    arppuY = serverDf[['1月arppu']]

    trainDf = serverDf[serverDf['server_id'] < 20]
    testDf = serverDf[serverDf['server_id'] >= 20]

    # 遍历所有模型，拟合payusers并计算测试集MAPE
    for model_name, model_func in roi_arpu_cpu_algorithm.MODELS.items():
        print(f"Processing model {model_name}")

        trainX = trainDf[['11月payusers', '12月payusers']].values
        testX = testDf[['11月payusers', '12月payusers']].values

        trainY = trainDf['1月payusers'].values
        testY = testDf['1月payusers'].values

        if trainX.shape[0] != trainY.shape[0]:
            raise ValueError(f"Inconsistent number of samples in trainX and trainY: {trainX.shape[0]} vs {trainY.shape[0]}")

        print('trainX shape:', np.array(trainX).shape)
        print('trainY shape:', np.array(trainY).shape)

        model = roi_arpu_cpu_algorithm.get_model(model_name, trainX, trainY)

        # 预测训练集和测试集
        y_pred_train, _ = model([trainX])
        y_pred_test, _ = model([testX])
        
        # 计算测试集MAPE
        mape_test = metrics.mean_absolute_percentage_error(testY, y_pred_test)
        # 计算训练集MAPE
        mape_train = metrics.mean_absolute_percentage_error(trainY, y_pred_train)
        
        print(f"Model {model_name} train MAPE: {mape_train}")

if __name__ == '__main__':
    mainMonth()
