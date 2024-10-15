# 直接预测revenue

import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData():
    filename = '/src/data/xiaoyu_historical_data_20240401_20241007.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
        select
            install_day,
            sum(usd) as usd,
            sum(d1) as d1,
            sum(ins) as ins,
            sum(pud1) as pud1
        from
            tmp_lw_cost_and_roi_by_day
        where
            install_day between 20240401
            and 20241007
        group by
            install_day;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

def preprocessData(data):
    # 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum',
        'ins': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 创建数据框
    df = pd.DataFrame({
        'date': aggregated_data['install_day'],
        'ad_spend': aggregated_data['usd'],
        'revenue': aggregated_data['d1'], 
        'ins': aggregated_data['ins'],
        'pud1': aggregated_data['pud1'],
    })

    # 按日期排序
    df = df.sort_values('date', ascending=True)

    # 移除含NaN的行
    df = df.dropna()

    # 更改列名以适应Prophet模型
    df = df.rename(columns={'date': 'ds', 'revenue': 'y'})

    # 添加是否为周末的特征
    df['is_weekend'] = df['ds'].dt.dayofweek >= 5

    # 移除含NaN的行
    df = df.dropna()

    return df

def train(train_df):    
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('ad_spend')
    model.add_regressor('is_weekend')
    model.fit(train_df)

    # 打印模型训练日志
    print("Model Training Completed")

    return model

def predict(model, future_df):
    # 调用模型进行预测
    forecast = model.predict(future_df)
    return forecast[['ds', 'yhat']]

def main():
    # 获取历史数据
    historical_data = getHistoricalData()

    # 数据预处理
    df = preprocessData(historical_data)

    test_start_date = '2024-07-01'
    test_end_date = '2024-10-07'

    # 初始化结果列表
    results = []

    # 按天训练和预测
    current_date = pd.to_datetime(test_start_date)
    end_date = pd.to_datetime(test_end_date)

    while current_date <= end_date:
        # 定义训练集
        train_start_date = current_date - pd.Timedelta(days=60)
        train_df = df[(df['ds'] >= train_start_date) & (df['ds'] < current_date)]
        
        # 训练模型
        model = train(train_df)

        # 定义测试集
        test_df = df[df['ds'] == current_date]

        if not test_df.empty:
            # 预测
            future_df = test_df[['ds', 'ad_spend', 'is_weekend']].copy()
            # future_df = test_df.copy()
            predictions = predict(model, future_df)

            # 合并预测结果与测试数据
            test_df = test_df.merge(predictions, on='ds', how='left')

            # 计算MAPE
            test_df['mape'] = np.abs((test_df['y'] - test_df['yhat']) / (1 + test_df['y'])) * 100

            # 添加结果到列表
            results.append(test_df)

        # 移动到下一天
        current_date += pd.Timedelta(days=1)

    # 合并所有结果
    results_df = pd.concat(results)

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['ds', 'yhat', 'y', 'mape']])

    # 输出到 CSV 文件
    results_df.to_csv('/src/data/revenue.csv', index=False)

    print(f"Results DataFrame has been saved")

    # 计算并输出pud1_pct的MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"revenue的MAPE的平均值: {average_mape:.2f}%")

if __name__ == "__main__":
    main()
