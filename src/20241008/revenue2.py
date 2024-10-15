# 使用预测的付费用户数，估计付费金额，而不是计算arppu

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

# 大R削弱
def getHistoricalData2(startDate, endDate, limit):
    filename = f'/src/data/lw_{startDate}_{endDate}_{limit}.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
select
    to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyymmdd') as date,
    sum(least(user_revenue_24h, {limit})) as 24hours_revenue_capped,
    country,
    mediasource
from (
    select
        game_uid,
        country,
        mediasource,
        install_timestamp,
        sum(case when event_time - cast(install_timestamp as bigint) between 0 and 86400
            then revenue_value_usd else 0 end) as user_revenue_24h
    from dwd_overseas_revenue_allproject
    where
        app = 502
        and app_package = 'com.fun.lastwar.gp'
        and zone = 0
        and day between '{startDate}' and '{endDate}'
        and install_day >= '{startDate}'
    group by game_uid, install_timestamp,country,mediasource
) as user_revenue_summary
group by to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyymmdd'),country,mediasource
;
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
    # model.add_regressor('pud1 prediction')
    model.add_regressor('pud1')
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

    historical_data2 = getHistoricalData2('20240401', '20241015', 100)

    # 数据预处理
    df = preprocessData(historical_data)

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_all_all.csv')
    pud1Df = pud1Df[['date', 'predicted_pud1_pct']]
    pud1Df = pud1Df.rename(columns={'date': 'ds'})
    pud1Df['ds'] = pd.to_datetime(pud1Df['ds'], format='%Y-%m-%d')
    df = df.merge(pud1Df, on='ds', how='left')
    df['last_pud1'] = df['pud1'].shift(1)
    df['pud1 prediction'] = df['last_pud1'] * (1 + df['predicted_pud1_pct'])
    df['pud1 mape'] = np.abs((df['pud1'] - df['pud1 prediction']) / (1 + df['pud1'])) * 100
    
    # 将pud1 prediction为NaN的行去掉
    df = df.dropna(subset=['pud1 prediction'])

    # print(df)
    print('pud1 mape:', df['pud1 mape'].mean())

    test_start_date = '2024-09-01'
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
            # future_df = test_df[['ds', 'pud1 prediction', 'is_weekend']].copy()

            future_df = test_df.copy()
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
    results_df.to_csv('/src/data/revenue2.csv', index=False)

    print(f"Results DataFrame has been saved")

    # 计算并输出pud1_pct的MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"revenue的MAPE的平均值: {average_mape:.2f}%")

if __name__ == "__main__":
    main()
