# 直接采用cost金额，不再使用cost金额的增长率
# 预测结果直接是收入金额，不再使用收入金额增长率
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

def getHistoricalDataIOS():
    filename = '/src/data/xiaoyu_historical_data_ios_20240401_20241007.csv'
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
            tmp_lw_cost_and_roi_by_day_ios
        where
            install_day between 20240401
            and 20241007
        group by
            install_day;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def train(train_df, model_path='prophet_model.json'):
    # 准备Prophet所需的数据格式
    prophet_train_df = train_df[['date', 'revenue', 'ad_spend', 'is_weekend']].copy()
    prophet_train_df.columns = ['ds', 'y', 'ad_spend', 'is_weekend']

    # 移除含NaN的行
    prophet_train_df = prophet_train_df.dropna()

    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('ad_spend')
    model.add_regressor('is_weekend')
    model.fit(prophet_train_df)

    # 保存模型
    with open(model_path, 'w') as f:
        f.write(model_to_json(model))

    return model

def predict(test_df, model):
    results = []

    # 进行收入预测
    for index, row in test_df.iterrows():
        # 获取当前日期和相应数据
        current_date = row['date']
        current_ad_spend = row['ad_spend']
        
        # 预测日期是当前日期的下一天
        predict_date = current_date + pd.Timedelta(days=1)
        
        # 获取当前索引在 test_df 中的位置
        loc = test_df.index.get_loc(index)
        
        # 计算真实的花费变化比例
        if loc < len(test_df) - 1:
            next_day_row = test_df.iloc[loc + 1]
            
            # 获取该日期的周末特征
            is_weekend_value = 1 if predict_date.weekday() in [5, 6] else 0  # 计算预测日期是否为周末

            # 预测收入
            future_df = pd.DataFrame({
                'ds': [predict_date],
                'ad_spend': [next_day_row['ad_spend']],
                'is_weekend': [is_weekend_value]
            })
            
            # 调用模型进行预测
            forecast = model.predict(future_df)
            
            # 计算预估收入
            predicted_revenue = forecast['yhat'].values[0]

            results.append({
                'date': predict_date,
                'weekday': predict_date.weekday(),
                'lastday_real_spend': current_ad_spend,
                'predicted_spend': next_day_row['ad_spend'],
                'predicted_revenue': predicted_revenue,
                'real_revenue': next_day_row['revenue']
            })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算MAPE
    results_df['mape'] = np.abs((results_df['predicted_revenue'] - results_df['real_revenue']) / results_df['real_revenue']) * 100

    return results_df


def main():
    # 获取历史数据
    historical_data = getHistoricalData()
    # historical_data = getHistoricalDataIOS()

    # 转换 'install_day' 列为日期格式
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = historical_data.groupby('install_day').agg({
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

    # 确保日期列是日期格式
    df['date'] = pd.to_datetime(df['date'])
    # 按日期排序
    df = df.sort_values('date', ascending=True)

    # 添加周末特征
    df['is_weekend'] = df['date'].dt.dayofweek.isin([5, 6]).astype(int)

    # 移除含NaN的行
    df = df.dropna()

    # 分割训练集和测试集
    train_df = df[(df['date'] >= '2024-04-01') & (df['date'] <= '2024-09-12')]
    test_df = df[(df['date'] >= '2024-09-13') & (df['date'] <= '2024-10-07')]

    model_path = '/src/data/prophet_model3.json'

    # 检查模型是否存在
    if os.path.exists(model_path):
        # 加载模型
        with open(model_path, 'r') as f:
            model = model_from_json(f.read())
    else:
        # 训练模型
        model = train(train_df, model_path)

    # 进行预测
    results_df = predict(test_df, model)

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date', 'mape']])
    results_df[['date', 'mape']].to_csv('/src/data/prediction_results_1.csv', index=False)

    # 输出到 CSV 文件
    results_df.to_csv('/src/data/prediction_results.csv', index=False)

    print(f"Results DataFrame has been saved")

    # 计算并输出所有MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"所有MAPE的平均值: {average_mape:.2f}%")

if __name__ == "__main__":
    main()
