import os
import pandas as pd
import numpy as np
from prophet import Prophet

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData():
    filename = '/src/data/xiaoyu_historical_data_20240401_20241007_2.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
        select
            install_day,
            mediasource,
            country,
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
            install_day, mediasource, country;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

def preprocessData(data, media=None, country=None):
    # 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')

    # 过滤数据
    if media:
        data = data[data['mediasource'] == media]
    if country:
        data = data[data['country'] == country]

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

    # 确保日期列是日期格式
    df['date'] = pd.to_datetime(df['date'])
    # 按日期排序
    df = df.sort_values('date', ascending=True)

    # 计算付费用户数变化百分比
    df['pud1_pct'] = df['pud1'].pct_change()

    # 计算广告支出变化百分比
    df['ad_spend_pct'] = df['ad_spend'].pct_change()

    # 移除含NaN的行
    df = df.dropna()

    # 更改列名以适应Prophet模型
    df = df.rename(columns={'date': 'ds', 'pud1_pct': 'y'})

    # 排除y值为inf的行
    df = df[~df['y'].isin([np.inf, -np.inf])]
    # 排除ad_spend_pct为inf的行
    df = df[~df['ad_spend_pct'].isin([np.inf, -np.inf])]

    # 确保数据类型正确
    df['ds'] = pd.to_datetime(df['ds'])
    df['y'] = df['y'].astype(float)
    df['ad_spend_pct'] = df['ad_spend_pct'].astype(float)

    # 添加T-3, T-2, T-1的y值作为输入
    df['y_lag_1'] = df['y'].shift(1)
    df['y_lag_2'] = df['y'].shift(2)
    df['y_lag_3'] = df['y'].shift(3)

    # 添加是否为周末的特征
    df['is_weekend'] = df['ds'].dt.dayofweek >= 5

    # 移除含NaN的行
    df = df.dropna()

    df['cap'] = 1

    return df

def train(train_df):    
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('ad_spend_pct')
    model.add_regressor('is_weekend')
    model.fit(train_df, verbose=False)

    # 打印模型训练日志
    print("Model Training Completed")

    return model

def predict(model, future_df):
    # 调用模型进行预测
    forecast = model.predict(future_df)
    return forecast[['ds', 'yhat']]

def main(group_by_media=False, group_by_country=False):
    # 获取历史数据
    historical_data = getHistoricalData()

    # 获取所有媒体和国家的列表
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    countryList = ['GCC','JP','KR','T1','T2','T3','TW','US']
    
    medias = mediaList if group_by_media else [None]
    countries = countryList if group_by_country else [None]

    # 定义测试集范围
    test_start_date = '2024-07-01'
    test_end_date = '2024-10-07'

    # 初始化结果列表
    results = []

    # 按照分组进行遍历
    for media in medias:
        for country in countries:
            # 数据预处理
            df = preprocessData(historical_data, media, country)

            # 在测试集范围内逐天更新模型并进行预测
            for current_date in pd.date_range(start=test_start_date, end=test_end_date):
                # 训练集为从当前日期的前60天到前一天
                train_start_date = current_date - pd.Timedelta(days=60)
                train_df = df[(df['ds'] >= train_start_date) & (df['ds'] < current_date)]
                
                if len(train_df) < 30:
                    continue
                
                print(f'media: {media}, country: {country}')
                print(f"Training model for {current_date}...")
                # 训练模型
                model = train(train_df)
                
                # 预测当天的pud1_pct
                if current_date in df['ds'].values:
                    current_day_row = df[df['ds'] == current_date].iloc[0]
                    future_df = pd.DataFrame({
                        'ds': [current_date],
                        'ad_spend_pct': [current_day_row['ad_spend_pct']],
                        'is_weekend': [current_day_row['is_weekend']],
                        'y_lag_1': [current_day_row['y_lag_1']],
                        'y_lag_2': [current_day_row['y_lag_2']],
                        'y_lag_3': [current_day_row['y_lag_3']],
                        'cap': [1]
                    })
                    predictions = predict(model, future_df)
                    predicted_pud1_pct = predictions['yhat'].values[0]
                    real_pud1_pct = current_day_row['y']
                    results.append({
                        'date': current_date,
                        'media': media,
                        'country': country,
                        'predicted_pud1_pct': predicted_pud1_pct,
                        'real_pud1_pct': real_pud1_pct
                    })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算pud1_pct的MAPE
    results_df['mape'] = np.abs((results_df['real_pud1_pct'] - results_df['predicted_pud1_pct']) / (1 + results_df['real_pud1_pct'])) * 100

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date', 'media', 'country', 'mape']])

    # 输出到 CSV 文件
    group_name_str = f"{'media' if group_by_media else 'all'}_{'country' if group_by_country else 'all'}"
    output_filename = f'/src/data/pud1_pct_prediction_results_{group_name_str}.csv'
    results_df.to_csv(output_filename, index=False)

    print(f"Results DataFrame has been saved to {output_filename}")

    # 计算并输出pud1_pct的MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"所有pud1_pct的MAPE的平均值: {average_mape:.2f}%")

if __name__ == "__main__":
    # main(False, False)
    # main(True, False)
    # main(False, True)
    main(True, True)
