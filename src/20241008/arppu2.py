import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

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

    # 按 'install_day'分组并汇总所需列
    aggregated_data = data.groupby(['install_day']).agg({
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

    # 计算ARPPU
    df['arppu'] = df['revenue'] / df['pud1']

    # 计算前3日的ARPPU
    df['arppu_T_1'] = df['arppu'].shift(1)
    df['arppu_T_2'] = df['arppu'].shift(2)
    df['arppu_T_3'] = df['arppu'].shift(3)

    # 修改列名以适应Prophet模型
    df.rename(columns={
        'date': 'ds',
        'arppu': 'y'
    }, inplace=True)

    df = df.dropna()

    return df

def train(train_df):
    # 准备Prophet所需的数据格式
    prophet_train_df = train_df[['ds', 'y', 'ad_spend', 'is_weekend', 'arppu_T_1', 'arppu_T_2', 'arppu_T_3']].copy()

    # 移除含NaN的行
    prophet_train_df = prophet_train_df.dropna()

    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('ad_spend')
    model.add_regressor('is_weekend')
    model.add_regressor('arppu_T_1')
    model.add_regressor('arppu_T_2')
    model.add_regressor('arppu_T_3')
    model.fit(prophet_train_df)

    return model

def predict(model, future_df):
    # 调用模型进行预测
    forecast = model.predict(future_df)
    return forecast['yhat'].values[0]

def main(group_by_media=False, group_by_country=False):
    # 获取历史数据
    historical_data = getHistoricalData()

    # print(historical_data['mediasource'].unique())
    # print(historical_data['country'].unique())
    # mediaList = ['Facebook Ads','Organic','Playhouse','Twitter','Web-GP','Web-PR','acemobi_int','applovin_int','bytedanceglobal_int','flmobi_int','googleadwords_int','snapchat_int','unityads_int','KR-KROOH','gnacompany_int','yahoojapan_int','tiktoklive_int','smartnewsads_int','KOL','Web','None','Playable ads','metaweb_int','appsflyer_sdk_test_int','W2A']
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    countryList = ['GCC','JP','KR','OTHER','T1','T2','T3','TW','US']

    # 获取所有媒体和国家的列表
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
                
                # 训练模型
                model = train(train_df)
                
                # 预测当天的ARPPU
                if current_date in df['ds'].values:
                    current_day_row = df[df['ds'] == current_date].iloc[0]
                    future_df = pd.DataFrame({
                        'ds': [current_date],
                        'ad_spend': [current_day_row['ad_spend']],
                        'is_weekend': [current_day_row['is_weekend']],
                        'arppu_T_1': [current_day_row['arppu_T_1']],
                        'arppu_T_2': [current_day_row['arppu_T_2']],
                        'arppu_T_3': [current_day_row['arppu_T_3']]
                    })
                    predicted_arppu = predict(model, future_df)
                    real_arppu = current_day_row['revenue'] / current_day_row['pud1']
                    results.append({
                        'date': current_date,
                        'media': media,
                        'country': country,
                        'predicted_arppu': predicted_arppu,
                        'real_arppu': real_arppu
                    })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算ARPPU的MAPE
    results_df['arppu_mape'] = np.abs((results_df['real_arppu'] - results_df['predicted_arppu']) / results_df['real_arppu']) * 100

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date', 'media', 'country', 'arppu_mape']])

    # 输出到 CSV 文件
    group_name_str = f"{'media' if group_by_media else 'all'}_{'country' if group_by_country else 'all'}"
    output_filename = f'/src/data/arppu_prediction_results_{group_name_str}.csv'
    results_df.to_csv(output_filename, index=False)

    print(f"Results DataFrame has been saved to {output_filename}")

    # 计算并输出ARPPU的MAPE的平均值
    average_arppu_mape = results_df['arppu_mape'].mean()
    print(f"所有ARPPU的MAPE的平均值: {average_arppu_mape:.2f}%")

if __name__ == "__main__":
    # main()
    # main(True, False)
    # main(False, True)
    main(True, True)
