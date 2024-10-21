# 用花费金额预测收入金额，按周进行预测，计算每周的MAPE
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

def corr():
    data = getHistoricalData()
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum',
        'ins': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 按日期排序
    aggregated_data = aggregated_data.sort_values('install_day', ascending=True)

    # 移除含NaN的行
    aggregated_data = aggregated_data.dropna()

    # 计算相关系数
    print(aggregated_data.corr())

def corrMedia():
    data = getHistoricalData()
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')

    # 按 'mediasource' 分组
    grouped = data.groupby('mediasource')

    # 初始化一个字典来存储每个媒体来源的相关系数
    corr_dict = {}

    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int','Twitter','snapchat_int']

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_media_all.csv')

    for name, group in grouped:
        if name not in mediaList:
            continue
        # 按 'install_day' 分组并汇总所需列
        aggregated_data = group.groupby('install_day').agg({
            'usd': 'sum',
            'd1': 'sum',
            'ins': 'sum',
            'pud1': 'sum',
        }).reset_index()

        pud1DfMedia = pud1Df[pud1Df['media'] == name]
        pud1DfMedia = pud1DfMedia[['date','predicted_pud1_pct','real_pud1_pct']]
        pud1DfMedia.rename(columns={'date':'install_day'},inplace=True)
        pud1DfMedia['install_day'] = pd.to_datetime(pud1DfMedia['install_day'], format='%Y-%m-%d')

        aggregated_data = aggregated_data.merge(pud1DfMedia, on='install_day', how='left')


        # 按日期排序
        aggregated_data = aggregated_data.sort_values('install_day', ascending=True)
        aggregated_data['last pud1'] = aggregated_data['pud1'].shift(1)
        aggregated_data['pud1 real'] = (1 + aggregated_data['real_pud1_pct']) * aggregated_data['last pud1']
        aggregated_data['pud1 predicted'] = (1 + aggregated_data['predicted_pud1_pct']) * aggregated_data['last pud1']

        # 移除含NaN的行
        aggregated_data = aggregated_data.dropna()

        # 计算相关系数
        corr_matrix = aggregated_data.corr()

        # 将相关系数矩阵存储在字典中
        corr_dict[name] = corr_matrix

    # 打印每个媒体来源的相关系数矩阵
    for media, corr_matrix in corr_dict.items():
        print(f"Media Source: {media}")
        print(corr_matrix)
        print("\n")

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

def main(group_by_media=False, group_by_country=False):
    # 获取历史数据
    historical_data = getHistoricalData()

    # # 获取所有媒体和国家的列表
    # mediaList = historical_data['mediasource'].unique() if group_by_media else [None]
    # print(mediaList)
    
    # countryList = historical_data['country'].unique() if group_by_country else [None]
    # 获取所有媒体和国家的列表
    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US'] if group_by_country else [None]

    # 初始化结果列表
    results = []

    # 按照分组进行遍历
    for media in mediaList:
        for country in countryList:
            # 数据预处理
            df = preprocessData(historical_data, media, country)

            test_start_date = '2024-07-01'
            test_end_date = '2024-10-07'

            # 按周训练和预测
            current_date = pd.to_datetime(test_start_date)
            end_date = pd.to_datetime(test_end_date)

            while current_date <= end_date:
                # 定义训练集
                train_start_date = current_date - pd.Timedelta(days=60)
                train_df = df[(df['ds'] >= train_start_date) & (df['ds'] < current_date)]
                
                if len(train_df) < 30:
                    current_date += pd.Timedelta(days=7)
                    continue

                # 训练模型
                model = train(train_df)

                # 定义未来7天的预测集
                future_dates = pd.date_range(start=current_date, periods=7)
                future_df = df[df['ds'].isin(future_dates)][['ds', 'ad_spend', 'is_weekend', 'y']].copy()

                if not future_df.empty:
                    # 预测
                    predictions = predict(model, future_df)
                    predictions['country'] = country if country else 'all'
                    predictions['media'] = media if media else 'all'

                    # 合并预测结果与测试数据
                    future_df = future_df.merge(predictions, on='ds', how='left')

                    # 添加结果到列表
                    results.append(future_df)

                # 移动到下一周
                current_date += pd.Timedelta(days=7)

    # 合并所有结果
    results_df = pd.concat(results)

    # 计算每周的真实收入与预测收入
    results_df['week'] = results_df['ds'].dt.isocalendar().week
    weekly_results = results_df.groupby(['week','country','media']).agg({
        'y': 'sum',
        'yhat': 'sum'
    }).reset_index()

    # 计算每周的MAPE
    weekly_results['mape'] = np.abs((weekly_results['y'] - weekly_results['yhat']) / (1 + weekly_results['y'])) * 100

    # 输出查询表单
    print("Weekly Results DataFrame:")
    print(weekly_results[['week', 'yhat', 'y', 'mape']])

    # 输出到 CSV 文件
    group_name_str = f"{'media' if group_by_media else 'all'}_{'country' if group_by_country else 'all'}"
    output_filename = f'/src/data/weekly_revenue_{group_name_str}.csv'
    weekly_results.to_csv(output_filename, index=False)

    print(f"Weekly Results DataFrame has been saved to {output_filename}")

    # 计算并输出每周收入的MAPE的平均值
    average_mape = weekly_results['mape'].mean()
    print(f"每周收入的MAPE的平均值: {average_mape:.2f}%")

def test():
    # 读取所有组合的结果文件
    df_all_all = pd.read_csv('/src/data/weekly_revenue_all_all.csv')
    df_all_country = pd.read_csv('/src/data/weekly_revenue_all_country.csv')
    df_media_all = pd.read_csv('/src/data/weekly_revenue_media_all.csv')
    df_media_country = pd.read_csv('/src/data/weekly_revenue_media_country.csv')

    # 计算并输出所有数据的平均MAPE
    mape_all_all = df_all_all['mape'].mean()
    print(f"mape_all_all: {mape_all_all:.2f}%")

    # 计算并输出按媒体分组的平均MAPE
    print("MAPE by media:")
    mape_by_media = df_media_all.groupby('media')['mape'].mean()
    for media, mape in mape_by_media.items():
        print(f"{media}: {mape:.2f}%")

    # 计算并输出按国家分组的平均MAPE
    print("MAPE by country:")
    mape_by_country = df_all_country.groupby('country')['mape'].mean()
    for country, mape in mape_by_country.items():
        print(f"{country}: {mape:.2f}%")

    # 计算并输出按媒体和国家分组的平均MAPE
    print("MAPE by media and country:")
    mape_by_media_country = df_media_country.groupby(['media', 'country'])['mape'].mean()
    for (media, country), mape in mape_by_media_country.items():
        print(f"{media} - {country}: {mape:.2f}%") 


if __name__ == "__main__":
    # main(False, False)
    # main(True, False)
    # main(False, True)
    # main(True, True)

    # test()

    # corr()
    corrMedia()