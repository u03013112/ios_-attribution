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

def preprocessData(original_data, capped_data0, media=None, country=None):
    # 转换 'install_day' 列为日期格式
    original_data['install_day'] = pd.to_datetime(original_data['install_day'], format='%Y%m%d')
    capped_data0['date'] = pd.to_datetime(capped_data0['date'], format='%Y%m%d')

    countryGroupList = [
        {'name':'T1', 'countries':['AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','GB','MO','IL']},
        {'name':'T2', 'countries':['BG','BV','BY','EE','ES','GL','GR','HU','ID','KZ','LT','LV','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA']},
        {'name':'T3', 'countries':['AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','HU','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','RS','SM','SR','UA','UY','XK']},
        {'name':'GCC', 'countries':['SA','AE','QA','KW','BH','OM']},
        {'name':'US', 'countries':['US']},
        {'name':'JP', 'countries':['JP']},
        {'name':'KR', 'countries':['KR']},
        {'name':'TW', 'countries':['TW']}
    ]

    capped_data = capped_data0.copy()
    capped_data['country_group'] = 'Other'
    for group in countryGroupList:
        for country0 in group['countries']:
            capped_data.loc[capped_data['country'] == country0, 'country_group'] = group['name']
    
    capped_data['country'] = capped_data['country_group']

    # 过滤数据
    if media:
        original_data = original_data[original_data['mediasource'] == media]
        capped_data = capped_data[capped_data['mediasource'] == media]
    if country:
        original_data = original_data[original_data['country'] == country]
        capped_data = capped_data[capped_data['country'] == country]

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = original_data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum',
        'ins': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 按 'date' 分组并汇总削弱大R版本的数据
    capped_aggregated_data = capped_data.groupby('date').agg({
        '24hours_revenue_capped': 'sum'
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
    df = df.rename(columns={'date': 'ds'})

    # 添加是否为周末的特征
    df['is_weekend'] = df['ds'].dt.dayofweek >= 5

    # 移除含NaN的行
    df = df.dropna()

    # 合并削弱大R版本的数据
    capped_aggregated_data = capped_aggregated_data.rename(columns={'date': 'ds', '24hours_revenue_capped': 'y'})
    df = df.merge(capped_aggregated_data, on='ds', how='left')

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

def main(N=400,group_by_media=False, group_by_country=False):
    # 获取历史数据
    historical_data = getHistoricalData()
    historical_data2 = getHistoricalData2('20240401', '20241015', N)

    # 获取所有媒体和国家的列表
    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int']
    countryList = ['T1', 'T2', 'T3', 'TW', 'US','GCC', 'JP', 'KR']
    
    medias = mediaList if group_by_media else [None]
    countries = countryList if group_by_country else [None]

    # 定义测试集范围
    test_start_date = '2024-07-01'
    test_end_date = '2024-10-07'

    # 初始化结果列表
    results = []

    group_name_str = f"{'media' if group_by_media else 'all'}_{'country' if group_by_country else 'all'}"
    # 按照分组进行遍历
    for media in medias:
        for country in countries:
            # 数据预处理
            df = preprocessData(historical_data, historical_data2, media, country)

            pud1Df = pd.read_csv(f'/src/data/pud1_pct_prediction_results_{group_name_str}.csv')
            if media:
                pud1Df = pud1Df[pud1Df['media'] == media]
            if country:
                pud1Df = pud1Df[pud1Df['country'] == country]

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

            # 在测试集范围内逐天更新模型并进行预测
            for current_date in pd.date_range(start=test_start_date, end=test_end_date):
                # 训练集为从当前日期的前60天到前一天
                train_start_date = current_date - pd.Timedelta(days=60)
                train_df = df[(df['ds'] >= train_start_date) & (df['ds'] < current_date)]
                
                if len(train_df) < 30:
                    continue
                
                print(f'media: {media}, country: {country}')
                print(f"Training model for {current_date}...")
                print(train_df)

                # 训练模型
                model = train(train_df)
                
                # 预测当天的收入
                if current_date in df['ds'].values:
                    current_day_row = df[df['ds'] == current_date].iloc[0]
                    future_df = pd.DataFrame({
                        'ds': [current_date],
                        'pud1': [current_day_row['pud1 prediction']],
                        'is_weekend': [current_day_row['is_weekend']]
                    })
                    predictions = predict(model, future_df)
                    predicted_revenue = predictions['yhat'].values[0]
                    real_revenue = current_day_row['y']
                    results.append({
                        'date': current_date,
                        'media': media,
                        'country': country,
                        'predicted_revenue': predicted_revenue,
                        'real_revenue': real_revenue
                    })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算收入的MAPE
    results_df['mape'] = np.abs((results_df['real_revenue'] - results_df['predicted_revenue']) / (1 + results_df['real_revenue'])) * 100

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date', 'media', 'country', 'mape']])

    # 输出到 CSV 文件
    output_filename = f'/src/data/revenue_prediction_results_{group_name_str}{N}.csv'
    results_df.to_csv(output_filename, index=False)

    print(f"Results DataFrame has been saved to {output_filename}")

    # 计算并输出收入的MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"所有收入的MAPE的平均值: {average_mape:.2f}%")

def test(N):
    # 读取所有组合的结果文件
    df_all_all = pd.read_csv(f'/src/data/revenue_prediction_results_all_all{N}.csv')
    df_all_country = pd.read_csv(f'/src/data/revenue_prediction_results_all_country{N}.csv')
    df_media_all = pd.read_csv(f'/src/data/revenue_prediction_results_media_all{N}.csv')
    df_media_country = pd.read_csv(f'/src/data/revenue_prediction_results_media_country{N}.csv')

    # 计算并输出所有数据的平均MAPE
    mape_all_all = df_all_all['mape'].mean()
    print(f"mape_all_all: {mape_all_all:.2f}%")

    # 计算并输出按国家分组的平均MAPE
    print("MAPE by country:")
    mape_by_country = df_all_country.groupby('country')['mape'].mean()
    for country, mape in mape_by_country.items():
        print(f"{country}: {mape:.2f}%")

    # 计算并输出按媒体分组的平均MAPE
    print("MAPE by media:")
    mape_by_media = df_media_all.groupby('media')['mape'].mean()
    for media, mape in mape_by_media.items():
        print(f"{media}: {mape:.2f}%")

    # 计算并输出按媒体和国家分组的平均MAPE
    print("MAPE by media and country:")
    mape_by_media_country = df_media_country.groupby(['media', 'country'])['mape'].mean()
    for (media, country), mape in mape_by_media_country.items():
        print(f"{media} - {country}: {mape:.2f}%") 



if __name__ == "__main__":
    main(5000,False, False)
    main(5000,True, False)
    main(5000,False, True)
    main(5000,True, True)

    test(5000)

