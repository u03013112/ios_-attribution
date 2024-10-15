import os
import pandas as pd
import numpy as np

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

    return df

def main(group_by_media=False, group_by_country=False, N=15):
    # 获取历史数据
    historical_data = getHistoricalData()

    # 定义媒体和国家列表
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    countryList = ['GCC','JP','KR','OTHER','T1','T2','T3','TW','US']

    # 获取所有媒体和国家的列表
    medias = mediaList if group_by_media else [None]
    countries = countryList if group_by_country else [None]

    # 初始化结果列表
    results = []

    # 按照分组进行遍历
    for media in medias:
        for country in countries:
            # 数据预处理
            df = preprocessData(historical_data, media, country)

            # 计算滑动平均
            df['rolling_revenue'] = df['revenue'].shift(1).rolling(window=N).mean()
            df['rolling_pud1'] = df['pud1'].shift(1).rolling(window=N).mean()

            # 计算滑动平均后的 ARPPU
            df['predicted_arppu'] = df['rolling_revenue'] / df['rolling_pud1']

            # 计算真实的 ARPPU
            df['real_arppu'] = df['revenue'] / df['pud1']

            # # 计算滑动平均后的 ARPPU
            # df['predicted_arppu'] = df['real_arppu'].shift(1).rolling(window=N).mean()

            # 过滤掉前 N 天的数据，因为它们没有足够的历史数据进行滑动平均
            df = df.dropna(subset=['predicted_arppu'])

            # 计算 ARPPU 的 MAPE
            df['arppu_mape'] = np.abs((df['real_arppu'] - df['predicted_arppu']) / df['real_arppu']) * 100

            # 将结果添加到结果列表
            for _, row in df.iterrows():
                results.append({
                    'date': row['date'],
                    'media': media,
                    'country': country,
                    'predicted_arppu': row['predicted_arppu'],
                    'real_arppu': row['real_arppu'],
                    'arppu_mape': row['arppu_mape']
                })

    # 转换为 DataFrame
    results_df = pd.DataFrame(results)

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date', 'media', 'country', 'arppu_mape']])

    # 输出到 CSV 文件
    group_name_str = f"{'media' if group_by_media else 'all'}_{'country' if group_by_country else 'all'}"
    output_filename = f'/src/data/arppu_prediction_results_{group_name_str}_rolling{N}.csv'
    results_df.to_csv(output_filename, index=False)

    print(f"Results DataFrame has been saved to {output_filename}")

    # 计算并输出 ARPPU 的 MAPE 的平均值
    average_arppu_mape = results_df['arppu_mape'].mean()
    print(f"所有 ARPPU 的 MAPE 的平均值: {average_arppu_mape:.2f}%")

if __name__ == "__main__":
    main()
    main(True, False)
    main(False, True)
    main(True, True, N=15)
