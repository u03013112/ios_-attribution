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

def allMape():
    arppuDf = pd.read_csv('/src/data/arppu_prediction_results_all_all.csv')
    arppuDf = arppuDf[['date', 'predicted_arppu']]

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_all_all.csv')
    pud1Df = pud1Df[['date', 'predicted_pud1_pct']]

    predictedDf = pd.merge(arppuDf, pud1Df, on='date', how='inner')
    predictedDf['date'] = pd.to_datetime(predictedDf['date'], format='%Y-%m-%d')

    realDf = getHistoricalData()
    realDf = realDf.groupby('install_day').agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()
    realDf['last_pud1'] = realDf['pud1'].shift(1)
    realDf['arppu'] = realDf['d1'] / realDf['pud1']
    realDf.rename(columns={'install_day': 'date'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    df = pd.merge(predictedDf, realDf, on= ['date'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] *(1 + df['predicted_pud1_pct']) * df['last_pud1']
    df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
    df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
    df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
    
    mape = df['mape'].mean()
    pud1_mape = df['pud1_mape'].mean()
    arppu_mape = df['arppu_mape'].mean()
    
    print(df)
    print('all MAPE:', mape)
    print('all pud1 MAPE:', pud1_mape)
    print('all arppu MAPE:', arppu_mape)

def allMape2():
    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_all_all.csv')
    pud1Df = pud1Df[['date', 'predicted_pud1_pct']]
    pud1Df['date'] = pd.to_datetime(pud1Df['date'], format='%Y-%m-%d')

    realDf = getHistoricalData()
    realDf = realDf.groupby('install_day').agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()
    realDf['arppu'] = realDf['d1'] / realDf['pud1']
    realDf['last_pud1'] = realDf['pud1'].shift(1)
    realDf['predicted_arppu'] = realDf['arppu'].shift(1).rolling(15).mean()

    realDf.rename(columns={'install_day': 'date'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    df = pd.merge(pud1Df, realDf, on= ['date'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] *(1 + df['predicted_pud1_pct']) * df['last_pud1']
    df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
    df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
    df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
    
    mape = df['mape'].mean()
    pud1_mape = df['pud1_mape'].mean()
    arppu_mape = df['arppu_mape'].mean()
    
    print(df)
    print('all MAPE:', mape)
    print('all pud1 MAPE:', pud1_mape)
    print('all arppu MAPE:', arppu_mape)



def mediaMape():
    # 读取分媒体的预测结果
    arppuDf = pd.read_csv('/src/data/arppu_prediction_results_media_all.csv')
    arppuDf = arppuDf[['date', 'media', 'predicted_arppu']]

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_media_all.csv')
    pud1Df = pud1Df[['date', 'media', 'predicted_pud1_pct']]

    # 合并预测结果
    predictedDf = pd.merge(arppuDf, pud1Df, on=['date', 'media'], how='inner')
    predictedDf['date'] = pd.to_datetime(predictedDf['date'], format='%Y-%m-%d')

    # 获取真实数据
    realDf = getHistoricalData()
    realDf = realDf.groupby(['install_day', 'mediasource']).agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()
    realDf['last_pud1'] = realDf.groupby('mediasource')['pud1'].shift(1)
    realDf['arppu'] = realDf['d1'] / realDf['pud1']

    realDf.rename(columns={'install_day': 'date', 'mediasource': 'media'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    # 合并预测和真实数据
    df = pd.merge(predictedDf, realDf, on=['date', 'media'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
    df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
    df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
    df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
    
    # 计算并输出分媒体的MAPE
    media_mape = df.groupby('media').agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Media MAPE:')
    print(media_mape)

    # # 保存结果到CSV文件
    # media_mape.to_csv('/src/data/media_mape_results.csv', index=False)
    # print('Media MAPE results have been saved to /src/data/media_mape_results.csv')

def countryMape():
    # 读取分国家的预测结果
    arppuDf = pd.read_csv('/src/data/arppu_prediction_results_all_country.csv')
    arppuDf = arppuDf[['date', 'country', 'predicted_arppu']]

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_all_country.csv')
    pud1Df = pud1Df[['date', 'country', 'predicted_pud1_pct']]

    # 合并预测结果
    predictedDf = pd.merge(arppuDf, pud1Df, on=['date', 'country'], how='inner')
    predictedDf['date'] = pd.to_datetime(predictedDf['date'], format='%Y-%m-%d')

    # 获取真实数据
    realDf = getHistoricalData()
    realDf = realDf.groupby(['install_day', 'country']).agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()
    realDf['last_pud1'] = realDf.groupby('country')['pud1'].shift(1)
    realDf['arppu'] = realDf['d1'] / realDf['pud1']
    realDf.rename(columns={'install_day': 'date'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    # 合并预测和真实数据
    df = pd.merge(predictedDf, realDf, on=['date', 'country'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
    df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
    df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
    df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
    
    # 计算并输出分国家的MAPE
    country_mape = df.groupby('country').agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Country MAPE:')
    print(country_mape)

    # # 保存结果到CSV文件
    # country_mape.to_csv('/src/data/country_mape_results.csv', index=False)
    # print('Country MAPE results have been saved to /src/data/country_mape_results.csv')

def mediaAndCountryMape():
    # 读取分媒体和国家的预测结果
    arppuDf = pd.read_csv('/src/data/arppu_prediction_results_media_country.csv')
    arppuDf = arppuDf[['date', 'media', 'country', 'predicted_arppu']]

    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_media_country.csv')
    pud1Df = pud1Df[['date', 'media', 'country', 'predicted_pud1_pct']]

    # 合并预测结果
    predictedDf = pd.merge(arppuDf, pud1Df, on=['date', 'media', 'country'], how='inner')
    predictedDf['date'] = pd.to_datetime(predictedDf['date'], format='%Y-%m-%d')

    # 获取真实数据
    realDf = getHistoricalData()
    realDf = realDf.groupby(['install_day', 'mediasource', 'country']).agg({
        'd1': 'sum',
        'pud1': 'sum',
    }).reset_index()
    realDf['last_pud1'] = realDf.groupby(['mediasource', 'country'])['pud1'].shift(1)
    realDf['arppu'] = realDf['d1'] / realDf['pud1']
    realDf.rename(columns={'install_day': 'date', 'mediasource': 'media'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    # 合并预测和真实数据
    df = pd.merge(predictedDf, realDf, on=['date', 'media', 'country'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
    df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
    df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
    df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
    
    # 计算并输出分媒体和国家的MAPE
    media_country_mape = df.groupby(['media', 'country']).agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Media and Country MAPE:')
    print(media_country_mape)

    # # 保存结果到CSV文件
    # media_country_mape.to_csv('/src/data/media_country_mape_results.csv', index=False)
    # print('Media and Country MAPE results have been saved to /src/data/media_country_mape_results.csv')


if __name__ == "__main__":
    allMape()
    mediaMape()
    countryMape()
    mediaAndCountryMape()

    # allMape2()
