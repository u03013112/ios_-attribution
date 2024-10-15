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

def calculateRollingARPPU(df, N=15):
    df['rolling_revenue'] = df['d1'].shift(1).rolling(window=N).mean()
    df['rolling_pud1'] = df['pud1'].shift(1).rolling(window=N).mean()
    df['predicted_arppu'] = df['rolling_revenue'] / df['rolling_pud1']
    df = df.dropna(subset=['predicted_arppu'])
    return df

def allMape():
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

    realDf.rename(columns={'install_day': 'date'}, inplace=True)
    realDf['date'] = pd.to_datetime(realDf['date'], format='%Y%m%d')

    # 计算滑动平均 ARPPU
    realDf = calculateRollingARPPU(realDf)

    df = pd.merge(pud1Df, realDf, on=['date'], how='left')
    df['real_revenue'] = df['d1']
    df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
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
    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_media_all.csv')
    pud1Df = pud1Df[['date', 'media', 'predicted_pud1_pct']]
    pud1Df['date'] = pd.to_datetime(pud1Df['date'], format='%Y-%m-%d')

    realDf = getHistoricalData()
    media_list = realDf['mediasource'].unique()
    
    results = []
    for media in media_list:
        media_realDf = realDf[realDf['mediasource'] == media]
        media_realDf = media_realDf.groupby('install_day').agg({
            'd1': 'sum',
            'pud1': 'sum',
        }).reset_index()
        media_realDf['arppu'] = media_realDf['d1'] / media_realDf['pud1']
        media_realDf['last_pud1'] = media_realDf['pud1'].shift(1)
        media_realDf.rename(columns={'install_day': 'date'}, inplace=True)
        media_realDf['date'] = pd.to_datetime(media_realDf['date'], format='%Y%m%d')
        
        # 计算滑动平均 ARPPU
        media_realDf = calculateRollingARPPU(media_realDf)
        
        media_pud1Df = pud1Df[pud1Df['media'] == media]
        df = pd.merge(media_pud1Df, media_realDf, on=['date'], how='left')
        df['real_revenue'] = df['d1']
        df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
        df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
        df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
        df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
        
        results.append(df)
    
    final_df = pd.concat(results)
    media_mape = final_df.groupby('media').agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Media MAPE:')
    print(media_mape)

def countryMape():
    # 读取分国家的预测结果
    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_all_country.csv')
    pud1Df = pud1Df[['date', 'country', 'predicted_pud1_pct']]
    pud1Df['date'] = pd.to_datetime(pud1Df['date'], format='%Y-%m-%d')

    realDf = getHistoricalData()
    country_list = realDf['country'].unique()
    
    results = []
    for country in country_list:
        country_realDf = realDf[realDf['country'] == country]
        country_realDf = country_realDf.groupby('install_day').agg({
            'd1': 'sum',
            'pud1': 'sum',
        }).reset_index()
        country_realDf['arppu'] = country_realDf['d1'] / country_realDf['pud1']
        country_realDf['last_pud1'] = country_realDf['pud1'].shift(1)
        country_realDf.rename(columns={'install_day': 'date'}, inplace=True)
        country_realDf['date'] = pd.to_datetime(country_realDf['date'], format='%Y%m%d')
        
        # 计算滑动平均 ARPPU
        country_realDf = calculateRollingARPPU(country_realDf)
        
        country_pud1Df = pud1Df[pud1Df['country'] == country]
        df = pd.merge(country_pud1Df, country_realDf, on=['date'], how='left')
        df['real_revenue'] = df['d1']
        df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
        df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
        df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
        df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
        
        results.append(df)
    
    final_df = pd.concat(results)
    country_mape = final_df.groupby('country').agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Country MAPE:')
    print(country_mape)

def mediaAndCountryMape():
    # 读取分媒体和国家的预测结果
    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results_media_country.csv')
    pud1Df = pud1Df[['date', 'media', 'country', 'predicted_pud1_pct']]
    pud1Df['date'] = pd.to_datetime(pud1Df['date'], format='%Y-%m-%d')

    realDf = getHistoricalData()
    media_country_list = realDf.groupby(['mediasource', 'country']).size().reset_index()[['mediasource', 'country']]
    
    results = []
    for _, row in media_country_list.iterrows():
        media = row['mediasource']
        country = row['country']
        media_country_realDf = realDf[(realDf['mediasource'] == media) & (realDf['country'] == country)]
        media_country_realDf = media_country_realDf.groupby('install_day').agg({
            'd1': 'sum',
            'pud1': 'sum',
        }).reset_index()
        media_country_realDf['arppu'] = media_country_realDf['d1'] / media_country_realDf['pud1']
        media_country_realDf['last_pud1'] = media_country_realDf['pud1'].shift(1)
        media_country_realDf.rename(columns={'install_day': 'date'}, inplace=True)
        media_country_realDf['date'] = pd.to_datetime(media_country_realDf['date'], format='%Y%m%d')
        
        # 计算滑动平均 ARPPU
        media_country_realDf = calculateRollingARPPU(media_country_realDf)
        
        media_country_pud1Df = pud1Df[(pud1Df['media'] == media) & (pud1Df['country'] == country)]
        df = pd.merge(media_country_pud1Df, media_country_realDf, on=['date'], how='left')
        df['real_revenue'] = df['d1']
        df['predicted_revenue'] = df['predicted_arppu'] * (1 + df['predicted_pud1_pct']) * df['last_pud1']
        df['mape'] = np.abs(df['real_revenue'] - df['predicted_revenue']) / df['real_revenue']
        df['pud1_mape'] = np.abs(df['pud1'] - (1 + df['predicted_pud1_pct']) * df['last_pud1']) / df['pud1']
        df['arppu_mape'] = np.abs(df['arppu'] - df['predicted_arppu']) / df['arppu']
        
        results.append(df)
    
    final_df = pd.concat(results)
    media_country_mape = final_df.groupby(['media', 'country']).agg({
        'mape': 'mean',
        'pud1_mape': 'mean',
        'arppu_mape': 'mean'
    }).reset_index()
    
    print('Media and Country MAPE:')
    print(media_country_mape)

if __name__ == "__main__":
    allMape()
    mediaMape()
    countryMape()
    mediaAndCountryMape()
