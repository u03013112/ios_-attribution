# 优化和尝试预估arppu

import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getData():
    filename = '/src/data/20241126_data.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
SELECT
    install_day,
    media,
    country,
    cost,
    pu_1d as pu,
    revenue_1d as revenue,
    actual_arppu
FROM
    lastwar_predict_day1_pu_pct_by_cost_pct__nerfr_historical_data2
WHERE
    day BETWEEN '20240801' AND '20241031'
    and platform = 'android'
    and group_name = 'g1__all'
    and max_r = 10000000000
;
        '''
        print("执行的SQL语句如下：\n")
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

def rawALL():
    df = getData()
    df = df[(df['media'] == 'ALL') & (df['country'] == 'ALL')]

    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)

    print(df)
    df.to_csv('/src/data/20241126_data_ALL.csv', index=False)
    
def corr():
    df = getData()
    groupedDf = df.groupby(['media', 'country'])
    for (media, country), group in groupedDf:
        print('\n>>',media, country)
        print(group.corr()[['cost','pu','revenue','actual_arppu']])

def rollingN():
    df = getData()
    groupedDf = df.groupby(['media', 'country'])
    for (media, country), group in groupedDf:
        print('\n>>',media, country)

        group = group.sort_values(by='install_day')
        group['arppu7'] = group['actual_arppu'].shift(1).rolling(window=7).mean()
        group['arppu15'] = group['actual_arppu'].shift(1).rolling(window=15).mean()
        group['arppu30'] = group['actual_arppu'].shift(1).rolling(window=30).mean()
        group = group.dropna()

        # print(group)
        # print(group.corr()[['cost','actual_arppu']])

        # 计算MAPE
        group['mape7'] = np.abs((group['actual_arppu'] - group['arppu7']) / group['actual_arppu'])
        group['mape15'] = np.abs((group['actual_arppu'] - group['arppu15']) / group['actual_arppu'])
        group['mape30'] = np.abs((group['actual_arppu'] - group['arppu30']) / group['actual_arppu'])

        # print(group[['mape7','mape15','mape30']].mean())
        print(group[['mape15']].mean())

def rollingN2():
    df = getData()
    groupedDf = df.groupby(['media', 'country'])
    for (media, country), group in groupedDf:
        print('\n>>',media, country)

        group = group.sort_values(by='install_day')
        
        group['pu7'] = group['pu'].shift(1).rolling(window=7).mean()
        group['pu15'] = group['pu'].shift(1).rolling(window=15).mean()
        group['pu30'] = group['pu'].shift(1).rolling(window=30).mean()

        group['revenue7'] = group['revenue'].shift(1).rolling(window=7).mean()
        group['revenue15'] = group['revenue'].shift(1).rolling(window=15).mean()
        group['revenue30'] = group['revenue'].shift(1).rolling(window=30).mean()

        group['arppu7'] = group['revenue7'] / group['pu7']
        group['arppu15'] = group['revenue15'] / group['pu15']
        group['arppu30'] = group['revenue30'] / group['pu30']

        group = group.dropna()

        # print(group)
        print(group.corr()[['cost','actual_arppu']])

        # 计算MAPE
        group['mape7'] = np.abs((group['actual_arppu'] - group['arppu7']) / group['actual_arppu'])
        group['mape15'] = np.abs((group['actual_arppu'] - group['arppu15']) / group['actual_arppu'])
        group['mape30'] = np.abs((group['actual_arppu'] - group['arppu30']) / group['actual_arppu'])

        print(group[['mape7','mape15','mape30']].mean())
        
def rollingN3():
    df = getData()
    groupedDf = df.groupby(['media', 'country'])
    for (media, country), group in groupedDf:
        print('\n>>',media, country)

        group = group.sort_values(by='install_day')
        
        for N in [1,3,7,14,21,28]:
            group[f'arppu{N}'] = group['actual_arppu'].shift(1).rolling(window=N).mean()
            group = group.dropna()

            group[f'mape{N}'] = np.abs((group['actual_arppu'] - group[f'arppu{N}']) / group['actual_arppu'])

            print(f'{N}日均值MAPE：',group[[f'mape{N}']].mean())
    

# 这种方案效果不好
def week():
    df = getData()
    groupedDf = df.groupby(['media', 'country'])
    for (media, country), group in groupedDf:
        print('\n>>',media, country)
        
        group['install_day'] = pd.to_datetime(group['install_day'], format='%Y%m%d')
        group = group.sort_values(by='install_day')
        group['week'] = group['install_day'].dt.strftime('%Y-%W')

        weekDf = group.groupby('week').agg({
            'cost': 'sum',
            'pu': 'sum',
            'revenue': 'sum',
        }).reset_index()
        weekDf['arppu'] = weekDf['revenue'] / weekDf['pu']
        weekDf['arppu1'] = weekDf['arppu'].shift(1)
        weekDf['arppu2'] = weekDf['arppu'].shift(1).rolling(window=2).mean()
        weekDf['arppu4'] = weekDf['arppu'].shift(1).rolling(window=4).mean()

        weekDf = weekDf.dropna()
        # print(weekDf)
        print(weekDf.corr()[['cost','arppu']])

        # 计算MAPE
        weekDf['mape1'] = np.abs((weekDf['arppu'] - weekDf['arppu1']) / weekDf['arppu'])
        weekDf['mape2'] = np.abs((weekDf['arppu'] - weekDf['arppu2']) / weekDf['arppu'])
        weekDf['mape4'] = np.abs((weekDf['arppu'] - weekDf['arppu4']) / weekDf['arppu'])

        print(weekDf[['mape1','mape2','mape4']].mean())


if __name__ == '__main__':
    # rawALL()
    # corr()
    # rollingN()
    # rollingN2()
    rollingN3()
    # week()
