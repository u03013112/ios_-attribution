import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getData():
    filename = '/src/data/20241126_data_20240101_20241231.csv'
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
    day BETWEEN '20240101' AND '20241231'
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

def corr():
    df = getData()

    totalDf = df.groupby(['install_day']).agg({
        'cost': 'sum',
        'revenue': 'sum',
    }).reset_index()
    
    totalDf['roi'] = totalDf['revenue'] / totalDf['cost']

    print(totalDf.corr())

def corrWeek():
    df = getData()

    totalDf = df.groupby(['install_day']).agg({
        'cost': 'sum',
        'revenue': 'sum',
    }).reset_index()
    
    totalDf['install_day'] = pd.to_datetime(totalDf['install_day'], format='%Y%m%d')
    totalDf['week'] = totalDf['install_day'].dt.strftime('%Y-%W')

    totalDf = totalDf.groupby('week').agg({
        'cost': 'sum',
        'revenue': 'sum',
    }).reset_index()

    totalDf['roi'] = totalDf['revenue'] / totalDf['cost']
    totalDf['last_week_roi'] = totalDf['roi'].shift(1)
    totalDf['last_week_cost'] = totalDf['cost'].shift(1)
    totalDf['last_week_revenue'] = totalDf['revenue'].shift(1)

    print(totalDf.corr())

if __name__ == '__main__':
    # corr()
    corrWeek()