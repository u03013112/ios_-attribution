# 尝试一下，预测arppu
# 思路，使用现有分两组的数据，即2美元以下和2美元以上
# 预测付费用户占比，然后再根据前15日分组内的平均arppu，计算出arppu
# 然后计算此估算方法的mape，与之前方法做对比

import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

from prophet import Prophet

def getData():
    filename = '/src/data/20241126_predictArppu2Data.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
SELECT
    install_day,
    media,
    country,
    group_name,
    pay_user_group_name,
    cost,
    pu_1d as pu,
    revenue_1d as revenue,
    actual_arppu
FROM
    lastwar_predict_day1_pu_pct_by_cost_pct__nerfr_historical_data2
WHERE
    day BETWEEN '20240701' AND '20241031'
    and platform = 'android'
    and group_name = 'g2__2'
    and max_r = 10000000000
;
        '''
        print("执行的SQL语句如下：\n")
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def train():
    df = getData()
    
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    
    groupDf = train_df.groupby(['media', 'country'])

    models = {}
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
            # continue

        print('\n>>',media, country)
        
        # 计算每日的pay_user_group_name == '0_2' 的pu占比
        total_pu = group.groupby('install_day')['pu'].sum().reset_index()
        pu_0_2 = group[group['pay_user_group_name'] == '0_2'].groupby('install_day')['pu'].sum().reset_index()
        
        pu_0_2 = pu_0_2.rename(columns={'pu': 'pu_0_2'})
        merged = pd.merge(total_pu, pu_0_2, on='install_day', how='left')
        merged['pu_0_2'] = merged['pu_0_2'].fillna(0)
        merged['pu_pct'] = merged['pu_0_2'] / merged['pu']
        
        # 准备Prophet模型的数据
        prophet_df = merged[['install_day', 'pu_pct']].rename(columns={'install_day': 'ds', 'pu_pct': 'y'})
        

        # 初始化并训练Prophet模型
        model = Prophet()
        model.fit(prophet_df)
        
        # 保存模型
        models[(media, country)] = model
    
    return models
        

def test(models):
    df = getData()

    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    groupDf = df.groupby(['media', 'country'])

    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
            # continue

        print('\n>>',media, country)

        # 按照install_day升序排序
        group = group.sort_values(by='install_day')

        # 计算arppu
        group['arppu'] = group['revenue'] / group['pu']

        model = models[(media, country)]
        future_dates = pd.date_range(start='2024-08-01', end='2024-10-31')
        future = pd.DataFrame({'ds': future_dates})
        forecast = model.predict(future)
        forecast = forecast[['ds', 'yhat']].rename(columns={'ds': 'install_day', 'yhat': 'predicted_pu_pct'})
        # print('forecast:')
        # print(forecast)

        df1 = group[group['pay_user_group_name'] == '0_2'].copy()
        df1['arppu_avg'] = df1['arppu'].shift(1).rolling(window=15, min_periods=1).mean()
        df1 = df1[df1['install_day'] >= '2024-08-01']
        df1 = pd.merge(df1, forecast, on='install_day', how='left')

        df2 = group[group['pay_user_group_name'] == '2_inf'].copy()
        df2['arppu_avg'] = df2['arppu'].shift(1).rolling(window=15, min_periods=1).mean()
        df2 = df2[df2['install_day'] >= '2024-08-01']
        df2 = pd.merge(df2, forecast, on='install_day', how='left')
        df2['predicted_pu_pct'] = 1 - df2['predicted_pu_pct']

        group = pd.concat([df1, df2])
        # print('group:')
        # print(group[group['install_day'] == '2024-08-01'])

        group['predicted_arppu'] = group['predicted_pu_pct'] * group['arppu_avg']
        group = group.groupby('install_day').agg({
            'pu': 'sum',
            'revenue': 'sum',
            'predicted_arppu': 'sum'
        }).reset_index()
        group['actual_arppu'] = group['revenue'] / group['pu']

        # 计算mape
        group['mape'] = np.abs((group['actual_arppu'] - group['predicted_arppu']) / group['actual_arppu'])
        # print('ret:')
        # print(group)
        overall_mape = group['mape'].mean()
        print(f'Overall MAPE: {overall_mape}')


if __name__ == '__main__':

    models = train()
    test(models)