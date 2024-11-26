import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error


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

def prophetTest():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df = df.sort_values(by='install_day').reset_index(drop=True)
    # 用2024年7月1日到8月31日的数据训练
    train_df = df[(df['install_day'] >= '2024-07-01') & (df['install_day'] <= '2024-08-31')]
    test_df = df[(df['install_day'] >= '2024-09-01') & (df['install_day'] <= '2024-09-30')]
    # 输入cost，输出revenue


    ret = {}
    groupDf = train_df.groupby(['media', 'country'])
    for (media, country), group in groupDf:
        # if (media, country) not in [('ALL', 'ALL'),('GOOGLE', 'ALL')]:
        #     continue
        
        # 准备训练数据
        train_data = group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds', 'revenue': 'y'})
        
        # 初始化并训练Prophet模型
        model = Prophet()
        model.add_regressor('cost')
        model.fit(train_data)
        
        # 准备测试数据
        test_group = test_df[(test_df['media'] == media) & (test_df['country'] == country)]
        test_data = test_group[['install_day', 'cost', 'revenue']].rename(columns={'install_day': 'ds'})

        # 进行预测
        forecast = model.predict(test_data)

        retDf = pd.merge(test_data, forecast[['ds', 'yhat']], on='ds', how='left')
        retDf['mape'] = np.abs((retDf['revenue'] - retDf['yhat']) / retDf['revenue'])
        retDf.to_csv(f'/src/data/20241126_prophet_{media}_{country}.csv', index=False)

        # 计算MAPE
        # mape = mean_absolute_percentage_error(retDf['revenue'], retDf['yhat'])
        mape = retDf['mape'].mean()
        
        # 输出结果
        print(f'Media: {media}, Country: {country}, MAPE: {mape:.4f}')
        ret[(media, country)] = mape

    print(ret)


if __name__ == '__main__':
    prophetTest()