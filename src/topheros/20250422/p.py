import sys
sys.path.append('/src')

from src.maxCompute import execSql

import os
import datetime
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from sklearn.metrics import mean_absolute_percentage_error

def getDataFromMC(startDayStr, endDayStr):
    filename = f'/src/data/th_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)

    else:
        sql = f'''
select
    install_day AS day,
    app_package as appid,
    sum(cost_value_usd) as cost,
    sum(revenue_d1) as r1usd,
    sum(revenue_d7) as r7usd,
    sum(revenue_d30) as r30usd,
    sum(revenue_d60) as r60usd,
    sum(revenue_d90) as r90usd,
    sum(revenue_d120) as r120usd,
    sum(revenue_d150) as r150usd,
    sum(revenue_d180) as r180usd,
    sum(revenue_d210) as r210usd,
    sum(revenue_d240) as r240usd,
    sum(revenue_d270) as r270usd
from
    dws_overseas_public_roi
where
    app = '116'
    and zone = '0'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package
;
        '''
        print('从MC获得数据')
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def main():
    startDayStr = '20240701'
    endDayStr = '20250424'

    df = getDataFromMC(startDayStr, endDayStr)
    df['day'] = pd.to_datetime(df['day'], format='%Y%m%d')

    # 先计算出可以完整获得1日、7日、30日……的最后日期
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')
    endDay1 = endDay - datetime.timedelta(days=1)
    endDay7 = endDay - datetime.timedelta(days=7)
    endDay30 = endDay - datetime.timedelta(days=30)
    endDay60 = endDay - datetime.timedelta(days=60)
    endDay90 = endDay - datetime.timedelta(days=90)
    endDay120 = endDay - datetime.timedelta(days=120)
    endDay150 = endDay - datetime.timedelta(days=150)
    endDay180 = endDay - datetime.timedelta(days=180)
    endDay210 = endDay - datetime.timedelta(days=210)
    endDay240 = endDay - datetime.timedelta(days=240)
    endDay270 = endDay - datetime.timedelta(days=270)

    print('endDay1:', endDay1)
    print('endDay7:', endDay7)
    print('endDay30:', endDay30)
    print('endDay60:', endDay60)
    print('endDay90:', endDay90)
    print('endDay120:', endDay120)
    print('endDay150:', endDay150)
    print('endDay180:', endDay180)
    print('endDay210:', endDay210)
    print('endDay240:', endDay240)
    print('endDay270:', endDay270)

    df = df[df['day'] <= endDay1].copy()
    df.loc[df['day']> endDay7,'r7usd'] = None
    df.loc[df['day']> endDay30,'r30usd'] = None
    df.loc[df['day']> endDay60,'r60usd'] = None
    df.loc[df['day']> endDay90,'r90usd'] = None
    df.loc[df['day']> endDay120,'r120usd'] = None
    df.loc[df['day']> endDay150,'r150usd'] = None
    df.loc[df['day']> endDay180,'r180usd'] = None
    df.loc[df['day']> endDay210,'r210usd'] = None
    df.loc[df['day']> endDay240,'r240usd'] = None
    df.loc[df['day']> endDay270,'r270usd'] = None

    df['month'] = df['day'].dt.strftime('%Y%m')
    
    def custom_sum(series):
        return series.sum() if not series.isnull().any() else None

    df = df.groupby(['month', 'appid']).agg({
        'cost': custom_sum,
        'r1usd': custom_sum,
        'r7usd': custom_sum,
        'r30usd': custom_sum,
        'r60usd': custom_sum,
        'r90usd': custom_sum,
        'r120usd': custom_sum,
        'r150usd': custom_sum,
        'r180usd': custom_sum,
        'r210usd': custom_sum,
        'r240usd': custom_sum,
        'r270usd': custom_sum
    }).reset_index()

    df = df.sort_values(by=['month', 'appid'])
    # print(df[df['appid'] == 'com.greenmushroom.boomblitz.gp'])    

    # appidList = df['appid'].unique()

    # for appid in appidList:
    #     print(f'appid: {appid}')
    #     appDf = df[df['appid'] == appid]
    #     appDf = appDf.sort_values(by=['month'])
    #     appDf.to_csv(f'/src/data/th2_{appid}_20240701_20250422.csv', index=False)
    #     print(appDf.corr())
    #     print('')

    # 不分app
    df2 = df.groupby(['month']).agg({
        'cost': custom_sum,
        'r1usd': custom_sum,
        'r7usd': custom_sum,
        'r30usd': custom_sum,
        'r60usd': custom_sum,
        'r90usd': custom_sum,
        'r120usd': custom_sum,
        'r150usd': custom_sum,
        'r180usd': custom_sum,
        'r210usd': custom_sum,
        'r240usd': custom_sum,
        'r270usd': custom_sum
    }).reset_index()
    df2 = df2.sort_values(by=['month'])
    df2.to_csv('/src/data/th2_20240701_20250424.csv', index=False)
    print('不分appid:')
    print(df2.corr())
    print('')

def predict():
    df = pd.read_csv('/src/data/th2_20240701_20250424.csv')
    df = df[['cost', 'r30usd', 'r60usd', 'r90usd', 'r120usd', 'r150usd', 'r180usd', 'r210usd', 'r240usd', 'r270usd']]
    
    df['roi30'] = df['r30usd'] / df['cost']
    df['roi60'] = df['r60usd'] / df['cost']
    df['roi90'] = df['r90usd'] / df['cost']
    df['roi120'] = df['r120usd'] / df['cost']
    df['roi150'] = df['r150usd'] / df['cost']
    df['roi180'] = df['r180usd'] / df['cost']
    df['roi210'] = df['r210usd'] / df['cost']
    df['roi240'] = df['r240usd'] / df['cost']
    df['roi270'] = df['r270usd'] / df['cost']

    df.to_csv('/src/data/th2_20240701_20250424_roi.csv', index=False)

    roiList = [
        df['roi30'].mean(),
        df['roi60'].mean(),
        df['roi90'].mean(),
        df['roi120'].mean(),
        df['roi150'].mean(),
        df['roi180'].mean(),
        df['roi210'].mean(),
        df['roi240'].mean()
    ]

    print('roiList:')
    print(roiList)

    # 基于roiList预测后面roi270,roi300, roi330, roi360
    # 使用对数模型拟合
    # x_data = np.array([30, 60, 90, 120, 150, 180, 210, 240])
    x_data = np.array([30, 60, 90, 120, 150, 180, 210])
    y_data = np.array(roiList)
    y_data = y_data[:len(x_data)]

    def log_model(x, a, b):
        return a * np.log(x) + b

    popt, _ = curve_fit(log_model, x_data, y_data)
    a, b = popt

    print(f'拟合参数: a = {a}, b = {b}')

    # 计算每个点的误差mape
    y_pred = log_model(x_data, a, b)
    mape_values = mean_absolute_percentage_error(y_data, y_pred)
    print(f'每个点的误差MAPE: {mape_values}')

    # 预测后续的roi270, roi300, roi330, roi360
    # future_x = np.array([270, 300, 330, 360])
    future_x = np.array([240, 270, 300, 330, 360, 390, 420, 450, 480, 510, 540, 570, 600, 630, 660, 690, 720])
    future_roi = log_model(future_x, a, b)
    print('预测的ROI:', future_roi)

    # 画图，x轴为时间，y轴为roi
    # 现有的roi 画一条线
    # 预测的roi 画一条线，包括前面拟合部分数据和后面预测部分数据
    x_data2 = np.concatenate((x_data, future_x))
    y_data2 = np.concatenate((y_data, future_roi))

    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set(style="darkgrid")
    plt.figure(figsize=(10, 6))
    plt.plot(x_data, y_data, 'o', label='real ROI')
    plt.plot(x_data2, log_model(x_data2, a, b), 'r-', label='fitted ROI')
    plt.xlabel('Days')
    plt.ylabel('ROI')
    plt.title('ROI Prediction')
    plt.legend()
    plt.xticks(np.arange(0, 400, 30))
    plt.grid(True)
    plt.savefig('/src/data/th2_20240701_20250424_roi_prediction.png')
    plt.close()
    

# 计算均值的MAPE
def meanMape():
    df = pd.read_csv('/src/data/th2_20240701_20250422_roi.csv')
    df = df[['roi30', 'roi60', 'roi90', 'roi120', 'roi150', 'roi180', 'roi210', 'roi240']]
    df['roi30mean'] = df['roi30'].mean()
    df['roi60mean'] = df['roi60'].mean()
    df['roi90mean'] = df['roi90'].mean()
    df['roi120mean'] = df['roi120'].mean()
    df['roi150mean'] = df['roi150'].mean()
    df['roi180mean'] = df['roi180'].mean()
    df['roi210mean'] = df['roi210'].mean()
    df['roi240mean'] = df['roi240'].mean()

    df['roi30mape'] = abs(df['roi30'] - df['roi30mean']) / df['roi30mean']
    df['roi60mape'] = abs(df['roi60'] - df['roi60mean']) / df['roi60mean']
    df['roi90mape'] = abs(df['roi90'] - df['roi90mean']) / df['roi90mean']
    df['roi120mape'] = abs(df['roi120'] - df['roi120mean']) / df['roi120mean']
    df['roi150mape'] = abs(df['roi150'] - df['roi150mean']) / df['roi150mean']
    df['roi180mape'] = abs(df['roi180'] - df['roi180mean']) / df['roi180mean']
    df['roi210mape'] = abs(df['roi210'] - df['roi210mean']) / df['roi210mean']
    df['roi240mape'] = abs(df['roi240'] - df['roi240mean']) / df['roi240mean']

    roi30mape = df['roi30mape'].mean()
    roi60mape = df['roi60mape'].mean()
    roi90mape = df['roi90mape'].mean()
    roi120mape = df['roi120mape'].mean()
    roi150mape = df['roi150mape'].mean()
    roi180mape = df['roi180mape'].mean()
    roi210mape = df['roi210mape'].mean()
    roi240mape = df['roi240mape'].mean()

    print('roi30mape:', roi30mape)
    print('roi60mape:', roi60mape)
    print('roi90mape:', roi90mape)
    print('roi120mape:', roi120mape)
    print('roi150mape:', roi150mape)
    print('roi180mape:', roi180mape)
    print('roi210mape:', roi210mape)
    print('roi240mape:', roi240mape)




def debug():
    df = pd.read_csv('/src/data/th2_20240701_20250422.csv')

    df = df[['cost', 'r30usd', 'r60usd', 'r90usd', 'r120usd', 'r150usd', 'r180usd', 'r210usd', 'r240usd', 'r270usd']]

    cols = df.columns.tolist()

    for i in range(len(cols)-1):
        col0 = cols[i]
        col1 = cols[i+1]

        # 计算比例
        df0 = df[[col0, col1]].copy()
        # 排除空值所在的行
        df0 = df0.dropna()

        if df0[col1].sum() == 0 or df0[col0].sum() == 0:
            continue

        # print(df0[col1].sum(), df0[col0].sum())

        ratio = df0[col1].sum() / df0[col0].sum()
        print(f'{col1} / {col0} = {ratio}')
        
        



if __name__ == '__main__':
    # main()
    # debug()
    predict()

    # meanMape()
