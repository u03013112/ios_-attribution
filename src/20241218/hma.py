import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getData():
    filename = '/src/data/20241218_data.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
SELECT 
    install_day,
    country,
    mediasource,
    sum(impression) as impression,
    sum(click) as click,
    sum(installs) as installs,
    sum(cost_value_usd) as cost_value_usd,
    sum(revenue_d1) as revenue_d1,
    sum(revenue_d3) as revenue_d3,
    sum(revenue_d7) as revenue_d7,
    sum(payusers_d1) as payusers_d1,
    sum(payusers_d3) as payusers_d3,
    sum(payusers_d7) as payusers_d7,
    sum(retention_1) as retention_1,
    sum(retention_2) as retention_2,
    sum(retention_6) as retention_6
FROM rg_bi.dws_overseas_public_roi
where 
    app = '502' 
    and facebook_segment in ('country', 'N/A') 
    and app_package = 'com.fun.lastwar.gp'
    and install_day between '20240101' and '20241231'
group by 
    install_day,
    country,
    mediasource
;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def calculate_hma(data, col, window=20):
    half_length = int(window / 2)
    sqrt_length = int(np.sqrt(window))
    data['WMA_half'] = data[col].rolling(window=half_length).mean()
    data['WMA_full'] = data[col].rolling(window=window).mean()
    data['HMA'] = 2 * data['WMA_half'] - data['WMA_full']
    data['HMA'] = data['HMA'].rolling(window=sqrt_length).mean()
    return data

def calculate_ema(data, col, span=20):
    data['EMA'] = data[col].ewm(span=span, adjust=False).mean()
    return data

def main():
    df = getData()

    totalDf = df.groupby(['install_day']).agg({
        'impression': 'sum',
        'click': 'sum',
        'installs': 'sum',
        'cost_value_usd': 'sum',
        'revenue_d1': 'sum',
        'revenue_d3': 'sum',
        'revenue_d7': 'sum',
        'payusers_d1': 'sum',
        'payusers_d3': 'sum',
        'payusers_d7': 'sum',
        'retention_1': 'sum',
        'retention_2': 'sum',
        'retention_6': 'sum'
    }).reset_index()

    totalDf['CPM'] = totalDf['cost_value_usd'] / totalDf['impression'] * 1000
    totalDf['CPI'] = totalDf['cost_value_usd'] / totalDf['installs']
    totalDf['ROI_d7'] = totalDf['revenue_d7'] / totalDf['cost_value_usd']

    cols = ['CPM', 'CPI', 'ROI_d7']

    totalDf = totalDf[['install_day'] + cols]
    totalDf['install_day'] = pd.to_datetime(totalDf['install_day'], format='%Y%m%d')
    totalDf = totalDf.sort_values('install_day', ascending=False)

    for col in cols:
        # totalDf = calculate_hma(totalDf, col, window=7)
        totalDf = calculate_ema(totalDf, col, span=7)

        # 画图，x坐标是install_day
        plt.figure(figsize=(18, 9))

        # 绘制原始数据和HMA
        plt.plot(totalDf['install_day'], totalDf[col], label=col, color='blue', alpha=0.3)
        # plt.plot(totalDf['install_day'], totalDf['HMA'], label='HMA', color='red')
        plt.plot(totalDf['install_day'], totalDf['EMA'], label='EMA', color='green')

        # 设置标题和标签
        # plt.title(f'{col} and HMA')
        plt.title(f'{col} and EMA')
        plt.xlabel('Install Day')
        plt.ylabel(col)
        plt.legend()
        plt.grid(True)  # 添加网格线

        # 保存图片到 /src/data/20241218_HMA_{col}.png
        plt.tight_layout()
        # plt.savefig(f'/src/data/20241218_HMA_{col}.png')
        plt.savefig(f'/src/data/20241218_EMA_{col}.png')
        plt.close()

if __name__ == '__main__':
    main()
