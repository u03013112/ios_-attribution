import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

import matplotlib.pyplot as plt

def getData():
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
        # 计算每列的MACD，并画图
        totalDf[col + '_EMA12'] = totalDf[col].ewm(span=12).mean()
        totalDf[col + '_EMA26'] = totalDf[col].ewm(span=26).mean()
        totalDf[col + '_DIF'] = totalDf[col + '_EMA12'] - totalDf[col + '_EMA26']
        totalDf[col + '_DEA'] = totalDf[col + '_DIF'].ewm(span=9).mean()
        totalDf[col + '_MACD'] = totalDf[col + '_DIF'] - totalDf[col + '_DEA']

        # 画图，3张图，竖着排列，x坐标是install_day
        # 第一张图，y坐标是col，col + '_EMA12'，col + '_EMA26'
        # 第二张图，y坐标是MACD 和 信号线
        # 第三张图，y坐标是柱状图
        # 保存图片到 /src/data/20241218_MACD_{col}.png
        # 创建图表
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(18, 18), sharex=True)

        # 第一张图，y坐标是col，col + '_EMA12'，col + '_EMA26'
        ax1.plot(totalDf['install_day'], totalDf[col], label=col, color='blue')
        ax1.plot(totalDf['install_day'], totalDf[col + '_EMA12'], label=col + ' EMA12', color='red')
        ax1.plot(totalDf['install_day'], totalDf[col + '_EMA26'], label=col + ' EMA26', color='green')
        ax1.set_title(f'{col} and EMAs')
        ax1.legend()

        # 第二张图，y坐标是MACD 和 信号线
        ax2.plot(totalDf['install_day'], totalDf[col + '_DIF'], label='MACD', color='blue')
        ax2.plot(totalDf['install_day'], totalDf[col + '_DEA'], label='Signal Line', color='red')
        ax2.set_title(f'{col} MACD and Signal Line')
        ax2.legend()

        # 第三张图，y坐标是柱状图
        ax3.bar(totalDf['install_day'], totalDf[col + '_MACD'], label='MACD Histogram', color='blue')
        ax3.set_title(f'{col} MACD Histogram')
        ax3.legend()

        # 设置x轴标签
        ax3.set_xlabel('Install Day')

        # 保存图片到 /src/data/20241218_MACD_{col}.png
        plt.tight_layout()
        plt.savefig(f'/src/data/20241218_MACD_{col}.png')
        plt.close()

if __name__ == '__main__':
    main()


