import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

import matplotlib.pyplot as plt
from sklearn.model_selection import ParameterGrid

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


def calculate_macd(data, col, short_window, long_window, signal_window):
    data[col + '_EMA' + str(short_window)] = data[col].ewm(span=short_window).mean()
    data[col + '_EMA' + str(long_window)] = data[col].ewm(span=long_window).mean()
    data[col + '_DIF'] = data[col + '_EMA' + str(short_window)] - data[col + '_EMA' + str(long_window)]
    data[col + '_DEA'] = data[col + '_DIF'].ewm(span=signal_window).mean()
    data[col + '_MACD'] = data[col + '_DIF'] - data[col + '_DEA']
    return data

def backtest_macd(data, col, short_window, long_window, signal_window):
    data = calculate_macd(data.copy(), col, short_window, long_window, signal_window)
    
    # 计算信号变化
    data['MACD_Signal'] = np.where(data[col + '_MACD'] > 0, 1, -1)
    data['Signal_Change'] = data['MACD_Signal'].diff().abs() / 2  # 计算信号变化次数
    
    # 计算信号变化的平均周期
    signal_changes = data['Signal_Change'].sum()
    total_days = data.shape[0]
    if signal_changes == 0:
        average_period = float('inf')  # 如果没有信号变化，设为无穷大
    else:
        average_period = total_days / signal_changes
    
    # 计算与目标周期的差异
    target_period = 7
    period_diff = abs(average_period - target_period)
    
    return period_diff

def main():
    df = getData()

    # # for test
    # df = df[(df['country'] == 'US') & (df['mediasource'] == 'googleadwords_int')]

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
    totalDf = totalDf.sort_values('install_day', ascending=True)
    totalDf.dropna(inplace=True)

    totalDf.to_csv('/src/data/20241218_MACD_total.csv', index=False)

    param_grid = {
        'short_window': range(5, 15),
        'long_window': range(16, 30),
        'signal_window': range(5, 15)
    }

    for col in cols:
        short_window = 7
        long_window = 14
        signal_window = 7

        if short_window == 0:
            best_params = None
            best_performance = float('inf')

            for params in ParameterGrid(param_grid):
                performance = backtest_macd(totalDf, col, params['short_window'], params['long_window'], params['signal_window'])
                if performance < best_performance:
                    best_performance = performance
                    best_params = params

            print(f"最佳参数 for {col}: {best_params}")
            print(f"最佳表现 for {col}: {best_performance}")

            short_window = best_params['short_window']
            long_window = best_params['long_window']
            signal_window = best_params['signal_window']

        # 使用最佳参数计算MACD
        if col == 'ROI_d7':
            print('ROI_d7')
            print(totalDf.tail(10))
            totalDf_trimmed = totalDf.iloc[:-7]  # 去掉最后7天的数据
            totalDf_trimmed = calculate_macd(totalDf_trimmed, col, short_window, long_window, signal_window)
        else:
            totalDf = calculate_macd(totalDf, col, short_window, long_window, signal_window)

        # 画图，3张图，竖着排列，x坐标是install_day
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(26, 18), sharex=True)

        # 第一张图，y坐标是col，col + '_EMA12'，col + '_EMA26'
        ax1.plot(totalDf['install_day'], totalDf[col], label=col, color='blue')
        ax1.plot(totalDf['install_day'], totalDf[col + '_EMA' + str(short_window)], label=col + f' EMA{short_window}', color='red')
        ax1.plot(totalDf['install_day'], totalDf[col + '_EMA' + str(long_window)], label=col + f' EMA{long_window}', color='green')
        ax1.set_title(f'{col} and EMAs')
        ax1.legend()
        ax1.grid(True)  # 添加网格线

        # 第二张图，y坐标是MACD 和 信号线
        if col == 'ROI_d7':
            ax2.plot(totalDf_trimmed['install_day'], totalDf_trimmed[col + '_DIF'], label=f'DIF={short_window}EMA-{long_window}EMA', color='blue')
            ax2.plot(totalDf_trimmed['install_day'], totalDf_trimmed[col + '_DEA'], label=f'DIF_EMA{signal_window}', color='red')
        else:
            ax2.plot(totalDf['install_day'], totalDf[col + '_DIF'], label=f'DIF={short_window}EMA-{long_window}EMA', color='blue')
            ax2.plot(totalDf['install_day'], totalDf[col + '_DEA'], label=f'DIF_EMA{signal_window}', color='red')
        ax2.axhline(y=0, color='black', linestyle='--')  # 添加 y=0 的横线
        ax2.set_title(f'{col} MACD and Signal Line')
        ax2.legend()
        ax2.grid(True)  # 添加网格线

        # 第三张图，y坐标是柱状图
        if col == 'ROI_d7':
            ax3.bar(totalDf_trimmed['install_day'], totalDf_trimmed[col + '_MACD'], label='MACD Histogram', color='blue')
        else:
            ax3.bar(totalDf['install_day'], totalDf[col + '_MACD'], label='MACD Histogram', color='blue')
        ax3.axhline(y=0, color='black', linestyle='--')  # 添加 y=0 的横线
        ax3.set_title(f'{col} MACD Histogram')
        ax3.legend()
        ax3.grid(True)  # 添加网格线

        # 设置x轴标签
        ax3.set_xlabel('Install Day')

        # 保存图片到 /src/data/20241218_MACD_{col}.png
        plt.tight_layout()
        plt.savefig(f'/src/data/20241218_MACD_{col}.png')
        plt.close()

if __name__ == '__main__':
    main()
