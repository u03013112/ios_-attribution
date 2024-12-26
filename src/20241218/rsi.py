import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import ParameterGrid

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


def calculate_rsi(data, col, window):
    delta = data[col].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    data[col + '_RSI'] = rsi
    return data

def calculate_bollinger_bands(data, col, window, num_std_dev):
    rolling_mean = data[col].rolling(window=window).mean()
    rolling_std = data[col].rolling(window=window).std()
    data[col + '_Bollinger_Mid'] = rolling_mean
    data[col + '_Bollinger_Upper'] = rolling_mean + (rolling_std * num_std_dev)
    data[col + '_Bollinger_Lower'] = rolling_mean - (rolling_std * num_std_dev)
    return data

def backtest_rsi_bollinger(data, col, rsi_window, rsi_threshold, bollinger_window, num_std_dev):
    data = calculate_rsi(data.copy(), col, rsi_window)
    data = calculate_bollinger_bands(data, col, bollinger_window, num_std_dev)
    
    # 生成交易信号
    data['Signal'] = 0
    data.loc[(data[col + '_RSI'] < rsi_threshold) & (data[col] < data[col + '_Bollinger_Lower']), 'Signal'] = 1  # 买入信号
    data.loc[(data[col + '_RSI'] > 100 - rsi_threshold) & (data[col] > data[col + '_Bollinger_Upper']), 'Signal'] = -1  # 卖出信号
    
    # 计算持仓状态
    data['Position'] = data['Signal'].shift()  # 持仓状态
    data['Position'].fillna(0, inplace=True)
    
    # 计算收益
    data['Returns'] = data[col].pct_change()  # 价格变化百分比
    data['Strategy_Returns'] = data['Returns'] * data['Position']  # 策略收益
    
    # 计算累积收益
    cumulative_returns = data['Strategy_Returns'].cumsum().iloc[-1]
    return cumulative_returns, data

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

    param_grid = {
        'rsi_window': range(10, 21, 2),
        'rsi_threshold': range(30, 71, 10),
        'bollinger_window': range(10, 21, 2),
        'num_std_dev': [1, 2, 3]
    }

    for col in cols:
        best_params = None
        best_performance = -np.inf
        best_data = None

        for params in ParameterGrid(param_grid):
            performance, data_with_signals = backtest_rsi_bollinger(totalDf, col, params['rsi_window'], params['rsi_threshold'], params['bollinger_window'], params['num_std_dev'])
            if performance > best_performance:
                best_performance = performance
                best_params = params
                best_data = data_with_signals

        print(f"最佳参数 for {col}: {best_params}")
        print(f"最佳表现 for {col}: {best_performance}")

        # 使用最佳参数计算RSI和布林带
        totalDf = calculate_rsi(totalDf, col, best_params['rsi_window'])
        totalDf = calculate_bollinger_bands(totalDf, col, best_params['bollinger_window'], best_params['num_std_dev'])

        # 画图，3张图，竖着排列，x坐标是install_day
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(18, 18), sharex=True)

        # 第一张图，y坐标是col，布林带上下轨和中轨
        ax1.plot(totalDf['install_day'], totalDf[col], label=col, color='blue')
        ax1.plot(totalDf['install_day'], totalDf[col + '_Bollinger_Mid'], label='Bollinger Mid', color='red')
        ax1.plot(totalDf['install_day'], totalDf[col + '_Bollinger_Upper'], label='Bollinger Upper', color='green')
        ax1.plot(totalDf['install_day'], totalDf[col + '_Bollinger_Lower'], label='Bollinger Lower', color='green')
        ax1.set_title(f'{col} and Bollinger Bands')
        ax1.legend()
        ax1.grid(True)  # 添加网格线

        # 第二张图，y坐标是RSI
        ax2.plot(totalDf['install_day'], totalDf[col + '_RSI'], label='RSI', color='blue')
        ax2.axhline(30, color='red', linestyle='--')
        ax2.axhline(70, color='red', linestyle='--')
        ax2.set_title(f'{col} RSI')
        ax2.legend()
        ax2.grid(True)  # 添加网格线

        # 第三张图，y坐标是策略信号
        ax3.plot(best_data['install_day'], best_data['Signal'], label='Signal', color='blue')
        ax3.set_title(f'{col} Trading Signal')
        ax3.legend()
        ax3.grid(True)  # 添加网格线

        # 设置x轴标签
        ax3.set_xlabel('Install Day')

        # 保存图片到 /src/data/20241218_RSI_Bollinger_{col}.png
        plt.tight_layout()
        plt.savefig(f'/src/data/20241218_RSI_Bollinger_{col}.png')
        plt.close()

if __name__ == '__main__':
    main()