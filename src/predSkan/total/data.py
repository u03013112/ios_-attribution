# 数据分析
# 主要针对相关性
# 分析一下各种处理方法与7日真实回收之间的相关性
import pandas as pd


import sys
sys.path.append('/src')
from src.tools import getFilename

from sklearn.metrics import r2_score

def r1():
    df4 = pd.read_csv(getFilename('totalData_20220501_20220930'))
    dfSum = df4.groupby('install_date').agg({
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    }).sort_values(by=['install_date'])
    # print(dfSum)
    print(dfSum.corr('kendall'))

    print(r2_score(dfSum['sumr1usd'], dfSum['sumr7usd']))


def roll():
    df4 = pd.read_csv(getFilename('totalData_20220501_20220930'))
    dfSum = df4.groupby('install_date').agg({
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    }).sort_values(by=['install_date'])
    for r in [3,5,7]:
        dfSum['roll%d'%r] = (dfSum.sumr7usd.rolling(r).mean())
    print(dfSum)
    print(dfSum.corr('kendall'))

    # print(r2_score(dfSum['sumr1usd'], dfSum['sumr7usd']))

def ema():
    df4 = pd.read_csv(getFilename('totalData_20220501_20220930'))
    dfSum = df4.groupby('install_date').agg({
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    }).sort_values(by=['install_date'])
    for r in [3,5,7]:
        dfSum['ema%d'%r] = (dfSum.sumr7usd.ewm(span=r).mean())
    print(dfSum)
    print(dfSum.corr('kendall'))

def max():
    df4 = pd.read_csv(getFilename('totalData_20220501_20220930'))
    dfSum = df4.groupby('install_date').agg({
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    }).sort_values(by=['install_date'])

    for max in [200,500]:
        df4 = pd.read_csv(getFilename('totalData_20220501_20220930_%.2f'%max))
        dfSum2 = df4.groupby('install_date').agg({
            'sumr1usd':'sum',
            'sumr7usd':'sum'
        }).sort_values(by=['install_date'])
        dfSum['max%d'%max] = dfSum2['sumr7usd']
    
    print(dfSum)
    print(dfSum.corr('kendall'))
    print(dfSum.corr())

if __name__ == '__main__':
    # r1()
    # roll()
    # ema()
    max()