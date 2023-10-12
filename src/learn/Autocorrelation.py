# 自相关系数是一种统计学概念，用于度量同一变量在不同时间点的数值之间的相关性。在时间序列数据分析中，自相关系数常常被用来检测数据中的随机性。计算自相关系数时，通常会考虑数据在不同的时间滞后（lag）下的相关性。例如，如果我们想要检查每天的气温是否与前一天的气温有关，我们就可以计算一阶滞后的自相关系数。
# 自相关系数的取值范围在-1到1之间，接近1表示强正相关，接近-1表示强负相关，接近0则表示没有相关性。
# 在一些统计分析和机器学习的模型中，例如ARIMA模型或者LSTM神经网络模型，自相关系数是非常重要的一个指标。

import numpy as np
import pandas as pd

df = pd.read_csv('/src/data/androidUserPostback_20220101_20230401.csv')
df = df[['appsflyer_id','install_date','r1usd','r7usd','media']]

# media 列中 restricted -> Facebook Ads
df['media'] = df['media'].replace('restricted','Facebook Ads')

groupDf = df.groupby(['install_date','media']).agg({
    'r1usd':'sum',
    'r7usd':'sum'
}).reset_index()

groupDf['r7/r1'] = groupDf['r7usd']/groupDf['r1usd']

mediaList = [
    'Facebook Ads',
    'bytedanceglobal_int',
    'googleadwords_int'
]

for media in mediaList:
    print('media:',media)
    mediaDf = groupDf[groupDf['media']==media].sort_values(by='install_date')

    mediaDf['r1usd rolling7'] = mediaDf['r1usd'].rolling(7).sum()
    mediaDf['r7usd rolling7'] = mediaDf['r7usd'].rolling(7).sum()
    mediaDf['r7/r1 rolling7'] = mediaDf['r7usd rolling7']/mediaDf['r1usd rolling7']

    for lag in [1,7,14]:
        print('lag:',lag)
        print('r1 与 r7 的相关系数:',mediaDf['r1usd'].corr(mediaDf['r7usd']))
        print('r1 自相关系数:',mediaDf['r1usd'].autocorr(lag=lag))
        print('r7 自相关系数:',mediaDf['r7usd'].autocorr(lag=lag))
        print('r7/r1 自相关系数:',mediaDf['r7/r1'].autocorr(lag=lag))

        print('r1 rolling7 与 r7 rolling7 的相关系数:',mediaDf['r1usd rolling7'].corr(mediaDf['r7usd rolling7']))
        print('r1 rolling7 自相关系数:',mediaDf['r1usd rolling7'].autocorr(lag=lag))
        print('r7 rolling7 自相关系数:',mediaDf['r7usd rolling7'].autocorr(lag=lag))
        print('r7/r1 rolling7 自相关系数:',mediaDf['r7/r1 rolling7'].autocorr(lag=lag))