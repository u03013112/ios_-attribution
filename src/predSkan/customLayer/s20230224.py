# 尝试将数据进行平均化

import pandas as pd


# 先查看一下每天媒体与非媒体的首日付费金额占比
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame


def r1usdMediaVsOther():
    afDf = pd.read_csv(getFilename('afDataR7C_20221001_20230201'))
    
    df = afDf.groupby(['install_date','media_group'],as_index=False).agg({
        'sumr1usd':'sum'
    }).sort_values(by=['install_date'])

    mediaDf = df.loc[df.media_group != 'unknown']
    otherDf = df.loc[df.media_group == 'unknown']

    mergeDf = pd.merge(mediaDf,otherDf,on = ['install_date'],suffixes=('_media','_other'))
    mergeDf['media_vs_other'] = mergeDf['sumr1usd_media']/mergeDf['sumr1usd_other']

    return mergeDf

def getWeight(df):
    # 只保留一位小数，即每10%列为一个档次
    df['media_vs_other'] = df['media_vs_other'].round(1)
    # print(df)
    count = df['media_vs_other'].value_counts(ascending=True,normalize=True)
    # count = df['media_vs_other'].value_counts(bins=20,normalize=True)

    # print(count)
    # print(type(count))
    countDf = pd.DataFrame({
        'media_vs_other':count.index,
        'counts':count.values
    })
    mergeDf = pd.merge(df,countDf,how = 'left',on=['media_vs_other'])
    # print(mergeDf)
    mergeDf['weight'] = 1/mergeDf['counts']
    return mergeDf['weight'].to_numpy()

import numpy as np
if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = r1usdMediaVsOther()
        df.to_csv(getFilename('s20230224'))
    df = pd.read_csv(getFilename('s20230224'))
    w = getWeight(df)
    print(w)
    np.save('/src/data/s20230224.npy',w)
    