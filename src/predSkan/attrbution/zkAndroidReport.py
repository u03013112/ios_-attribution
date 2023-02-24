import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')

from src.tools import getFilename

# 暂时只看着3个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    # 'unknown'
]

# 分别针对每一种idfa配合去中位数
def report0():

    
    for idfa in (0.2,0.3):
        totalDf = pd.DataFrame()
        for i in range(20):
            # zkr0.300000_19

            filename = 'zkr%.1f00000_%d'%(idfa,i)
            df = pd.read_csv(getFilename(filename))

            totalDf = totalDf.append(df,ignore_index=True)

        medianDf = totalDf.groupby(['install_date','media'],as_index=False).agg({
            # 'r7usd_real':'median',
            # 'r7usd_predict':'median',
            'r7usd_real':'mean',
            'r7usd_predict':'mean',
        })

        medianDf.loc[:,'mape'] = 0
        medianDf['mape'] = (medianDf['r7usd_real'] - medianDf['r7usd_predict'])/medianDf['r7usd_real']
        medianDf.loc[medianDf.mape <0,'mape'] *= -1

        medianDf.to_csv(getFilename('zkr%.2f'%idfa))

        medianDf.set_index(["install_date"], inplace=True)
        for media in mediaList:
            mediaDf = medianDf.loc[medianDf.media == media]
            mape = mediaDf['mape'].mean()
            corr = mediaDf.corr()['r7usd_real']['r7usd_predict']
            print(idfa,media,'mape:',mape,'corr:',corr)

            plt.title("%s 7day revenue"%(media))
            plt.figure(figsize=(10.8, 3.2))
            mediaDf['r7usd_real'].plot(label='real')
            mediaDf['r7usd_predict'].plot(label='predict')

            plt.xticks(rotation=45)
            plt.legend(loc='best')
            plt.tight_layout()
            plt.savefig('/src/data/zk%s.png'%(media))
            plt.clf()
    return

if __name__ == '__main__':
    report0()