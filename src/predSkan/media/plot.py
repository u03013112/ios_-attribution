import matplotlib.pyplot as plt
import datetime
import pandas as pd
import sys
sys.path.append('/src')

from src.tools import getFilename

def mediaR1R7():
    df = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    mediaGroups = df['media_group'].unique()
    for mediaGroup in mediaGroups:
        mediaDf = df.loc[df.media_group == mediaGroup]
        mediaDf = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'})

        plt.title("media %s r1/r7"%mediaGroup) 
        plt.xlabel("date")
        plt.ylabel("usd")
        mediaDf['sumr1usd'].plot(label='r1usd')
        mediaDf['sumr7usd'].plot(label='r7usd')
        mediaDf['count'].plot(label='count')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.savefig('/src/data/media_%s.png'%mediaGroup)
        print('save to /src/data/media_%s.png'%mediaGroup)
        plt.clf()
        print(list(mediaDf['sumr1usd'].to_numpy().reshape(-1)))
        print(list(mediaDf['sumr7usd'].to_numpy().reshape(-1)))

if __name__ == '__main__':
    mediaR1R7()

