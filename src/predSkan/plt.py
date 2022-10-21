# 画一些图
import matplotlib.pyplot as plt

import pandas as pd
import sys
sys.path.append('/src')

from src.tools import getFilename

def totalCvR7():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7_%d.png'%(cv))
        print('save to /src/data/totalCvR7_%d.png'%(cv))

def totalCvR7Less1000():
    print('画图，cv count 小于1000，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 1000)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L1k_%d.png'%(cv))
        print('save to /src/data/totalCvR7L1k_%d.png'%(cv))

def totalCvR7Less50():
    print('画图，cv count 小于50，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 50)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L50_%d.png'%(cv))
        print('save to /src/data/totalCvR7L50_%d.png'%(cv))

# 过滤掉平均7日付费过高的数据，最高每人400usd
def totalCvR7F1():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    df.loc[df.sumr7usd > df['count'] * 400,'sumr7usd']=df['count']*400
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7F1_%d.png'%(cv))
        print('save to /src/data/totalCvR7F1_%d.png'%(cv))


if __name__ == '__main__':
    # totalCvR7()
    # totalCvR7Less1000()
    # totalCvR7Less50()
    totalCvR7F1()