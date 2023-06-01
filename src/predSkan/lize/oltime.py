# 为了发现在线时长（首日或48小时内）与用户质量的关系

import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')

from src.tools import getFilename

def corr1():
    dfRaw = pd.read_csv(getFilename('demoSsAll'))

    # print(df.corr()['r7usd'])

    dfRaw = dfRaw[['sp1','sp2','r7usd']]

    for i in range(1,24):
        
        h = 24 - i

        sp1 = h * 20
        sp2 = h * 40

        df = dfRaw.copy(deep = True)
        df.loc[df['sp1'] > sp1,'sp1'] = sp1
        df.loc[df['sp2'] > sp2,'sp2'] = sp2
        print('sp1上限：',h,'小时')
        print('sp2上限：',h*2,'小时')
        print(df.corr()['r7usd'][['sp1','sp2']])

# 在线时间与次登是否有关
def corr2():
    dfRaw = pd.read_csv(getFilename('demoSsAll'))
    dfRaw.loc[dfRaw['login2']>0,'login2'] = 1
    dfRaw.loc[dfRaw['login7']>0,'login7'] = 1

    dfRaw.loc[dfRaw['sp1'] > 240,'sp1'] = 240
    print(dfRaw.corr()['sp1'][['login2','login7']])

    dfRaw.loc[dfRaw['sp2'] > 480,'sp2'] = 480
    print(dfRaw.corr()['sp2'][['login2','login7']])
    

    # 筛选出 login2 == 0 和 login2 == 1 的数据
    df_login2_0 = dfRaw[dfRaw['login2'] == 0]
    df_login2_1 = dfRaw[dfRaw['login2'] == 1]

    # 创建一个新的图形
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    # 绘制上面的子图（login2 == 0）
    ax1.scatter(df_login2_0.index, df_login2_0['sp1'], s=1)
    ax1.set_title('login2 == 0')
    ax1.set_ylabel('sp1')

    # 绘制下面的子图（login2 == 1）
    ax2.scatter(df_login2_1.index, df_login2_1['sp1'], s=1)
    ax2.set_title('login2 == 1')
    ax2.set_xlabel('Index')
    ax2.set_ylabel('sp1')

    # 保存图片到指定路径
    plt.savefig('/src/data/corr2.jpg')



if __name__ == '__main__':
    # corr1()
    corr2()