# 尝试划线
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import sys
sys.path.append('/src')

from src.tools import getFilename

# roi相关
def roi():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    plt.title("7day roi")
    plt.figure(figsize=(10.8, 3.2))
    df['install_day'] = df['install_day'].astype('string')
    df.set_index(["install_day"], inplace=True)

    # 在这里开始划线
    df['roi'].plot(alpha = 0.3,label='roi')
    # 暂时设定本年所有kpi都是0.06
    df['kpi'] = 0.06
    df['kpi'].plot(label='kpi')
    df['kpi0.9'] = df['kpi']*.9
    df['kpi0.9'].plot(label='kpi0.9')
    df['kpi1.1'] = df['kpi']*1.1
    df['kpi1.1'].plot(label='kpi1.1')
    
    # 为了向后取滚动平均值，需要先将时序倒序过来
    dfR = df.sort_values(by=['install_day'],ascending = False).reset_index()
    # 先做一个3日的
    dfR['cost3'] = dfR['cost'].rolling(window=3).mean()
    dfR['r7usd3']= dfR['r7usd'].rolling(window=3).mean()
    dfR['roi3'] = dfR['r7usd3']/dfR['cost3']
    # 再做一个7日的
    dfR['cost7'] = dfR['cost'].rolling(window=7).mean()
    dfR['r7usd7']= dfR['r7usd'].rolling(window=7).mean()
    dfR['roi7'] = dfR['r7usd7']/dfR['cost7']
    # 再将时间升序，然后划线
    dfR = dfR.sort_values(by=['install_day'],ascending = True).reset_index()
    dfR['roi3'].plot(alpha = 0.5,label='roi3')
    dfR['roi7'].plot(label='roi7')

    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('/src/data/maRoi.png')
    print('save to /src/data/maRoi.png')
    plt.clf()

# 花费相关
def cost():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    plt.title("cost usd")
    plt.figure(figsize=(10.8, 3.2))
    df['install_day'] = df['install_day'].astype('string')
    df.set_index(["install_day"], inplace=True)

    df['cost'].plot(label='cost')

    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('/src/data/maCost.png')
    print('save to /src/data/maCost.png')
    plt.clf()

def macd():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    
    plt.figure(figsize=(10.8, 6.4))
    df['install_day'] = df['install_day'].astype('string')
    df.set_index(["install_day"], inplace=True)

    # 现针对7日ROI划线macd
    plt.figure(1)
    plt.subplot(2, 1, 1)
    plt.title("7d roi macd")
    exp1 = df['roi'].ewm(span=12, adjust=False).mean()
    exp2 = df['roi'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    exp3 = macd.ewm(span=9, adjust=False).mean()
    macd.plot(label='MACD')
    exp3.plot(label='DIF')
    plt.legend(loc='best')

    # 现针对1日ROI划线macd
    plt.figure(1)
    plt.subplot(2, 1, 2)
    plt.title("1d roi macd")
    exp1 = df['roi1'].ewm(span=12, adjust=False).mean()
    exp2 = df['roi1'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    exp3 = macd.ewm(span=9, adjust=False).mean()
    macd.plot(label='MACD')
    exp3.plot(label='DIF')
    plt.legend(loc='best')

    
    plt.tight_layout()
    plt.savefig('/src/data/maMACD.png')
    print('save to /src/data/maMACD.png')
    plt.clf()

# 应该是画macd与对应属性的对比图，暂时先分开画两张图
# 然后用属性与ROI7日后平移做对比，比如cpup的趋势是否有相关之处，可以作为有效信号

def single(name, N = 30):
    # df = pd.read_csv(getFilename('advData20220501_20221201'))
    df = pd.read_csv(getFilename('vaData20220501_20221201'))
    
    plt.figure(figsize=(10.8, 9.6))
    df['install_day'] = df['install_day'].astype('string')
    df.set_index(["install_day"], inplace=True)

    ax1 = plt.subplot(311)
        
    df[name].plot(label = name)
    df['%s%d_mean'%(name,N)].plot(label = '%s%d_mean'%(name,N),alpha = 0.3)
    df['%s%d_kpi_mean'%(name,N)].plot(label = '%s%d_kpi_mean'%(name,N),alpha = 0.5)
    plt.legend(loc='best')
    plt.grid(linestyle = '--', linewidth = 0.5)
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(10))

    ax2 = plt.subplot(312)
    df['%s_dif'%name].plot(label='DIF')
    df['%s_dea'%name].plot(label='DEA')
    plt.legend(loc='best')
    plt.grid(linestyle = '--', linewidth = 0.5)
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(10))

    ax3 = plt.subplot(313)
    # MACD bar / Oscillator
    df['%s_his'%name].plot(label='OSC')
    df['zero'] = 0
    df['zero'].plot()
    ax3.xaxis.set_major_locator(ticker.MultipleLocator(10))

    # 可能需要看到基本决策趋势代表什么
    plt.legend(loc='best')
    plt.grid(linestyle = '--', linewidth = 0.5)
    plt.tight_layout()
    plt.savefig('/src/data/ma_%sMACD.png'%name)
    print('save to /src/data/ma_%sMACD.png'%name)
    plt.clf()

# 尝试将数值分析画出来
def va(name,N = 30):
    df = pd.read_csv(getFilename('vaData20220501_20221201'))
    df['install_day'] = df['install_day'].astype('string')

    fig = plt.figure(figsize=(10.8, 3.2))
    ax = fig.add_subplot(111)
    ax.plot(df['install_day'],df['%s%d_mean'%(name,N)], label = '%s%d_mean'%(name,N),alpha = 0.3)
    ax.plot(df['install_day'],df['%s%d_kpi_mean'%(name,N)], label = '%s%d_kpi_mean'%(name,N),alpha = 0.5)
    ax.plot(df['install_day'],df[name], label = name,alpha = 1.0)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(10))

    # ax2 = ax.twinx()
    # ax2.plot(df['install_day'],df['%s%d_pos'%(name,N)], label = '%s%d_pos'%(name,N),alpha = 0.7)
    # ax2.plot(df['install_day'],df['%s%d_kpi_pos'%(name,N)], label = '%s%d_kpi_pos'%(name,N),alpha = 0.4)
    # ax2.xaxis.set_major_locator(ticker.MultipleLocator(10))

    # fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)

    # ax.set_ylabel(name)
    # ax2.set_ylabel(r"pos")

    plt.legend(loc='best')
    plt.xticks(rotation=45)
    plt.grid(linestyle = '--', linewidth = 0.5)
    plt.tight_layout()
    plt.savefig('/src/data/ma_%sVA.png'%name)
    print('save to /src/data/ma_%sVA.png'%name)
    plt.clf()


def test():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    

    # plt.figure(figsize=(10.8, 3.2))
    df['install_day'] = df['install_day'].astype('string')
    # df.set_index(["install_day"], inplace=True)

    fig = plt.figure(figsize=(10.8, 3.2))
    ax = fig.add_subplot(111)
    ax.plot(df['install_day'],df['roi'], label = 'roi',alpha = 0.3)

    dfR = df.sort_values(by=['install_day'],ascending = False).reset_index()
    
    # 再做一个7日的
    dfR['cost7'] = dfR['cost'].rolling(window=7).mean()
    dfR['r7usd7']= dfR['r7usd'].rolling(window=7).mean()
    dfR['roi7'] = dfR['r7usd7']/dfR['cost7']
    # 再将时间升序，然后划线
    dfR = dfR.sort_values(by=['install_day'],ascending = True).reset_index()
    dfR['roi7'].plot(label='roi7')
    ax.plot(dfR['install_day'],dfR['roi7'], label = 'roi7')

    # 暂时设定本年所有kpi都是0.06
    df['kpi'] = 0.06
    df['kpi'].plot(label='kpi')
    df['kpi0.9'] = df['kpi']*.9
    df['kpi0.9'].plot(label='kpi0.9')
    df['kpi1.1'] = df['kpi']*1.1
    df['kpi1.1'].plot(label='kpi1.1')
    

    ax2 = ax.twinx()
    ax2.plot(df['install_day'],df['cost'], label = 'cost')
    fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)
    
    ax.set_ylabel(r"ROI")
    ax2.set_ylabel(r"cost usd")

    plt.tight_layout()
    plt.savefig('/src/data/maTest.png')
    print('save to /src/data/maTest.png')
    plt.clf()


if __name__ == '__main__':
    # roi()
    # cost()
    # macd()
    # test()
    
    single('r1usd')
    single('cpup')
    single('cpm')
    single('roi1')
    single('roi')
    single('users')
    # va('r1usd')
    