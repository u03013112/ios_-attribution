# 方案五：此方案暂时只判断是否需要增加花费
# 此方案与方案四的主要区别在于加入了惩罚，对于有正向相关系数的属性，如果低于kpi均值，则惩罚，惩罚方式与奖励方式一致。

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import sys
sys.path.append('/src')
from src.tools import getFilename

# 尝试获得最近N日的满足KPI的均值
# 也可以尝试算EMA
# 然后当前值高于（相关性为负的是低于）均值时，就认为是正信号
# 用正信号乘以相关系数
# 最后画图，与实际ROI曲线作对比

# p为超越kpi均线多少倍开始计算，默认1.0即超越就算，1.1就是超越10%才开始计算
def func5( N = 30,p = 1.0):
    df = pd.read_csv(getFilename('oscData20220501_20221201'))
    df['increase_score'] = 0

    corrDf = pd.read_csv(getFilename('corr20220501_20221201'))
    # 所有敏感指标NameList
    nameList = ['r1usd','users','cpm','cpup','roi1']
    # print(corrDf)
    # 因为roi在最后一行，所以直接就取最后一个
    roiIndex = len(corrDf)-1
    for name in nameList:
        c = corrDf[name].iloc[roiIndex]
        osc3Name = '%s_his_osc3'%(name)
        cWeightName = '%s_cWeight3'%(name)
        df[cWeightName] = 0
        df.loc[df[osc3Name] == 'fastUp',cWeightName] = 2.0
        df.loc[df[osc3Name] == 'up',cWeightName] = 1.0
        df.loc[df[osc3Name] == 'slowUp',cWeightName] = 0.5
        df.loc[df[osc3Name] == 'changeToUp',cWeightName] = 0.5
        
        df.loc[df[osc3Name] == 'fastDown',cWeightName] = 2.0
        df.loc[df[osc3Name] == 'down',cWeightName] = 1.0
        df.loc[df[osc3Name] == 'slowDown',cWeightName] = 0.5
        df.loc[df[osc3Name] == 'changeToDown',cWeightName] = 0.5
        
        df['over_kpi_%s'%name] = (df[name] - df['%s%d_kpi_mean'%(name,N)])/df['%s%d_kpi_mean'%(name,N)]
        df['corr%s'%name] = c
        df['increase_score'] += c*df[cWeightName]*df['over_kpi_%s'%name]

    fig = plt.figure(figsize=(10.8, 3.2))
    df['install_day'] = df['install_day'].astype('string')

    ax = plt.subplot(111)
    
    ax.plot(df['install_day'],df['roi'],label = 'roi',c = 'b',alpha = 0.3)
    ax.plot(df['install_day'],df['kpi'],label = 'kpi',c = 'r')
    
    dfR = df.sort_values(by=['install_day'],ascending = False).reset_index()
    
    dfR['cost7'] = dfR['cost'].rolling(window=7).mean()
    dfR['r7usd7']= dfR['r7usd'].rolling(window=7).mean()
    dfR['roi7'] = dfR['r7usd7']/dfR['cost7']
    # 再将时间升序，然后划线
    dfR = dfR.sort_values(by=['install_day'],ascending = True).reset_index()
    dfR['roi7'].plot(label='roi7')
    ax.plot(dfR['install_day'],dfR['roi7'], label = 'roi7')

    ax.xaxis.set_major_locator(ticker.MultipleLocator(30))

    ax2 = ax.twinx()
    ax2.plot(df['install_day'],df['increase_score'],label = 'increase_score')
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(30))
    
    ax.set_ylabel('roi')
    ax2.set_ylabel('score')

    # plt.legend(loc='best')
    fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)
    plt.xticks(rotation=45)
    
    plt.grid(linestyle = '--', linewidth = 0.5)
    plt.tight_layout()
    plt.savefig('/src/data/ma_func5.png')
    print('save to /src/data/ma_func5.png')
    plt.clf()

    return df.loc[:,~df.columns.str.match('Unnamed')]

func5().to_csv(getFilename('func5'))

# 结论上是不分偏差比较大，可能需要将超出幅度也考虑进去。
