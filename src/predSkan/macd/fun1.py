# 尝试制定一些方案，来判断应该加预算还是减预算，然后用data里面定义的标签来确认一下准确率

import pandas as pd

import sys
sys.path.append('/src')
from src.tools import getFilename

# 简单判断，用r1usd来做个样本，主要是测试一下查准率
def func1():
    df = pd.read_csv(getFilename('labelData20220501_20221201'))
    df['install_day'] = df['install_day'].astype('string')
    # 直接用roi1*3.05来做判断
    dfRet = pd.DataFrame({
        'install_day':df['install_day'],
        'roi1':df['roi1'],
        'roi7p':df['roi1']*4,
        'kpi0':df['kpi0'],
        'kpi1':df['kpi1']
    })
    dfRet['label0'] = 0
    dfRet.loc[dfRet.roi7p > dfRet.kpi1,'label0'] = 1
    dfRet.loc[dfRet.roi7p < dfRet.kpi0,'label0'] = 2

    return dfRet
    
from sklearn.metrics import precision_score,recall_score,f1_score
# 尝试对label0进行计算
# 由于是3分类，所以计算P&R的时候是比较麻烦的
def label0PR(funcRet):
    df = pd.read_csv(getFilename('labelData20220501_20221201'))
    
    y_true = list(df['label0'])
    y_pred = list(funcRet['label0'])
    print(precision_score(y_true, y_pred, average='macro'))  
    print(precision_score(y_true, y_pred, average='micro'))  
    print(precision_score(y_true, y_pred, average='weighted'))  
    print(precision_score(y_true, y_pred, average=None))  

    print(recall_score(y_true, y_pred, average='macro'))  
    print(recall_score(y_true, y_pred, average='micro'))  
    print(recall_score(y_true, y_pred, average='weighted'))  
    print(recall_score(y_true, y_pred, average=None))  

    print(f1_score(y_true, y_pred, average='macro'))  
    print(f1_score(y_true, y_pred, average='micro'))  
    print(f1_score(y_true, y_pred, average='weighted'))  
    print(f1_score(y_true, y_pred, average=None))  

# 尝试将结论画到图上，感性的看看，但是为了比较清楚，可能画点并不清楚
import matplotlib.pyplot as plt
def label0Plot(funcRet):
    df = pd.read_csv(getFilename('labelData20220501_20221201'))
    df['install_day'] = df['install_day'].astype('string')
    retDf = pd.DataFrame({
        'install_day':funcRet['install_day'],
        'kpi0':funcRet['kpi0'],
        'kpi1':funcRet['kpi1'],
        'roiN':df['roiN'],
        'label0':funcRet['label0']
    })
    retDf.loc[retDf.label0 == 2,'label0'] = -1
    # print(retDf)
    fig = plt.figure(figsize=(10.8, 3.2))
    ax = fig.add_subplot(111)
    ax.plot(df['install_day'],df['roiN'], label = 'roiN')

    retDf['kpi0'].plot(label='kpi0')
    retDf['kpi1'].plot(label='kpi1')

    funcRet['roi7p'].plot(label='roi7p')
    df['roi'].plot(label='roi')

    ax2 = ax.twinx()
    ax2.plot(retDf['install_day'],retDf['label0'],'o', label = 'label0')
    fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)
    
    ax.set_ylabel(r"ROI")
    ax2.set_ylabel(r"label0")

    plt.tight_layout()
    plt.savefig('/src/data/maLabel0.png')
    print('save to /src/data/maLabel0.png')
    plt.clf()


if __name__ == '__main__':
    ret = func1()
    label0PR(ret)
    label0Plot(ret)

