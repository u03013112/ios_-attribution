# 大盘预测AI
# 直接用大盘数据来做拟合

# 然后对整体做测试
# 流程确定后，尝试更改分组，查看效果
import pandas as pd

import sys
sys.path.append('/src')
from src.predSkan.data import getTotalData
from src.tools import getFilename
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]
# groupList = [[0],[1,2,3,4,5,6,7,8,9,10],[11,12,13,14,15,16,17,18,19,20],[21,22,23,24,25,26,27,28,29,30],[31,32,33,34,35,36,37,38,39,40],[41,42,43,44,45,46,47,48,49,50],[51,52,53,54,55,56,57,58,59,60],[61,62],[63]]

# 从maxCompute取数据
def dataStep0(sinceTimeStr,unitlTimeStr):
    df = getTotalData(sinceTimeStr,unitlTimeStr)
    df.to_csv(getFilename('totalData%s_%s'%(sinceTimeStr,unitlTimeStr)))
# 从本地文件取数据，跑过步骤0的可以直接从这里开始，更快速
def dataStep1(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df

# 对每组数据分别整理
def dataStep2(dataDf1):
    dataDf1.insert(dataDf1.shape[1],'group',0)
    for i in range(len(groupList)):
        l = groupList[i]
        for cv in l:
            dataDf1.loc[dataDf1.cv == cv,'group'] = i
    return dataDf1


import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# 单独训练一定次数后，保存结果
def createMod():
    modList = []
    for i in range(len(groupList)):
        mod = keras.Sequential(
            [
                # 暂时只做这一层
                layers.Dense(1, input_shape=(1,))
            ]
        )
        mod.compile(optimizer='adam',loss='mse')
        modList.append(mod)
    return modList

def train(dataDf2,modList):
    for i in range(len(groupList)):
        x = dataDf2.loc[dataDf2.group == i].groupby('install_date').agg('sum')
        print(x)


if __name__ == '__main__':
    # dataStep0('20220501','20220930')
    df = dataStep1('20220501','20220930')
    df2 =dataStep2(df)
    # print(df2)
    train(df2,None)