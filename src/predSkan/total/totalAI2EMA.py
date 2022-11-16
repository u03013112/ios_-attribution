# 尝试进行整体预测
# 改为EMA数据来做预测
# 测试集仍用原始数据
import pandas as pd

import sys
sys.path.append('/src')
from src.predSkan.data import getTotalData
from src.tools import getFilename
from src.googleSheet import GSheet
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]

import datetime
# 各种命名都用这个后缀，防止重名
filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H')

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

# 对数据做基础处理
def dataStep3(dataDf2):
    # print(dataDf2.sort_values(by=['install_date','group']))
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf2['install_date'].unique()
    for install_date in install_date_list:
        df = dataDf2.loc[(dataDf2.install_date == install_date)]
        for i in range(len(groupList)):
            if df.loc[df.group == i,'sumr7usd'].sum() == 0 and df.loc[df.group == i,'count'].sum() == 0:
                dataDf2 = dataDf2.append(pd.DataFrame(data={
                    'install_date':[install_date],
                    'count':[0],
                    'sumr7usd':[0],
                    'group':[i]
                }),ignore_index=True)
                # print('补充：',install_date,i)
    # print(dataDf2.sort_values(by=['install_date','group']))
    return dataDf2

# 尝试对数据进行EMA计算，这个要在
def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# 单独训练一定次数后，保存结果
def createMod(createModFun):    
    mod = createModFun()
    return mod

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512, activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            layers.Dense(512, activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

createModList = [
    {
        'name':'mod3',
        'createModFunc':createModFunc3
    }
]

epochMax = 1000

class LossAndErrorPrintingCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        if epoch > 0 and epoch%100 == 0:
            keys = list(logs.keys())
            str = 'epoch %d/%d:'%(epoch,epochMax)
            for key in keys:
                str += '[%s]:%.2f '%(key,logs[key])
            print(str)


def train(dataDf3):
    for day in [3,7,14]:
        checkpoint_filepath = '/src/src/predSkan/total/mod/ema/mod_ema%d_%s_{epoch:05d}-{val_loss:.2f}.h5'%(day,filenameSuffix)
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True)

        trainDf = dataDf3.loc[
            (dataDf3.install_date > '2022-08-01') & (dataDf3.install_date < '2022-09-01')
            ].sort_values(by=['install_date','group'])
        trainX = trainDf['count'].to_numpy().reshape((-1,64))
        trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        trainSumByDay = dataAddEMA(trainSumByDay,day)
        trainY = trainSumByDay['ema'].to_numpy()

        testDf = dataDf3.loc[
                (dataDf3.install_date >= '2022-09-01') & (dataDf3.install_date < '2022-09-07')
            ].sort_values(by=['install_date','group'])
        testX = testDf['count'].to_numpy().reshape((-1,64))
        testSumByDay = testDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        testY = testSumByDay['sumr7usd'].to_numpy()

        mod = createModFunc3()
        mod.fit(trainX, trainY, epochs=epochMax, validation_data=(testX,testY)
            # ,callbacks=[earlyStoppingLoss,earlyStoppingValLoss]
            # ,callbacks=[earlyStoppingValLoss]
            ,callbacks=[model_checkpoint_callback,LossAndErrorPrintingCallback()]
            ,batch_size=128
            ,verbose=0
            )
    

if __name__ == '__main__':
    # dataStep0('20220501','20220930')
    df = dataStep1('20220501','20220930')
    df2 = dataStep2(df)
    df3 = dataStep3(df2)
    
    train(df3)

