# 尝试进行整体预测
# 按地区进行汇总
# 改为EMA数据来做预测
# 测试集仍用原始数据
import pandas as pd

import sys
sys.path.append('/src')
from src.predSkan.data import getTotalDataGroupByGeo,getTotalDataGroupByGeoMax
from src.tools import getFilename
from src.googleSheet import GSheet
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]
geoList = [
    {'name':'US','codeList':['US']},
    {'name':'T1','codeList':['CA','AU','GB','UK','NZ','DE','FR']},
    {'name':'KR','codeList':['KR']},
    {'name':'GCC','codeList':['AE','BH','KW','OM','QA','ZA','SA']}
]
import datetime
# 各种命名都用这个后缀，防止重名
filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H')

# 从maxCompute取数据
def dataStep0(sinceTimeStr,unitlTimeStr):
    df = getTotalDataGroupByGeo(sinceTimeStr,unitlTimeStr)
    df.to_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))

# 从本地文件取数据，跑过步骤0的可以直接从这里开始，更快速
def dataStep1(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df

def dataStep0Max(sinceTimeStr,unitlTimeStr):
    df = getTotalDataGroupByGeoMax(sinceTimeStr,unitlTimeStr,max=500.0)
    df.to_csv(getFilename('totalGeoMaxData%s_%s'%(sinceTimeStr,unitlTimeStr)))

def dataStep1Max(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalGeoMaxData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df


# 对每组数据分别整理
def dataStep2(dataDf1):
    dataDf1.insert(dataDf1.shape[1],'group',0)
    for i in range(len(groupList)):
        l = groupList[i]
        for cv in l:
            dataDf1.loc[dataDf1.cv == cv,'group'] = i
    return dataDf1

# 添加geo属性
def dataStep3(dataDf2):
    dataDf2.insert(dataDf2.shape[1],'geo','unknown')
    for geo in geoList:
        name = geo['name']
        for code in geo['codeList']:
            dataDf2.loc[dataDf2.country_code == code,'geo'] = name
    return dataDf2

# 对数据做基础处理
def dataStep4(dataDf3):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf3['install_date'].unique()
    for install_date in install_date_list:
        print(install_date)
        df = dataDf3.loc[(dataDf3.install_date == install_date)]
        dataNeedAppend = {
            'install_date':[],
            'count':[],
            'sumr7usd':[],
            'group':[],
            'geo':[]
        }
        for i in range(len(groupList)):
            for geo in geoList:
                name = geo['name']
                # 这里要为每一个geo做补充
                if df.loc[(df.group == i) & (df.geo == name),'sumr7usd'].sum() == 0 \
                    and df.loc[(df.group == i) & (df.geo == name),'count'].sum() == 0:

                    dataNeedAppend['install_date'].append(install_date)
                    dataNeedAppend['count'].append(0)
                    dataNeedAppend['sumr7usd'].append(0)
                    dataNeedAppend['group'].append(i)
                    dataNeedAppend['geo'].append(name)

        dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    return dataDf3

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

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

epochMax = 1000
lossAndErrorPrintingCallbackSuffixStr = ''
class LossAndErrorPrintingCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        global lossAndErrorPrintingCallbackSuffixStr
        if epoch > 0 and epoch%100 == 0:
            keys = list(logs.keys())
            str = 'epoch %d/%d:'%(epoch,epochMax)
            for key in keys:
                str += '[%s]:%.2f '%(key,logs[key])
            print(lossAndErrorPrintingCallbackSuffixStr,str)

def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df

from sklearn.preprocessing import StandardScaler
def train(dataDf3):
    global lossAndErrorPrintingCallbackSuffixStr
    for geo in geoList:
        name = geo['name']

        for day in [3,7,14]:
            checkpoint_filepath = '/src/src/predSkan/geo/mod/ema/mod%s%d_%s_{epoch:05d}-{val_loss:.2f}.h5'%(name,day,filenameSuffix)
    
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = geo['name'] + 'ema%d'%day

            trainDf = dataDf3.loc[
                (dataDf3.install_date >= '2022-08-01') & (dataDf3.install_date < '2022-09-01') & (dataDf3.geo == name)
            ].sort_values(by=['install_date','group'])
            trainDf = trainDf.groupby(['install_date','group']).agg('sum')
            trainX = trainDf['count'].to_numpy().reshape((-1,64))
            trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
            trainSumByDay = dataAddEMA(trainSumByDay,day)
            trainY = trainSumByDay['ema'].to_numpy()

            testDf = dataDf3.loc[
                (dataDf3.install_date >= '2022-09-01') & (dataDf3.install_date <= '2022-09-07') & (dataDf3.geo == name)
            ].sort_values(by=['install_date','group'])
            testDf = testDf.groupby(['install_date','group']).agg('sum')
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
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        # dataStep0('20220501','20220930')
        df = dataStep1('20220501','20220930')
        df2 = dataStep2(df)
        df3 = dataStep3(df2)
        df4 = dataStep4(df3)
        df4.to_csv(getFilename('totalGeoDataSum_20220501_20220930'))
    df4 = pd.read_csv(getFilename('totalGeoDataSum_20220501_20220930'))

    train(df4)
