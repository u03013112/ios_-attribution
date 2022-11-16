# 大盘预测AI
# 直接用大盘数据来做拟合

# 流程确定后，尝试更改分组，查看效果
import pandas as pd

import sys
sys.path.append('/src')
from src.predSkan.data import getTotalData
from src.tools import getFilename
from src.googleSheet import GSheet
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]
# groupList = [[0],[1,2,3,4,5,6,7,8,9,10],[11,12,13,14,15,16,17,18,19,20],[21,22,23,24,25,26,27,28,29,30],[31,32,33,34,35,36,37,38,39,40],[41,42,43,44,45,46,47,48,49,50],[51,52,53,54,55,56,57,58,59,60],[61,62],[63]]

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


import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# 单独训练一定次数后，保存结果
def createMod(createModFun):
    modList = []
    for i in range(len(groupList)):
        mod = createModFun()
        modList.append(mod)
    return modList

def createModFunc1():
    mod = keras.Sequential(
        [
            # 暂时只做这一层
            layers.Dense(1, input_shape=(1,))
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def createModFunc2():
    mod = keras.Sequential(
        [
            layers.Dense(10, activation="tanh",input_shape=(1,)),
            layers.Dense(1, activation="tanh")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(10, activation="relu",input_shape=(1,)),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def createModFunc4():
    mod = keras.Sequential(
        [
            layers.Dense(100, activation="relu",input_shape=(1,)),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def createModFunc5():
    mod = keras.Sequential(
        [
            layers.Dense(100, activation="relu",input_shape=(1,)),
            layers.Dropout(0.3),
            layers.Dense(100, activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def createModFunc6():
    mod = keras.Sequential(
        [
            layers.Dense(100, activation="tanh",input_shape=(1,)),
            layers.Dropout(0.3),
            layers.Dense(100, activation="tanh"),
            layers.Dropout(0.3),
            layers.Dense(1, activation="tanh")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

createModList = [
    {
        'name':'mod3',
        'createModFunc':createModFunc3
    },{
        'name':'mod4',
        'createModFunc':createModFunc4
    },{
        'name':'mod5',
        'createModFunc':createModFunc5
    },{
        'name':'mod6',
        'createModFunc':createModFunc6
    },{
        'name':'mod1',
        'createModFunc':createModFunc1
    },{
        'name':'mod2',
        'createModFunc':createModFunc2
    }
]
def train(dataDf2,modList,modName):
    earlyStoppingLoss = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=5)
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
    for i in range(len(groupList)):
        trainDf = dataDf2.loc[(dataDf2.group == i) & (dataDf2.install_date < '2022-09-01')].groupby('install_date').agg('sum')
        testDf = dataDf2.loc[(dataDf2.group == i) & (dataDf2.install_date >= '2022-09-01')].groupby('install_date').agg('sum')
        trainX = trainDf['count'].to_numpy()
        trainY = trainDf['sumr7usd'].to_numpy()
        testX = testDf['count'].to_numpy()
        testY = testDf['sumr7usd'].to_numpy()
        mod = modList[i]
        history = mod.fit(trainX, trainY, epochs=500000, validation_data=(testX,testY)
        ,callbacks=[earlyStoppingLoss,earlyStoppingValLoss]
        ,batch_size=16
        # ,verbose=0
        )
        historyDf = pd.DataFrame(data=history.history)
        historyDf.to_csv(getFilename('history%d_%s%s'%(i,modName,filenameSuffix)))
        modFileName = '/src/src/predSkan/mod/mTotal%d_%s%s.h5'%(i,modName,filenameSuffix)
        mod.save(modFileName)
        print('save %s,loss:%f'%(modFileName,history.history['loss'][-1]))
        logFilename = '/src/src/predSkan/log/log%d.log'%(i)
        # 记录日志文件
        with open(logFilename, 'a+') as f:
            if 'val_loss' in history.history:
                f.write('%s %f %f\n'%(modFileName,history.history['loss'][-1],history.history['val_loss'][-1]))
            else:
                f.write('%s %f -\n'%(modFileName,history.history['loss'][-1]))

def loadMod(modName,suffix):
    modList = []
    for i in range(len(groupList)):
        modFileName = '/src/src/predSkan/mod/mTotal%d_%s_%s.h5'%(i,modName,suffix)
        mod = tf.keras.models.load_model(modFileName)
        modList.append(mod)
    return modList

import numpy as np
# 然后对整体做测试
def test(dataDf2,modList):
    sinceTimeStr = '20220901'
    unitlTimeStr = '20220930'
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

    y_true = np.array([])
    y_pred = np.array([])
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')

        y_pred_day = 0
        df = dataDf2.loc[(dataDf2.install_date == dayStr)]
        for i in range(len(groupList)):
            # count 就是预测的input
            count = df.loc[df.group == i,'count'].sum()
            if count == 0:
                # 没有这种，就不预测
                continue
            x = np.array([count])
            mod = modList[i]
            y_pred_day += mod.predict(x).reshape(-1).sum()
        y_pred = np.append(y_pred,y_pred_day)
        y_true_day = dataDf2.loc[dataDf2.install_date == dayStr,'sumr7usd'].sum()
        y_true = np.append(y_true,y_true_day)
    
    print(y_true.shape,y_pred.shape)

    def mapeFunc(y_true, y_pred):
        return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
    
    mape = mapeFunc(y_true,y_pred)
    print(mape)

if __name__ == '__main__':
    # dataStep0('20220501','20220930')
    df = dataStep1('20220501','20220930')
    df2 = dataStep2(df)
    for m in createModList:
        modList = createMod(m['createModFunc'])
        train(df2,modList,m['name'])
        # modList = loadMod(filenameSuffix)
        # test(df2,modList)
