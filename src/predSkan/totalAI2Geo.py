# 尝试进行整体预测
import pandas as pd

import sys
sys.path.append('/src')
from src.predSkan.data import getTotalDataGroupByGeo
from src.tools import getFilename
from src.googleSheet import GSheet
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]
geoList = [
    {'name':'US','codeList':['US']},
    {'name':'CA','codeList':['CA']},
    {'name':'AU','codeList':['AU']},
    {'name':'GB','codeList':['GB']},
    {'name':'NZ','codeList':['NZ']},
    {'name':'DE','codeList':['DE']},
    {'name':'FR','codeList':['FR']},
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
        for i in range(len(groupList)):
            for geo in geoList:
                name = geo['name']
                # 这里要为每一个geo做补充
                if df.loc[(df.group == i) & (df.geo == name),'sumr7usd'].sum() == 0 \
                    and df.loc[(df.group == i) & (df.geo == name),'count'].sum() == 0:

                    dataDf3 = dataDf3.append(pd.DataFrame(data={
                        'install_date':[install_date],
                        'count':[0],
                        'sumr7usd':[0],
                        'group':[i],
                        'geo':name
                    }),ignore_index=True)
                # print('补充：',install_date,i)
    return dataDf3

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# 单独训练一定次数后，保存结果
def createMod(createModFun):    
    mod = createModFun()
    return mod

def rmse(y_true, y_pred):
    from keras import backend
    return backend.sqrt(backend.mean(backend.square(y_pred - y_true), axis=-1))

def r_square(y_true, y_pred):
    from keras import backend as K
    SS_res =  K.sum(K.square(y_true - y_pred)) 
    SS_tot = K.sum(K.square(y_true - K.mean(y_true))) 
    return ( 1 - SS_res/(SS_tot + K.epsilon()) )

# r2作为loss，不再用1减，这个数值越小越好
def loss_r_square(y_true, y_pred):
    from keras import backend as K
    SS_res =  K.sum(K.square(y_true - y_pred)) 
    SS_tot = K.sum(K.square(y_true - K.mean(y_true))) 
    return ( SS_res/(SS_tot + K.epsilon()) )

def createModFunc1():
    mod = keras.Sequential(
        [
            layers.Dense(100, activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            layers.Dense(100, activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    mod.summary()
    return mod

def createModFunc2():
    mod = keras.Sequential(
        [
            layers.Dense(256, activation="relu", input_shape=(64,)),
            # layers.Dropout(0.3),
            layers.Dense(256, activation="relu"),
            # layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    # mod.compile(optimizer="Nadam", loss=rmse, metrics=[r_square, rmse])
    mod.compile(optimizer='adadelta',loss=loss_r_square,metrics=['mape',r_square])
    return mod

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(256, activation="relu", input_shape=(64,)),
            # layers.Dropout(0.3),
            layers.Dense(256, activation="relu"),
            # layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    # mod.compile(optimizer="Nadam", loss=rmse, metrics=[r_square, rmse])
    mod.compile(optimizer='adadelta',loss=loss_r_square,metrics=['mape',r_square])
    return mod

createModList = [
    {
        'name':'mod1',
        'createModFunc':createModFunc1
    },
    # {
    #     'name':'mod2',
    #     'createModFunc':createModFunc2
    # }
]

epochMax = 30000

class LossAndErrorPrintingCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        if epoch > 0 and epoch%100 == 0:
            keys = list(logs.keys())
            str = 'epoch %d/%d:'%(epoch,epochMax)
            for key in keys:
                str += '[%s]:%.2f '%(key,logs[key])
            print(str)


def train(dataDf3,mod,modName):
    earlyStoppingLoss = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=5)
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)

    for geo in geoList:
        name = geo['name']
        if name != 'GCC':
            continue
        checkpoint_filepath = '/src/src/predSkan/mod/geo/mod%s_%s_%s{epoch:05d}-{val_loss:.2f}.h5'%(name,modName,filenameSuffix)
    
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True
        )

        trainDf = dataDf3.loc[
            (dataDf3.install_date < '2022-09-01') & (dataDf3.geo == name)
        ].sort_values(by=['install_date','group'])
        trainDf = trainDf.groupby(['install_date','group']).agg('sum')
        trainX = trainDf['count'].to_numpy().reshape((-1,64))
        trainSumByDay = trainDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
        trainY = trainSumByDay.to_numpy()

        testDf = dataDf3.loc[
            (dataDf3.install_date >= '2022-09-01')  & (dataDf3.geo == name)
        ].sort_values(by=['install_date','group'])
        testDf = testDf.groupby(['install_date','group']).agg('sum')
        testX = testDf['count'].to_numpy().reshape((-1,64))
        testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
        testY = testSumByDay.to_numpy()

        history = mod.fit(trainX, trainY, epochs=epochMax, validation_data=(testX,testY)
            # ,callbacks=[earlyStoppingLoss,earlyStoppingValLoss]
            # ,callbacks=[earlyStoppingValLoss]
            ,callbacks=[model_checkpoint_callback,LossAndErrorPrintingCallback()]
            ,batch_size=128
            ,verbose=0
            )

        historyDf = pd.DataFrame(data=history.history)
        historyDf.to_csv(getFilename('historyT2_%s%s'%(modName,filenameSuffix)))
        # modFileName = '/src/src/predSkan/mod/mTotalT2_%s%s.h5'%(modName,filenameSuffix)
        # mod.save(modFileName)
        # print('save %s,val_loss:%f'%(modFileName,history.history['val_loss'][-1]))
        # logFilename = '/src/src/predSkan/log/logT2.log'
        # 记录日志文件
        # with open(logFilename, 'a+') as f:
        #     if 'val_loss' in history.history:
        #         f.write('%s %f %f\n'%(modFileName,history.history['loss'][-1],history.history['val_loss'][-1]))
        #     else:
        #         f.write('%s %f -\n'%(modFileName,history.history['loss'][-1]))
        

if __name__ == '__main__':
    # dataStep0('20220501','20220930')
    # df = dataStep1('20220501','20220930')
    # df2 = dataStep2(df)
    # df3 = dataStep3(df2)
    # df4 = dataStep4(df3)
    # df4.to_csv(getFilename('totalGeoData4_20220501_20220930'))
    df4 = pd.read_csv(getFilename('totalGeoData4_20220501_20220930'))

    for m in createModList:
        mod = createMod(m['createModFunc'])
        train(df4,mod,m['name'])

