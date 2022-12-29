# 首日回收 + 在线时间 版本
# 直接简单的将两组属性叠加，做一个更宽的模型，先试试效果，如果并不能获得更好的效果，那么就不考虑再将属性缩小
import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.predSkan.data import getTotalData,getTotalData2
from src.tools import afCvMapDataFrame
from src.maxCompute import execSql
from src.tools import getFilename
from src.predSkan.tools.ai import purgeRetCsv,logUpdate,createDoc

import datetime
import math

# 从数数获得数据
def dataStep0():

    df = pd.read_csv(getFilename('allDataFromSS'))
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')

    df.insert(df.shape[1],'otime_cv',0)
    df.insert(df.shape[1],'r1_cv',0)
    df.insert(df.shape[1],'r1lp_cv',0)
    df.insert(df.shape[1],'r1max_cv',0)

    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        if pd.isna(min_event_revenue):
            continue
        df.loc[(df.r1usd > min_event_revenue),'r1_cv'] = i

    for i in range(64):
        min_r1usdMax = i * 1.5    
        df.loc[(df.r1usd_max > min_r1usdMax),'r1max_cv'] = i
    
    for i in range(64):
        min_otime = i * 100
        df.loc[(df.otime > min_otime),'otime_cv'] = i

    for i in range(64):
        min_lp = i * 1350
        df.loc[(df.last_pay > min_lp),'r1lp_cv'] = i

    df.to_csv(getFilename('totalData'))

# r1_cv -> cv
def dataStep1():
    df = pd.read_csv(getFilename('totalData'))
    df.insert(df.shape[1],'count',1)
    df = df.groupby(['install_date','r1_cv'],as_index=False).agg({'r1usd':'sum','r7usd':'sum','count':'sum'})
    
    df = df.rename(columns={'r1usd':'sumr1usd'})
    df = df.rename(columns={'r7usd':'sumr7usd'})
    df = df.rename(columns={'r1_cv':'cv'})

    df = df.sort_values(by=['install_date','cv']).reset_index(drop=True)
    df = df.loc[(df.install_date >= '2022-05-01') & (df.install_date < '2022-12-01')]
    # df.to_csv(getFilename('totalData1'))
    return df

# otime_cv -> cv
def dataStep2():
    df = pd.read_csv(getFilename('totalData'))
    df.insert(df.shape[1],'count',1)
    df = df.groupby(['install_date','otime_cv'],as_index=False).agg({'r1usd':'sum','r7usd':'sum','count':'sum'})
    
    df = df.rename(columns={'r1usd':'sumr1usd'})
    df = df.rename(columns={'r7usd':'sumr7usd'})
    df = df.rename(columns={'otime_cv':'cv'})

    df = df.sort_values(by=['install_date','cv']).reset_index(drop=True)
    df = df.loc[(df.install_date >= '2022-05-01') & (df.install_date < '2022-12-01')]
    # df.to_csv(getFilename('totalData1'))
    return df

# 对数据做基础处理
def dataStep3(dataDf2):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf2['install_date'].unique()
    for install_date in install_date_list:
        df = dataDf2.loc[(dataDf2.install_date == install_date)]
        for i in range(64):
            if df.loc[df.cv == i,'sumr7usd'].sum() == 0 and df.loc[df.cv == i,'count'].sum() == 0:
                dataDf2 = dataDf2.append(pd.DataFrame(data={
                    'install_date':[install_date],
                    'count':[0],
                    'sumr7usd':[0],
                    'cv':[i]
                }),ignore_index=True)
                print('补充：',install_date,i)
    return dataDf2

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

def createModFunc1():
    mod = keras.Sequential(
        [
            layers.Dense(128,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(64,)),
            # layers.Dropout(0.3),
            layers.Dense(128, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            # layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    mod.compile(optimizer='RMSprop',loss='mse')
    mod.summary()
    return mod

def createModFunc2():
    mod = keras.Sequential(
        [
            layers.Dense(128,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(128,)),
            layers.Dropout(0.3),
            layers.Dense(128, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mse')
    mod.summary()
    return mod

epochMax = 15000
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

def getTrainX(trainDf):
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainXSum = trainX.sum(axis=1).reshape(-1,1)
    trainX = trainX/trainXSum
    return trainX

def getXMinAndMaxFromTrainX(trainX):
    min = trainX.T.min(axis=1)
    max = trainX.T.max(axis=1)    
    return min,max

def getTrainingDataY(trainDf):
    trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    trainY0 = trainSumByDay['sumr7usd'].to_numpy()
    trainY1 = trainSumByDay['sumr1usd'].to_numpy()
    # 这里为了解决部分0数据
    trainY0[trainY0 <= 0] = 1
    trainY1[trainY1 <= 0] = 1
    trainY = trainY0/trainY1 - 1
    return trainY, trainY0, trainY1

def getTrainingData(df,trainRate=0.6):
    dataDf = df.sort_values(by=['install_date','cv'])
    dataDf = dataDf.groupby(['install_date','cv']).agg('sum')
    dataX = getTrainX(dataDf)
    min,max = getXMinAndMaxFromTrainX(dataX)
    x = (dataX-min)/(max-min)
    x[x == np.inf] = 0
    x[x == -np.inf] = 0
    trainXSs = np.nan_to_num(x)
    
    dataY, dataY0, dataY1 = getTrainingDataY(dataDf)

    line = math.floor(len(trainXSs)*trainRate)
    
    trainX = dataX[:line]
    testX = dataX[line:]

    trainY = dataY[:line]
    testY = dataY[line:]
    trainY0 = dataY0[:line]
    testY0 = dataY0[line:]
    trainY1 = dataY1[:line]
    testY1 = dataY1[line:]

    return trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1, min, max

def train(dataDf1,dataDf2,message):
    global lossAndErrorPrintingCallbackSuffixStr
    
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta = .1,patience=300)
    for _ in range(10):
        name = 'total'
        
        # 各种命名都用这个后缀，防止重名
        filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
        # 每次都重新建立mod
        # mod = createModFunc1()
        mod = createModFunc2()

        modPath = '/src/src/predSkan/total/mod/%s/'%filenameSuffix
        checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.2f}-{val_loss:.2f}.h5')
    
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True
        )

        lossAndErrorPrintingCallbackSuffixStr = name

        trainX1, trainY, testX1, testY, trainY0, testY0, trainY1, testY1, min1, max1 = getTrainingData(dataDf1,0.6)
        trainX2, _, testX2, _, _, _, _, _, min2, max2 = getTrainingData(dataDf2,0.6)

        # 直接将两组结论并列
        trainX = np.append(trainX1,trainX2,axis=1)
        testX = np.append(testX1,testX2,axis=1)
        min = np.append(min1,min2)
        max = np.append(max1,max2)

        history = mod.fit(trainX, trainY, epochs=epochMax, validation_data=(testX,testY)
            ,callbacks=[
                # earlyStoppingValLoss,
                model_checkpoint_callback,
                LossAndErrorPrintingCallback()
            ]
            ,batch_size=128
            ,verbose=0
            )
        # 训练完成可以把mod清理掉了
        tf.keras.backend.clear_session()
        del mod

        # 每个国家一个log目录
        logDir = '/src/data/doc/total/'
        os.makedirs(logDir,exist_ok=True)
        # 将每次的明细结果放进去，
        docDirname = '%s/%s'%(logDir,name+filenameSuffix)
        val_loss = createDoc(modPath,trainX, trainY0,trainY1,testX,testY0, testY1,history,docDirname,message)
        minFilename = os.path.join(docDirname,'min.npy')
        np.save(minFilename, min)
        maxFilename = os.path.join(docDirname,'max.npy')
        np.save(maxFilename, max)
        # 将结果写入到国家日志里
        # retCsvFilename 记录所有结果
        retCsvFilename = os.path.join(logDir,'ret.csv')
        if os.path.exists(retCsvFilename):
            retDf = pd.read_csv(retCsvFilename)
        else:
            retDf = pd.DataFrame(data = {
                'path':[],
                'val_loss':[],
                'message':[]
            })
        logData = {
            'path':[docDirname],
            'val_loss':[val_loss],
            'message':[message]
        }
        retDf = retDf.append(pd.DataFrame(data=logData))
        # 将本次的结果添加，然后重新写文件，这个方式有点丑，暂时这样。
        
        retDf.to_csv(retCsvFilename)
        purgeRetCsv(retCsvFilename)
        logFilename = os.path.join(logDir,'log.txt')
        logUpdate(retCsvFilename,logFilename,name)


if __name__ == '__main__':
    # if __debug__:
    #     print('debug 模式，并未真的sql')
    # else:
        # dataStep0()
        # df = dataStep1()
        # print(df)
        # df4 = dataStep3(df)
        # df4.to_csv(getFilename('totalData2_ot'))

    df1 = pd.read_csv(getFilename('totalData2'))
    df2 = pd.read_csv(getFilename('totalData2_pmax'))
    train(df1,df2,'cv test 1+4')
    