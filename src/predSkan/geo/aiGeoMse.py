# 同第3版，主要尝试对参数归一化
# 对大R做切平处理
import pandas as pd
import os
import sys
sys.path.append('/src')
from src.predSkan.data import getTotalDataGroupByGeo
from src.tools import getFilename
from src.predSkan.tools.ai import purgeRetCsv,logUpdate

geoList = [
    {'name':'US','codeList':['US']},
    {'name':'T1','codeList':['CA','AU','GB','UK','NZ','DE','FR']},
    {'name':'KR','codeList':['KR']},
    {'name':'GCC','codeList':['AE','BH','KW','OM','QA','ZA','SA']}
]
import datetime

# 从maxCompute取数据
def dataStep0(sinceTimeStr,unitlTimeStr):
    df = getTotalDataGroupByGeo(sinceTimeStr,unitlTimeStr)
    df.to_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))

# 从本地文件取数据，跑过步骤0的可以直接从这里开始，更快速
def dataStep1(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df

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
            'cv':[],
            'geo':[]
        }
        for i in range(64):
            for geo in geoList:
                name = geo['name']
                # 这里要为每一个geo做补充
                if df.loc[(df.cv == i) & (df.geo == name),'sumr7usd'].sum() == 0 \
                    and df.loc[(df.cv == i) & (df.geo == name),'count'].sum() == 0:
                    dataNeedAppend['install_date'].append(install_date)
                    dataNeedAppend['count'].append(0)
                    dataNeedAppend['sumr7usd'].append(0)
                    dataNeedAppend['cv'].append(i)
                    dataNeedAppend['geo'].append(name)

        dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    return dataDf3

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

def createModFunc1():
    mod = keras.Sequential(
        [
            layers.Dense(128,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            layers.Dense(128, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    mod.compile(optimizer='RMSprop',loss='mse')
    mod.summary()
    return mod


def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    # mod.compile(optimizer='adadelta',loss='mape')
    mod.compile(optimizer='RMSprop',loss='mape')
    mod.summary()
    return mod

epochMax = 10000
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

def getTrainingData(df,geoName,sinceTimeStr,unitlTimeStr):
    trainDf = df.loc[
        (df.install_date >= sinceTimeStr) & (df.install_date < unitlTimeStr) & (df.geo == geoName)
    ].sort_values(by=['install_date','cv'])
    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    trainY0 = trainSumByDay['sumr7usd'].to_numpy()
    trainY1 = trainSumByDay['sumr1usd'].to_numpy()
    trainY = trainY0/trainY1 - 1

    # 尝试标准化
    mean = np.mean(trainX,axis=0)
    std = np.std(trainX,axis=0)
    std[std == 0 ] = 1
    # print(mean)
    # print(std)
    trainXSs = (trainX - mean)/std

    return trainXSs,mean,std, trainY, trainY0, trainY1

def getTestingData(df,geoName,sinceTimeStr,unitlTimeStr,mean,std):
    trainDf = df.loc[
        (df.install_date >= sinceTimeStr) & (df.install_date < unitlTimeStr) & (df.geo == geoName)
    ].sort_values(by=['install_date','cv'])
    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    trainY0 = trainSumByDay['sumr7usd'].to_numpy()
    trainY1 = trainSumByDay['sumr1usd'].to_numpy()
    trainY = trainY0/trainY1 - 1

    # 尝试标准化
    trainXSs = (trainX - mean)/std

    return trainXSs,trainY, trainY0, trainY1


def train(dataDf3,message):
    global lossAndErrorPrintingCallbackSuffixStr
    
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta = .1,patience=300)
    for _ in range(10):
        for geo in geoList:
            name = geo['name']

            # 各种命名都用这个后缀，防止重名
            filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
            # 每次都重新建立mod
            mod = createModFunc1()

            # 只是临时存储，这个路径下的所有文件都可以在训练后清除  
            checkpoint_filepath = '/src/src/predSkan/geo/mod/std/mod%s_%s_{epoch:05d}-{loss:.2f}-{val_loss:.2f}.h5'%(name,filenameSuffix)
        
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = geo['name']

            trainX,mean,std,trainY,trainY0,trainY1 = getTrainingData(dataDf3,name,'2022-05-01','2022-07-30')
            testX, testY, testY0, testY1 = getTestingData(dataDf3,name,'2022-09-01','2022-10-30',mean,std)

            meanDf = pd.DataFrame(data = {
                'mean':list(mean)
            })
            meanDf.to_csv('/src/data/%sMean20220501_20220731.csv'%name)

            stdDf = pd.DataFrame(data = {
                'std':list(std)
            })
            stdDf.to_csv('/src/data/%sStd20220501_20220731.csv'%name)

            history = mod.fit(trainX, trainY, epochs=epochMax, validation_data=(testX,testY)
                ,callbacks=[
                    earlyStoppingValLoss,
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
            logDir = '/src/data/doc/geo/%s'%(name)
            os.makedirs(logDir,exist_ok=True)
            # 将每次的明细结果放进去，
            docDirname = '/src/data/doc/geo/%s/%s'%(name,name+filenameSuffix)
            val_loss = createDoc('/src/src/predSkan/geo/mod/std','mod%s'%name,trainX, trainY0,trainY1,testX,testY0, testY1,history,docDirname,message)
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
            


import numpy as np
def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

from shutil import copyfile
import matplotlib.pyplot as plt
# 生成文档，暂时先把所有需要的东西凑齐，生成文档之后再说
def createDoc(modPath,modSuffix,trainX,trainY0, trainY1,testX,testY0, testY1,history,docDirname,message):
    os.makedirs(docDirname,exist_ok=True)
    # 需要将mod先copy出来
    modList = []
    g = os.walk(modPath)
    for modPath,dirList,fileList in g:  
        for fileName in fileList:  
            if fileName.startswith(modSuffix):
                modFileName=os.path.join(modPath, fileName)
                loss = fileName[:-3].split('-')[-1]
                
                modList.append({
                    'path':modFileName,
                    'loss':float(loss)
                })
    modList = sorted(modList,key=lambda x:x['loss'])
    bestMod = modList[0]
    modFilename = os.path.join(docDirname, 'bestMod.h5')
    copyfile(bestMod['path'], modFilename)

    mod = tf.keras.models.load_model(modFilename)

    retStr = '%s\n'%message
    retStr += '%s\n'%bestMod['path']
    # mod针对训练集表现
    trainYP = mod.predict(trainX)

    yt = trainY0.reshape(-1)
    yp = (trainYP.reshape(-1) + 1)*(trainY1.reshape(-1))
    pd.DataFrame(data={
        'true':list(yt),
        'pred':list(yp),
        'mape':list(np.abs((yp - yt) / yt)*100)
    }).to_csv(os.path.join(docDirname, 'train.csv'))

    trainMape = mapeFunc(yt,yp)
    retStr += 'train mape:%.2f%%\n'%(trainMape)

    plt.title("train")
    plt.plot(yt,'b-',label='true')
    plt.plot(yp,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'train.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()
    
    # mod针对测试集表现
    testYP = mod.predict(testX)
    
    yt = testY0.reshape(-1)
    yp = (testYP.reshape(-1) + 1)*(testY1.reshape(-1))
    pd.DataFrame(data={
        'true':list(yt),
        'pred':list(yp),
        'mape':list(np.abs((yp - yt) / yt)*100)
    }).to_csv(os.path.join(docDirname, 'test.csv'))

    testMape = mapeFunc(yt,yp)
    retStr += 'test mape:%.2f%%\n'%(testMape)

    plt.title("test")
    plt.plot(yt,'b-',label='true')
    plt.plot(yp,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'test.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    # 训练过程，loss变化
    loss = history.history['loss']
    val_loss = history.history['val_loss']

    pd.DataFrame(data={
        'loss':loss,
        'val_loss':val_loss
    }).to_csv(os.path.join(docDirname, 'history.csv'))

    plt.title("history")
    plt.plot(loss,'b-',label='loss')
    plt.plot(val_loss,'r-',label='val_loss')
    plt.legend()
    filename = os.path.join(docDirname, 'history.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    logFilename = os.path.join(docDirname, 'log.txt')
    with open(logFilename, 'w') as f:
        f.write(retStr)

    return testMape

if __name__ == '__main__':
    
    # dataStep0('20220501','20220930')
    # df = dataStep1('20220501','20220930')
    # df3 = dataStep3(df)
    # df4 = dataStep4(df3)
    # df4.to_csv(getFilename('geoData_20220501_20220930'))
    df4 = pd.read_csv(getFilename('geoData_20220501_20220930'))

    train(df4,'mse geo')