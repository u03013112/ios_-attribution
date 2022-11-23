# 预测7日付费总和/第1日付费总和
# 替换之前预测第1日至第7日付费总和
# 如果效果还说得过去，可以尝试预测第3日开始，乃至第7日
# 另外尝试对参数归一化

import pandas as pd
import os
import sys
sys.path.append('/src')
from src.predSkan.data import getTotalDataGroupByGeo,getTotalDataGroupByGeoMax
from src.tools import getFilename

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

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(64,)),
            layers.Dropout(0.2),
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            layers.Dropout(0.2),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
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
    trainY = (trainY0 - trainY1)
    return trainX, trainY, trainY0, trainY1

def train(dataDf3,message):
    global lossAndErrorPrintingCallbackSuffixStr
    
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta = 1,patience=1000)
    for _ in range(10):
        for geo in geoList:
            name = geo['name']

            # 各种命名都用这个后缀，防止重名
            filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
            # 每次都重新建立mod
            mod = createModFunc3()

            # 只是临时存储，这个路径下的所有文件都可以在训练后清除  
            checkpoint_filepath = '/src/src/predSkan/geo/mod/std/mod%s_%s_{epoch:05d}-{val_loss:.2f}.h5'%(name,filenameSuffix)
        
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = geo['name']

            trainX, trainY,_,_ = getTrainingData(dataDf3,name,'2022-08-01','2022-08-31')
            testX, testY, trainY0, trainY1 = getTrainingData(dataDf3,name,'2022-09-01','2022-09-30')

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
            val_loss = createDoc('/src/src/predSkan/geo/mod/std','mod%s'%name,trainX, trainY,testX,testY,history,docDirname,message, trainY0, trainY1)
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

            # logFilename 记录最好的5条摘要
            logFilename = os.path.join(logDir,'log.txt')
            
            with open(logFilename, 'w') as f:
                # 只记录前5条
                df = retDf.sort_values(by=['val_loss'])
                l = min(len(retDf),5)
                lines = '%s\n'%name
                for i in range(l):
                    lines += 'mape:%.2f%%,path:%s,message:%s\n'%(df.iloc[i].at['val_loss'],df.iloc[i].at['path'],df.iloc[i].at['message'])
                f.write(lines)


import numpy as np
def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

from shutil import copyfile
import matplotlib.pyplot as plt
# 生成文档，暂时先把所有需要的东西凑齐，生成文档之后再说
def createDoc(modPath,modSuffix,trainX,trainY,testX,testY,history,docDirname,message, trainY0, trainY1):
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
    # mod针对训练集表现
    trainYP = mod.predict(trainX)

    pd.DataFrame(data={
        'true':list(trainY.reshape(-1)),
        'pred':list(trainYP.reshape(-1)),
        'mape':list(np.abs((trainYP.reshape(-1) - trainY.reshape(-1)) / trainY.reshape(-1))*100)
    }).to_csv(os.path.join(docDirname, 'train.csv'))

    trainMape = mapeFunc(trainY.reshape(-1),trainYP.reshape(-1))
    retStr += 'train mape:%.2f%%\n'%(trainMape)

    plt.title("train")
    plt.plot(trainY.reshape(-1),'b-',label='true')
    plt.plot(trainYP.reshape(-1),'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'train.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()
    
    # mod针对测试集表现
    testYP = mod.predict(testX)
    testYP = testYP.reshape(-1) + trainY1.reshape(-1)

    testY = trainY0
    pd.DataFrame(data={
        'true':list(testY.reshape(-1)),
        'pred':list(testYP.reshape(-1)),
        'mape':list(np.abs((testYP.reshape(-1) - testY.reshape(-1)) / testY.reshape(-1))*100)
    }).to_csv(os.path.join(docDirname, 'test.csv'))

    testMape = mapeFunc(testY.reshape(-1),testYP.reshape(-1))
    retStr += 'test mape:%.2f%%\n'%(testMape)

    plt.title("test")
    plt.plot(testY.reshape(-1),'b-',label='true')
    plt.plot(testYP.reshape(-1),'r-',label='pred')
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
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        # dataStep0('20220501','20220930')
        df = dataStep1('20220501','20220930')
        df3 = dataStep3(df)
        df4 = dataStep4(df3)
        df4.to_csv(getFilename('geoData_20220501_20220930'))
    df4 = pd.read_csv(getFilename('geoData_20220501_20220930'))

    train(df4,'pred sum(r2~r7)')
