import pandas as pd
import numpy as np

# 将反复添加Unnamed的csv清理一下
def purgeRetCsv(retCsvFilename):
    retDf = pd.read_csv(retCsvFilename)
    retDf.loc[:,~retDf.columns.str.match('Unnamed')].to_csv(retCsvFilename)

# 输入ret.csv的文件全路径，将新的结论写入到log.txt
# titleStr 是写在log.txt的第一行
def logUpdate(retCsvFilename,logTxtFilename,titleStr):
    retDf = pd.read_csv(retCsvFilename)
    retDf = retDf.fillna(value=666)
    with open(logTxtFilename, 'w') as f:
        df = retDf.loc[retDf.groupby('message').val_loss.idxmin()].reset_index(drop=True)
        df = df.sort_values(by=['val_loss'])
        # print(df)
        lines = '%s\n'%titleStr
        for i in range(len(df)):
            lines += 'mape:%.2f%%,path:%s,message:%s\n'%(df.iloc[i].at['val_loss'],df.iloc[i].at['path'],df.iloc[i].at['message'])
        f.write(lines)

# 将DataFrameCsv转化成NumpyNpy，主要目的是帮助mean和std
# c是列名
def DataFrameCsvToNumpyNpy(csvFilename,c,npyFilename):
    df = pd.read_csv(csvFilename)
    np.save(npyFilename,df[c].to_numpy())


import numpy as np
def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

def weighted_mape_tf(y_true,y_pred):
    tot = tf.reduce_sum(y_true)
    tot = tf.clip_by_value(tot, clip_value_min=1,clip_value_max=1000)
    wmape = tf.realdiv(tf.reduce_sum(tf.abs(tf.subtract(y_true,y_pred))),tot)*100#/tot

    return(wmape)

import os
from shutil import copyfile
import matplotlib.pyplot as plt
import tensorflow as tf
# 生成文档，暂时先把所有需要的东西凑齐，生成文档之后再说
def createDoc(modPath,trainX,trainY0, trainY1,testX,testY0, testY1,history,docDirname,message):
    os.makedirs(docDirname,exist_ok=True)
    # 需要将mod先copy出来
    modList = []
    g = os.walk(modPath)
    for modPath,dirList,fileList in g:  
        for fileName in fileList:  
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
        'yp':list(trainYP.reshape(-1)),
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
        'yp':list(testYP.reshape(-1)),
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


import math
# 获得X的最大值和最小值
# trainX 要做成numpy array,shape (-1,64)
def getXMinAndMaxFromTrainX(trainX):
    min = trainX.T.min(axis=1)
    max = trainX.T.max(axis=1)    
    return min,max

# 从df里获得trainX，请在外面把df排好序
# df要先做好过滤，比如媒体过滤，然后放进来
# trainDf = df.sort_values(by=['install_date','cv'])
# trainDf = df.groupby(['install_date','cv']).agg('sum')
def getTrainX(trainDf):
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainXSum = trainX.sum(axis=1).reshape(-1,1)
    trainX = trainX/trainXSum
    return trainX

def getTrainingDataY(trainDf):
    trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    trainY0 = trainSumByDay['sumr7usd'].to_numpy()
    trainY1 = trainSumByDay['sumr1usd'].to_numpy()
    # 这里为了解决部分0数据
    trainY0[trainY0 <= 0] = 1
    trainY1[trainY1 <= 0] = 1
    trainY = trainY0/trainY1 - 1
    return trainY, trainY0, trainY1

def filterByMediaName(df,mediaName,trainRate=0.6):
    dataDf = df.loc[(df.media_group == mediaName)].sort_values(by=['install_date','cv'])
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

# 用于计算other的
def filterByMediaName2(df,mediaName,trainRate=0.6):
    dataDf = df.loc[(df.media_group != mediaName)].sort_values(by=['install_date','cv'])
    dataDf = dataDf.groupby(['install_date','cv']).agg('sum')
    dataX = getTrainX(dataDf)
    min,max = getXMinAndMaxFromTrainX(dataX)
    trainXSs = np.nan_to_num((dataX-min)/(max-min))
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

# 换了标准化方法 trainXSs = (trainX - mean)/std
def filterByMediaNameS(df,mediaName,trainRate=0.6):
    dataDf = df.loc[(df.media_group == mediaName)].sort_values(by=['install_date','cv'])
    dataDf = dataDf.groupby(['install_date','cv']).agg('sum')
    dataX = getTrainX(dataDf)
    # min,max = getXMinAndMaxFromTrainX(dataX)
    # x = (dataX-min)/(max-min)
    
    mean = np.mean(dataX,axis=0)
    std = np.std(dataX,axis=0)
    x = (dataX - mean)/std

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

    return trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1, mean, std

def filterByMediaNameS2(df,mediaName,trainRate=0.6):
    dataDf = df.loc[(df.media_group != mediaName)].sort_values(by=['install_date','cv'])
    dataDf = dataDf.groupby(['install_date','cv']).agg('sum')
    dataX = getTrainX(dataDf)
    # min,max = getXMinAndMaxFromTrainX(dataX)
    # x = (dataX-min)/(max-min)
    
    mean = np.mean(dataX,axis=0)
    std = np.std(dataX,axis=0)
    x = (dataX - mean)/std

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

    return trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1, mean, std


if __name__ == '__main__':
    # for geo in ['US','GCC','KR','T1']:
    #     # purgeRetCsv('/src/data/doc/geo/%s/ret.csv'%geo)
    #     # logUpdate('/src/data/doc/geo/%s/ret.csv'%geo,'/src/data/doc/geo/%s/log.txt'%geo,'%s'%geo)
    #     meanCsvFilename = '/src/data/%sMean20220501_20220731.csv'%geo
    #     c = 'mean'
    #     meanNpyFilename = '/src/data/%sMean.npy'%geo
    #     DataFrameCsvToNumpyNpy(meanCsvFilename,c,meanNpyFilename)

    #     stdCsvFilename = '/src/data/%sStd20220501_20220731.csv'%geo
    #     c = 'std'
    #     stdNpyFilename = '/src/data/%sStd.npy'%geo
    #     DataFrameCsvToNumpyNpy(stdCsvFilename,c,stdNpyFilename)

    meanCsvFilename = '/src/data/totalMean20220501_20220731.csv'
    c = 'mean'
    meanNpyFilename = '/src/data/totalMean.npy'
    DataFrameCsvToNumpyNpy(meanCsvFilename,c,meanNpyFilename)

    stdCsvFilename = '/src/data/totalStd20220501_20220731.csv'
    c = 'std'
    stdNpyFilename = '/src/data/totalStd.npy'
    DataFrameCsvToNumpyNpy(stdCsvFilename,c,stdNpyFilename)