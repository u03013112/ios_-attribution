# 尝试将之前训练好的整体模型进行拆分，并分别计算MAPE
import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras

import sys
sys.path.append('/src')
from src.tools import getFilename

def loadMod(modDocPath):
    modPath = os.path.join(modDocPath,'bestMod.h5')
    mod = tf.keras.models.load_model(modPath)

    # mod.summary()
    return mod

def getMediaMod(mod):
    outputLayerName = 'muti0'
    # outputLayerName = 'meidaAddOne'
    mediaMod = keras.models.Model(mod.input,mod.get_layer(outputLayerName).output)
    return mediaMod

def getOtherMod(mod):
    outputLayerName = 'muti1'
    # outputLayerName = 'otherAddOne'
    otherMod = keras.models.Model(mod.input,mod.get_layer(outputLayerName).output)
    return otherMod


def getTrainX(trainDf):
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainXSum = trainX.sum(axis=1).reshape(-1,1)
    trainX = trainX/trainXSum
    return trainX

def getXMinAndMaxFromTrainX(trainX):
    min = trainX.T.min(axis=1)
    max = trainX.T.max(axis=1)    
    return min,max

def std(dataX,min,max):
    x = (dataX-min)/(max-min)
    x[x == np.inf] = 0
    x[x == -np.inf] = 0
    trainXSs = np.nan_to_num(x)
    return trainXSs


def dataStep1(df):
    mediaDf = df.loc[df.media_group == 'google']
    media64Df = mediaDf.groupby(['install_date','cv']).agg('sum').sort_values(by=['install_date','cv'])
    media64 = getTrainX(media64Df)
    media1Df = mediaDf.groupby(['install_date']).agg('sum').sort_values(by=['install_date'])
    media1 = media1Df['sumr1usd'].to_numpy()
    mediaMin,mediaMax = getXMinAndMaxFromTrainX(media64)
    media64 = std(media64,mediaMin,mediaMax)
    # TODO:save mediaMin,mediaMax here

    otherDf = df.loc[df.media_group == 'unknown']
    other64Df = otherDf.groupby(['install_date','cv']).agg('sum').sort_values(by=['install_date','cv'])
    other64 = getTrainX(other64Df)
    other1Df = otherDf.groupby(['install_date']).agg('sum').sort_values(by=['install_date'])
    other1 = other1Df['sumr1usd'].to_numpy()
    otherMin,otherMax = getXMinAndMaxFromTrainX(other64)
    other64 = std(other64,otherMin,otherMax)
    # TODO:save otherMin,otherMax here

    y = media1Df['sumr7usd'].to_numpy() + other1Df['sumr7usd'].to_numpy()

    return media64,media1,other64,other1,y,media1Df['sumr7usd'].to_numpy(),other1Df['sumr7usd'].to_numpy()

def dataStep2(media64,media1,other64,other1,y):
    media64 = media64.reshape(-1,64)
    media1 = media1.reshape(-1,1)
    other64 = other64.reshape(-1,64)
    other1 = other1.reshape(-1,1)

    x = np.concatenate((media64,media1,other64,other1),axis=1)
    return x
    # 简单切割一下，为了分出训练集与测试集
    # trainingX = x[0:98]
    # testingX = x[98:123]

    # trainingY = y[0:98]
    # testingY = y[98:123]
    # return trainingX,testingX,trainingY,testingY

def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

if __name__ == '__main__':
    mod = loadMod('/src/data/doc/customLayer/total_20230224_022016')
    mediaMod = getMediaMod(mod)
    # mediaMod.summary()
    otherMod = getOtherMod(mod)


    afDf = pd.read_csv(getFilename('afDataR7C_20221001_20230201'))

    media64,media1,other64,other1,y,mediaY,otherY = dataStep1(afDf)
    x = dataStep2(media64,media1,other64,other1,y)

    
    yp = mediaMod.predict(x)
    # print(yp)

    # yp = otherMod.predict(x)
    # print(yp)

    mediaMape = mapeFunc(mediaY,yp)
    print(mediaMape)
