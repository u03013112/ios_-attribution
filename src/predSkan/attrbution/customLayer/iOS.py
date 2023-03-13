# 自定义层的iOS应用
# 原始数据与撞库使用相同的原始数据

# 暂时结论不好，各种偏，总是不能很好地将4各媒体分的均匀，而是偏向其中几个媒体，剩下的媒体毫无贡献
# 目前想到的的解决方案
# 1、增加数据数量，每天都做一个最近7天的汇总，或者平均
# 2、修改标准化方式，目前分开标准化可能会导致学偏

import datetime
import pandas as pd

import os
import sys
import numpy as np
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import afCvMapDataFrame
from src.predSkan.attrbution.customLayer.createMod import createModEasy04,createModEasy04_adam,createModEasy04_a,createModEasy04_b
from src.predSkan.tools.ai import purgeRetCsv,logUpdate

# 
def getFilename(filename):
    return '/src/data/customLayer/%s.csv'%(filename)

# 需要数据收集
# skan 3个媒体的 cv分布，和af所有数据的cv分布，计算出 自然量cv分布
# 按照上面cv分布计算出首日付费金额
# 真实7日回收作为y

# 暂时只看着3个媒体
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds']},
    {'name':'unknown','codeList':[]}
]

# 返回自然量
def step1():
    afDf = pd.read_csv('/src/data/zk/step0_afDf4.csv')
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]

    skanDf = pd.read_csv('/src/data/zk/step0_skanDf4.csv')
    skanDf = skanDf.loc[:,~skanDf.columns.str.match('Unnamed')]


    # 为了计算自然量，将af和skan都按照安装日期+cv汇总，并相减
    afSumDf = afDf.groupby(by = ['install_date_group','cv'],as_index=False).agg({
        'count':'sum'
    })
    skanSumDf = skanDf.groupby(by = ['install_date_group','cv'],as_index=False).agg({
        'count':'sum'
    })

    mergeDf = afSumDf.merge(skanSumDf,how='left',on = ['install_date_group','cv'],suffixes=('_af','_skan'))
    mergeDf.loc[:,'count'] = mergeDf['count_af'] - mergeDf['count_skan']
    organicDf = mergeDf.drop(['count_af','count_skan'], axis=1)
    # 简单处理，这里确实有一些cv是负的
    organicDf.loc[organicDf['count']<0,'count'] = 0

    if __debug__:
        mergeDf.to_csv(getFilename('step1_mergeDf'))
        organicDf.to_csv(getFilename('step1_organicDf'))

    return organicDf

# 返回3个媒体的cv分布
# 按照顺序 ： bd，fb，gg
# 暂时只看着3个媒体
mediaNameList = [
    'bytedance',
    'facebook',
    'google'
]
def step2():
    skanDf = pd.read_csv('/src/data/zk/step0_skanDf4.csv')
    skanDf = skanDf.loc[:,~skanDf.columns.str.match('Unnamed')]

    mediaDfList = []
    for mediaName in mediaNameList:
        mediaDf = skanDf.loc[skanDf.media_group == mediaName]
        mediaDfList.append(mediaDf)
        if __debug__:
            mediaDf.to_csv(getFilename('step2_mediaDf_%s'%mediaName))

    return mediaDfList

# 为cv分布添加首日付费金额
# 作为64 + 1 中的 +1
def step3():
    # 给Df添加usd列
    def addUSD(df):
        for i in range(len(afCvMapDataFrame)):
            min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
            max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
            if pd.isna(max_event_revenue):
                max_event_revenue = 0
            avg = (min_event_revenue + max_event_revenue)/2
            df.loc[df.cv == i,'usd'] = df['count']*avg
        return df
    
    organicDf = pd.read_csv(getFilename('step1_organicDf'))
    organicDf = addUSD(organicDf)
    organicSumDf = organicDf.groupby(by = ['install_date_group'],as_index=False).agg({
        'usd':'sum'
    })
    if __debug__:
        organicSumDf.to_csv(getFilename('step3_organicSumDf'))

    for mediaName in mediaNameList:
        mediaDf = pd.read_csv(getFilename('step2_mediaDf_%s'%mediaName))
        mediaDf = addUSD(mediaDf)
        mediaSumDf = mediaDf.groupby(by = ['install_date_group'],as_index=False).agg({
            'usd':'sum'
        })
        if __debug__:
            mediaSumDf.to_csv(getFilename('step3_mediaSumDf_%s'%mediaName))

# 获得总收入，作为y
def step4():
    afDf = pd.read_csv('/src/data/zk/step0_afDf4.csv')
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]

    afSumDf = afDf.groupby(by = ['install_date_group'],as_index=False).agg({
        'sumr7usd':'sum'
    })

    if __debug__:
        afSumDf.to_csv(getFilename('step4_SumDf'))
    return afSumDf

# 将不同媒体的64标准化
def step5():
    def fillCv(dataDf):
        # 每天补充满64组数据，没有的补0
        install_date_list = dataDf['install_date_group'].unique()
        for install_date in install_date_list:
            # print(install_date)
            df = dataDf.loc[(dataDf.install_date_group == install_date)]
            dataNeedAppend = {
                'install_date_group':[],
                'cv':[],
                'count':[]
            }
            for i in range(64):
                if df.loc[(df.cv == i),'count'].sum() == 0:
                    dataNeedAppend['install_date_group'].append(install_date)
                    dataNeedAppend['cv'].append(i)
                    dataNeedAppend['count'].append(0)

            dataDf = dataDf.append(pd.DataFrame(data=dataNeedAppend))
        
        dataDf = dataDf.groupby(by=['install_date_group','cv'],as_index=False).agg({
            'count':'sum'
        }).sort_values(by=['install_date_group','cv']).reset_index(drop=True)
        return dataDf

    def std(dataX,min,max):
        x = (dataX-min)/(max-min)
        x[x == np.inf] = 0
        x[x == -np.inf] = 0
        trainXSs = np.nan_to_num(x)
        return trainXSs

    def getTrainX(trainDf):
        trainX = trainDf['count'].to_numpy().reshape((-1,64))
        trainXSum = trainX.sum(axis=1).reshape(-1,1)
        trainX = trainX/trainXSum
        if __debug__:
            print('getTrainX1',trainX)
        min = trainX.T.min(axis=1)
        max = trainX.T.max(axis=1)    
        if __debug__:
            print('min',min)
            print('max',max)
        trainX = std(trainX,min,max)
        if __debug__:
            print('getTrainX2',trainX)
        return trainX

    np64List = []

    for mediaName in mediaNameList:
        mediaDf = pd.read_csv(getFilename('step2_mediaDf_%s'%mediaName))
        mediaDf = fillCv(mediaDf)
        mediaNp64 = getTrainX(mediaDf)
        # 按顺序加入
        np64List.append(mediaNp64)

    organicDf = pd.read_csv(getFilename('step1_organicDf'))
    organicDf = fillCv(organicDf)
    organicNp64 = getTrainX(organicDf)
    # 最后是自然量
    np64List.append(organicNp64)
    
    return np64List

# 将训练数据拼起来
# 部分数据读取自之前步骤的数据文件
def step6(np64List):
    np1List = []
    for mediaName in mediaNameList:
        mediaSumDf = pd.read_csv(getFilename('step3_mediaSumDf_%s'%mediaName))
        mediaNp1 = mediaSumDf['usd'].to_numpy().reshape((-1,1))
        np1List.append(mediaNp1)
    
    mediaSumDf = pd.read_csv(getFilename('step3_organicSumDf'))
    mediaNp1 = mediaSumDf['usd'].to_numpy().reshape((-1,1))
    np1List.append(mediaNp1)

    x = np.concatenate(
        (np64List[0],np1List[0],
        np64List[1],np1List[1],
        np64List[2],np1List[2],
        np64List[3],np1List[3])
    ,axis=1)
    
    # print(x.shape)

    return x


from tensorflow import keras
epochMax = 300
lossAndErrorPrintingCallbackSuffixStr = ''
class LossAndErrorPrintingCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        global lossAndErrorPrintingCallbackSuffixStr
        if epoch > 0 and epoch%100 == 0:
            keys = list(logs.keys())
            str = 'epoch %d/%d:'%(epoch,epochMax)
            for key in keys:
                str += '[%s]:%.3f '%(key,logs[key])
            print(lossAndErrorPrintingCallbackSuffixStr,str)

import tensorflow as tf
# 由于数据量过少，就不再搞那么多花里胡哨的东西，少训练几次，不考虑过拟合
def train(x,y,message):
    name = 'iOSCustom'
    global lossAndErrorPrintingCallbackSuffixStr
    lossAndErrorPrintingCallbackSuffixStr = name
    for _ in range(10):
        filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
        # mod = createModEasy04()
        # mod = createModEasy04_adam()
        mod = createModEasy04_b()

        modPath = '/src/src/predSkan/androidTotal/mod/%s%s/'%(name,filenameSuffix)
        checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.3f}-{val_loss:.3f}.h5')
    
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True
        )

        trainingX = x[0:30]
        trainingY = y[0:30]
        testingX = x[30:]
        testingY = y[30:]

        history = mod.fit(trainingX, trainingY, epochs=epochMax, validation_data=(testingX,testingY)
            ,callbacks=[
                # earlyStoppingValLoss,
                model_checkpoint_callback,
                LossAndErrorPrintingCallback()
            ]
            ,batch_size=32
            ,verbose=0
            )
        # 训练完成可以把mod清理掉了
        tf.keras.backend.clear_session()
        del mod

        logDir = '/src/data/doc/customLayer/'

        os.makedirs(logDir,exist_ok=True)
        # 将每次的明细结果放进去，
        docDirname = '%s/%s'%(logDir,name+filenameSuffix)
        val_loss = createDoc(modPath,trainingX,trainingY,testingX,testingY,history,docDirname,message)
        
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

from shutil import copyfile
import matplotlib.pyplot as plt
def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

# 与之前可以验证的文档相比，这个文档只验证总金额，并不验证分媒体
def createDoc(modPath,trainX,trainY,testX,testY,history,docDirname,message):
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
    
    bdMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-0').output)
    fbMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-1').output)
    ggMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-2').output)
    ogMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-3').output)
    
    bdMod2 = keras.models.Model(mod.input,mod.get_layer('muti-0').output)
    fbMod2 = keras.models.Model(mod.input,mod.get_layer('muti-1').output)
    ggMod2 = keras.models.Model(mod.input,mod.get_layer('muti-2').output)
    ogMod2 = keras.models.Model(mod.input,mod.get_layer('muti-3').output)

    retStr = '%s\n'%message
    retStr += '%s\n'%bestMod['path']
    # mod针对训练集表现
    
    trainY = trainY.reshape(-1)
    trainYP = mod.predict(trainX).reshape(-1)

    trainYPBd1 = bdMod1.predict(trainX).reshape(-1)
    trainYPFb1 = fbMod1.predict(trainX).reshape(-1)
    trainYPGg1 = ggMod1.predict(trainX).reshape(-1)
    trainYPOg1 = ogMod1.predict(trainX).reshape(-1)

    trainYPBd2 = bdMod2.predict(trainX).reshape(-1)
    trainYPFb2 = fbMod2.predict(trainX).reshape(-1)
    trainYPGg2 = ggMod2.predict(trainX).reshape(-1)
    trainYPOg2 = ogMod2.predict(trainX).reshape(-1)
    

    pd.DataFrame(data={
        'true':list(trainY),
        'pred':list(trainYP),
        'predBd1':list(trainYPBd1),
        'predFb1':list(trainYPFb1),
        'predGg1':list(trainYPGg1),
        'predOg1':list(trainYPOg1),
        'predBd2':list(trainYPBd2),
        'predFb2':list(trainYPFb2),
        'predGg2':list(trainYPGg2),
        'predOg2':list(trainYPOg2),
        'mape':list(np.abs((trainYP - trainY) / trainY)*100)
    }).to_csv(os.path.join(docDirname, 'train.csv'))

    trainMape = mapeFunc(trainY,trainYP)

    retStr += 'train mape:%.2f%%\n'%(trainMape)

    plt.title("train")
    plt.plot(trainY,'b-',label='true')
    plt.plot(trainYP,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'train.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("r7/r1 train")
    plt.plot(trainYPBd1,label='bd')
    plt.plot(trainYPFb1,label='fb')
    plt.plot(trainYPGg1,label='gg')
    plt.plot(trainYPOg1,label='og')
    plt.legend()
    filename = os.path.join(docDirname, 'trainR7R1.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    # TODO:ROI画图

    # mod针对测试集表现
    testY = testY.reshape(-1)
    testYP = mod.predict(testX).reshape(-1)

    testYPBd1 = bdMod1.predict(testX).reshape(-1)
    testYPFb1 = fbMod1.predict(testX).reshape(-1)
    testYPGg1 = ggMod1.predict(testX).reshape(-1)
    testYPOg1 = ogMod1.predict(testX).reshape(-1)

    testYPBd2 = bdMod2.predict(testX).reshape(-1)
    testYPFb2 = fbMod2.predict(testX).reshape(-1)
    testYPGg2 = ggMod2.predict(testX).reshape(-1)
    testYPOg2 = ogMod2.predict(testX).reshape(-1)
    

    pd.DataFrame(data={
        'true':list(testY),
        'pred':list(testYP),
        'predBd1':list(testYPBd1),
        'predFb1':list(testYPFb1),
        'predGg1':list(testYPGg1),
        'predOg1':list(testYPOg1),
        'predBd2':list(testYPBd2),
        'predFb2':list(testYPFb2),
        'predGg2':list(testYPGg2),
        'predOg2':list(testYPOg2),
        'mape':list(np.abs((testYP - testY) / testY)*100)
    }).to_csv(os.path.join(docDirname, 'test.csv'))

    testMape = mapeFunc(testY,testYP)

    retStr += 'test mape:%.2f%%\n'%(testMape)

    plt.title("test")
    plt.plot(testY,'b-',label='true')
    plt.plot(testYP,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'test.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("r7/r1 test")
    plt.plot(testYPBd1,label='bd')
    plt.plot(testYPFb1,label='fb')
    plt.plot(testYPGg1,label='gg')
    plt.plot(testYPOg1,label='og')
    plt.legend()
    filename = os.path.join(docDirname, 'testR7R1.png')
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


def main():
    # step1()
    # step2()
    # step3()
    # step4()
    # np64List = step5()
    # x = step6(np64List)
    # y = pd.read_csv(getFilename('step4_SumDf'))['sumr7usd'].to_numpy().reshape(-1,1)

    # np.save('/src/data/customLayer/x.npy',x)
    # np.save('/src/data/customLayer/y.npy',y)

    x = np.load('/src/data/customLayer/x.npy')
    y = np.load('/src/data/customLayer/y.npy')

    # xDf = pd.DataFrame(x, columns = list(np.arange(260)))
    # xDf.to_csv(getFilename('x'))

    train(x,y,'ios test3_b adam')


if __name__ == '__main__':
    main()
    