# 自定义层的iOS应用
# 原始数据与撞库使用相同的原始数据

# 尝试增加数据量，来解决模型偏向问题
# 由之前每7天汇总成一个数据，改为用向前的7日均线来做，这样数据量是之前的7倍 少一点（前6天的数据放弃）

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
    {'name':'bytedance','codeList':['bytedanceglobal_int'],'sname':'Bd'},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds'],'sname':'Fb'},
    {'name':'google','codeList':['googleadwords_int'],'sname':'Gg'},
    {'name':'unknown','codeList':[],'sname':''}
]

def addMediaGroup(df):
    # 所有不属于mediaList的都算是unknown，和自然量一起处理
    df.insert(df.shape[1],'media_group','unknown')
    for media in mediaList:
        name = media['name']
        for code in media['codeList']:
            df.loc[df.media == code,'media_group'] = name
    return df

# 重做install_date_group
# 不再是7天汇总成一个值，而是每天都是前7天的均值
def step0():
    def dataFill(dataDf3):
        # 每天补充满64组数据，没有的补0
        install_date_list = dataDf3['install_date'].unique()
        for install_date in install_date_list:
            # print(install_date)
            df = dataDf3.loc[(dataDf3.install_date == install_date)]
            dataNeedAppend = {
                'install_date':[],
                'cv':[],
                'media_group':[]
            }
            for i in range(64):
                for media in mediaList:
                    name = media['name']
                    if df.loc[(df.cv == i)]['count'].sum() == 0:
                        dataNeedAppend['install_date'].append(install_date)
                        dataNeedAppend['cv'].append(i)
                        dataNeedAppend['media_group'].append(name)

            dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
        dataDf3 = dataDf3.sort_values(by=['install_date','cv','media']).reset_index(drop=True)
        dataDf3 = dataDf3.fillna(0)
        return dataDf3


    afDf = pd.read_csv('/src/data/zk/iOSAF20220501_20230227.csv')
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]
    skanDf = pd.read_csv('/src/data/zk/iOSSKAN20220501_20230227.csv')
    skanDf = skanDf.loc[:,~skanDf.columns.str.match('Unnamed')]

    skanDf = skanDf.rename(columns={'install_date_af':'install_date'})
    
    afDf3 = addMediaGroup(afDf)
    skanDf3 = addMediaGroup(skanDf)


    afDf3 = dataFill(afDf3)
    skanDf3 = dataFill(skanDf3)

    afDf4 = afDf3.groupby(by=['install_date','media_group','cv'],as_index=False).agg({
        'count':'sum',
        'sumr1usd':'sum',
        'sumr7usd':'sum',
    }).sort_values(by=['media_group','cv','install_date'],ignore_index=True)

    afDf4['count7'] = afDf4['count'].rolling(7).sum()
    afDf4['sumr1usd7'] = afDf4['sumr1usd'].rolling(7).sum()
    afDf4['sumr7usd7'] = afDf4['sumr7usd'].rolling(7).sum()

    afDf4 = afDf4.drop(['count','sumr1usd','sumr7usd'], axis=1)
    afDf4 = afDf4.rename(columns={
        'install_date':'install_date_group',
        'count7':'count',
        'sumr1usd7':'sumr1usd',
        'sumr7usd7':'sumr7usd'
    })
    afDf4 = afDf4.loc[afDf4.install_date_group > '2022-05-06']

    skanDf4 = skanDf3.groupby(by=['install_date','media_group','cv'],as_index=False).agg({
        'count':'sum'
    }).sort_values(by=['media_group','cv','install_date'],ignore_index=True)

    skanDf4['count7'] = skanDf4['count'].rolling(7).sum()
    skanDf4 = skanDf4.drop(['count'], axis=1)
    skanDf4 = skanDf4.rename(columns={
        'install_date':'install_date_group',
        'count7':'count'
    })
    skanDf4 = skanDf4.loc[skanDf4.install_date_group > '2022-05-06']

    if __debug__:
        skanDf4.to_csv(getFilename('step0_skanDf4'))
        afDf4.to_csv(getFilename('step0_afDf4'))
    


# 返回自然量
def step1():
    afDf = pd.read_csv(getFilename('step0_afDf4'))
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]

    skanDf = pd.read_csv(getFilename('step0_skanDf4'))
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
    skanDf = pd.read_csv(getFilename('step0_skanDf4'))
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
    afDf = pd.read_csv(getFilename('step0_afDf4'))
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
        mediaDf = mediaDf.loc[mediaDf.install_date_group < '2023-02-27']
    
        mediaDf = fillCv(mediaDf)
        mediaNp64 = getTrainX(mediaDf)
        # 按顺序加入
        np64List.append(mediaNp64)

    organicDf = pd.read_csv(getFilename('step1_organicDf'))
    organicDf = organicDf.loc[organicDf.install_date_group < '2023-02-27']
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
        mediaSumDf = mediaSumDf.loc[mediaSumDf.install_date_group < '2023-02-27']
        mediaNp1 = mediaSumDf['usd'].to_numpy().reshape((-1,1))
        np1List.append(mediaNp1)
    
    mediaSumDf = pd.read_csv(getFilename('step3_organicSumDf'))
    mediaSumDf = mediaSumDf.loc[mediaSumDf.install_date_group < '2023-02-27']
    mediaNp1 = mediaSumDf['usd'].to_numpy().reshape((-1,1))
    np1List.append(mediaNp1)

    print(np64List[0].shape,np1List[0].shape,
        np64List[1].shape,np1List[1].shape,
        np64List[2].shape,np1List[2].shape,
        np64List[3].shape,np1List[3].shape)

    x = np.concatenate(
        (np64List[0],np1List[0],
        np64List[1],np1List[1],
        np64List[2],np1List[2],
        np64List[3],np1List[3])
    ,axis=1)
    
    # print(x.shape)

    return x


from tensorflow import keras
epochMax = 500
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

        trainingX = x[0:180]
        trainingY = y[0:180]
        testingX = x[180:]
        testingY = y[180:]

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

def cleanDf(df):
    df = df.loc[:,~df.columns.str.match('Unnamed')]
    return df

def report(docDirname):
    trainDf = pd.read_csv(os.path.join(docDirname, 'train.csv'))
    trainDf = trainDf.loc[:,~trainDf.columns.str.match('Unnamed')]
    testDf = pd.read_csv(os.path.join(docDirname, 'test.csv'))
    testDf = testDf.loc[:,~testDf.columns.str.match('Unnamed')]
    totalDf = trainDf.append(testDf)
    totalDf = totalDf.reset_index(drop=True)
    
    # print(len(trainDf),len(testDf),len(totalDf))
    # 添加安装日期
    install_date_group = pd.read_csv(getFilename('step4_SumDf'))['install_date_group']
    totalDf.loc[:,'install_date_group'] = install_date_group
    # print(totalDf)
    totalDf.to_csv(getFilename('reportTotalDf'))
    # 添加广告花费
    adCostDf = pd.read_csv(getFilename('adCost'))
    adCostDf = adCostDf.loc[:,~adCostDf.columns.str.match('Unnamed')]
    adCostDf2 = addMediaGroup(adCostDf)
    adCostDf3 = adCostDf2.groupby(by=['install_date','media_group'],as_index=False).agg({
        'cost':'sum'
    }).sort_values(by=['media_group','install_date'],ignore_index=True)
    adCostDf3.loc[:,'cost7'] = adCostDf3['cost'].rolling(7).sum()
    adCostDf4 = adCostDf3.drop(['cost'], axis=1)
    adCostDf4 = adCostDf4.rename(columns={
        'install_date':'install_date_group',
        'cost7':'cost'
    })
    adCostDf4 = adCostDf4.loc[(adCostDf4.install_date_group > '2022-05-06') & (adCostDf4.install_date_group < '2023-02-27')].reset_index(drop=True)
    adCostDf4.to_csv(getFilename('reportAdCost'))
    
    # 计算ROI
    plt.title("7d ROI")
    plt.figure(figsize=(10.8, 3.2))
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaAdCost = adCostDf4.loc[adCostDf4.media_group == name].reset_index(drop=True)
        sname = 'pred%s2'%(media['sname'])
        print(sname)
        print(mediaAdCost)
        r7usd = totalDf[sname].reset_index(drop=True)
        print(r7usd)
        mediaAdCost.loc[:,'r7usd'] = r7usd
        print(mediaAdCost)
        
        mediaAdCost.set_index(["install_date_group"], inplace=True)
        mediaAdCost.loc[:,'roi'] = mediaAdCost['r7usd']/mediaAdCost['cost']
        mediaAdCost['roi'].plot(label = name)
        mediaAdCost = cleanDf(mediaAdCost)
        mediaAdCost.to_csv(getFilename('roi_%s'%(name)))
    
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('/src/data/customLayer/roi.png')
    plt.clf()


def main():
    # step0()
    # step1()
    # step2()
    # step3()
    # step4()
    # np64List = step5()
    # x = step6(np64List)
    # y = pd.read_csv(getFilename('step4_SumDf'))['sumr7usd'].to_numpy().reshape(-1,1)
    # y = y[:-1]
    # np.save('/src/data/customLayer/xR7.npy',x)
    # np.save('/src/data/customLayer/yR7.npy',y)

    x = np.load('/src/data/customLayer/xR7.npy')
    y = np.load('/src/data/customLayer/yR7.npy')

    train(x,y,'ios test rolling data 180')


if __name__ == '__main__':
    # main()
    report('/src/data/doc/customLayer//iOSCustom_20230309_104015')
    