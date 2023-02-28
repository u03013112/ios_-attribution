# 为了发现为什么媒体的神经网络不能很有效的学到内容
import datetime

import numpy as np
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame
from src.predSkan.customLayer.createMod import createMod01,createMod02
from src.predSkan.tools.ai import purgeRetCsv,logUpdate

def getDataFromAF():
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)
    # 安卓没有idfv，只能用af id
    sql = '''
        select
            cv,
            count(*) as count,
            sum(r1usd) as sumR1usd,
            sum(r7usd) as sumR7usd,
            media,
            install_date
        from
        (
            select
                uid,
                case
                    when r1usd = 0
                    or r1usd is null then 0 %s
                    else 63
                end as cv,
                r1usd,
                r7usd,
                media,
                install_date
            from
            (
                select
                    appsflyer_id as uid,
                    to_char(
                        to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                        "yyyy-mm-dd"
                    ) as install_date,
                    sum(
                        case
                        when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                        else 0
                        end
                    ) as r1usd,
                    sum(
                        case
                        when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                        else 0
                        end
                    ) as r7usd,
                    media_source as media
                from
                    ods_platform_appsflyer_events
                where
                    app_id = 'com.topwar.gp'
                    and zone = 0
                    and day >= 20221001
                    and day <= 20230205
                    and install_time >= '2022-10-01'
                    and install_time < '2023-02-01'
                group by
                    install_date,
                    uid,
                    media
            )
        )group by
            cv,
            media,
            install_date    
        ;
    '''%(whenStr)
    
    print(sql)
    df = execSql(sql)
    return df

mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    # {'name':'bytedance','codeList':['bytedanceglobal_int']},
    # {'name':'facebook','codeList':['Social_facebook','restricted']},
    {'name':'unknown','codeList':[]}
]

def addMediaGroup(df):
    # 所有不属于mediaList的都算是unknown，和自然量一起处理
    df.insert(df.shape[1],'media_group','unknown')
    for media in mediaList:
        name = media['name']
        for code in media['codeList']:
            df.loc[df.media == code,'media_group'] = name
    return df

# 对数据做补充
def dataFill(dataDf3):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf3['install_date'].unique()
    for install_date in install_date_list:
        print(install_date)
        df = dataDf3.loc[(dataDf3.install_date == install_date)]
        dataNeedAppend = {
            'install_date':[],
            'cv':[],
            'count':[],
            'sumr1usd':[],
            'sumr7usd':[],
            'media_group':[]
        }
        for i in range(64):
            for media in mediaList:
                name = media['name']
                if df.loc[(df.cv == i) & (df.media_group == name),'sumr7usd'].sum() == 0 \
                    and df.loc[(df.cv == i) & (df.media_group == name),'count'].sum() == 0:

                    dataNeedAppend['install_date'].append(install_date)
                    dataNeedAppend['cv'].append(i)
                    dataNeedAppend['count'].append(0)
                    dataNeedAppend['sumr1usd'].append(0)
                    dataNeedAppend['sumr7usd'].append(0)
                    dataNeedAppend['media_group'].append(name)

        dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    dataDf3 = dataDf3.sort_values(by=['install_date','cv','media']).reset_index(drop=True)
    return dataDf3

# 训练数据整理
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

# 数据处理第一步：将数据整理成64 +1 + 64 +1，共4个Df，都是使用安装日期做key
# 以供后续处理
def dataStep1(df):
    mediaDf = df.loc[df.media_group == 'google']
    mediaDf.to_csv(getFilename('cccDebug01'))
    media64Df = mediaDf.groupby(['install_date','cv']).agg('sum').sort_values(by=['install_date','cv'])
    print('media64Df',media64Df)
    
    media64 = getTrainX(media64Df)
    print('media64',media64)
    media1Df = mediaDf.groupby(['install_date']).agg('sum').sort_values(by=['install_date'])
    media1 = media1Df['sumr1usd'].to_numpy()
    mediaMin,mediaMax = getXMinAndMaxFromTrainX(media64)
    print('mediaMin',mediaMin)
    print('mediaMax',mediaMax)

    media64 = std(media64,mediaMin,mediaMax)
    print('media64',media64)
    # TODO:save mediaMin,mediaMax here

    otherDf = df.loc[df.media_group == 'unknown']
    other64Df = otherDf.groupby(['install_date','cv']).agg('sum').sort_values(by=['install_date','cv'])
    other64 = getTrainX(other64Df)
    other1Df = otherDf.groupby(['install_date']).agg('sum').sort_values(by=['install_date'])
    other1 = other1Df['sumr1usd'].to_numpy()
    otherMin,otherMax = getXMinAndMaxFromTrainX(other64)
    other64 = std(other64,otherMin,otherMax)
    # TODO:save otherMin,otherMax here

    yMedia = media1Df['sumr7usd'].to_numpy()
    yOther = other1Df['sumr7usd'].to_numpy()
    y = yMedia + yOther

    return media64,media1,other64,other1,y,yMedia,yOther,mediaMin,mediaMax,otherMin,otherMax

# 将 64+1 + 64+1 组成一个大array，准备成为x
# 并进行切割，将数据切为两部分，8成训练，2成测试
def dataStep2(media64,media1,other64,other1,y,yMedia,yOther):
    media64 = media64.reshape(-1,64)
    media1 = media1.reshape(-1,1)
    other64 = other64.reshape(-1,64)
    other1 = other1.reshape(-1,1)

    x = np.concatenate((media64,media1,other64,other1),axis=1)
    # x = np.concatenate((other64,other1,media64,media1),axis=1)
    # 简单切割一下，为了分出训练集与测试集
    trainingX = x[0:98]
    testingX = x[98:123]

    trainingY = y[0:98]
    testingY = y[98:123]

    trainingMY = yMedia[0:98]
    testingMY = yMedia[98:123]

    trainingOY = yOther[0:98]
    testingOY = yOther[98:123]

    return trainingX,testingX,trainingY,testingY,trainingMY,testingMY,trainingOY,testingOY

from tensorflow import keras
epochMax = 100
# epochMax = 15000
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
def train(dataDf,message):
    global lossAndErrorPrintingCallbackSuffixStr

    for _ in range(10):
        name = 'total'
        filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')

        # mod = createMod01()
        mod = createMod02()

        modPath = '/src/src/predSkan/androidTotal/mod/%s%s/'%(name,filenameSuffix)
        checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.3f}-{val_loss:.3f}.h5')
    
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True
        )

        lossAndErrorPrintingCallbackSuffixStr = name
        
        media64,media1,other64,other1,y,yMedia,yOther,mediaMin,mediaMax,otherMin,otherMax = dataStep1(dataDf)
        trainingX,testingX,trainingY,testingY,trainingMY,testingMY,trainingOY,testingOY = dataStep2(media64,media1,other64,other1,y,yMedia,yOther)

        weight = np.load('/src/data/s20230224.npy')
        weight = weight[0:98]

        pd.DataFrame(trainingX).to_csv('/src/data/ccctrainingX.csv')
        pd.DataFrame(trainingY).to_csv('/src/data/ccctrainingY.csv')

        pd.DataFrame(testingX).to_csv('/src/data/ccctestingX.csv')
        pd.DataFrame(testingY).to_csv('/src/data/ccctestingY.csv')

        history = mod.fit(trainingX, trainingY, epochs=epochMax, validation_data=(testingX,testingY)
            ,callbacks=[
                # earlyStoppingValLoss,
                model_checkpoint_callback,
                LossAndErrorPrintingCallback()
            ]
            ,sample_weight = weight
            ,batch_size=128
            ,verbose=0
            )
        # 训练完成可以把mod清理掉了
        tf.keras.backend.clear_session()
        del mod

        logDir = '/src/data/doc/customLayer/'
        os.makedirs(logDir,exist_ok=True)
        # 将每次的明细结果放进去，
        docDirname = '%s/%s'%(logDir,name+filenameSuffix)
        val_loss = createDoc(modPath,trainingX,trainingY,testingX,testingY,history,docDirname,message,trainingMY,testingMY,trainingOY,testingOY)
        
        mediaMinFilename = os.path.join(docDirname,'mediaMin.npy')
        np.save(mediaMinFilename, mediaMin)
        mediaMaxFilename = os.path.join(docDirname,'mediaMax.npy')
        np.save(mediaMaxFilename, mediaMax)
        otherMinFilename = os.path.join(docDirname,'otherMin.npy')
        np.save(otherMinFilename, otherMin)
        otherMaxFilename = os.path.join(docDirname,'otherMax.npy')
        np.save(otherMaxFilename, otherMax)

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

def createDoc(modPath,trainX,trainY,testX,testY,history,docDirname,message,trainingMY,testingMY,trainingOY,testingOY):
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
    # mediaMod1 是 获得r7/r1的倍率
    mediaMod1 = keras.models.Model(mod.input,mod.get_layer('meidaAddOne').output)
    # mediaMod2 是 预测的媒体r7金额
    mediaMod2 = keras.models.Model(mod.input,mod.get_layer('muti0').output)
    # otherMod1 是 获得r7/r1的倍率
    otherMod1 = keras.models.Model(mod.input,mod.get_layer('otherAddOne').output)
    # otherMod2 是 预测的媒体r7金额
    otherMod2 = keras.models.Model(mod.input,mod.get_layer('muti1').output)

    retStr = '%s\n'%message
    retStr += '%s\n'%bestMod['path']
    # mod针对训练集表现
    
    trainYP = mod.predict(trainX).reshape(-1)

    trainYPMedia1 = mediaMod1.predict(trainX).reshape(-1)
    trainYPMedia2 = mediaMod2.predict(trainX).reshape(-1)
    trainYPOther1 = otherMod1.predict(trainX).reshape(-1)
    trainYPOther2 = otherMod2.predict(trainX).reshape(-1)

    pd.DataFrame(data={
        'true':list(trainY),
        'pred':list(trainYP),
        'predM1':list(trainYPMedia1),
        'predM2':list(trainYPMedia2),
        'media':list(trainingMY),
        'predO1':list(trainYPOther1),
        'predO2':list(trainYPOther2),
        'other':list(trainingOY),
        'mape':list(np.abs((trainYP - trainY) / trainY)*100),
        'mapeM':list(np.abs((trainYPMedia2 - trainingMY) / trainingMY)*100),
        'mapeO':list(np.abs((trainYPOther2 - trainingOY) / trainingOY)*100)
    }).to_csv(os.path.join(docDirname, 'train.csv'))

    trainMape = mapeFunc(trainY,trainYP)

    trainMediaMape = mapeFunc(trainingMY,trainYPMedia2)
    trainOtherMape = mapeFunc(trainingOY,trainYPOther2)

    retStr += 'train mape:%.2f%%\n'%(trainMape)
    retStr += 'train media mape:%.2f%%\n'%(trainMediaMape)
    retStr += 'train other mape:%.2f%%\n'%(trainOtherMape)

    plt.title("train")
    plt.plot(trainY,'b-',label='true')
    plt.plot(trainYP,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'train.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("media train")
    plt.plot(trainingMY,'b-',label='true')
    plt.plot(trainYPMedia2,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'trainMeida.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("train other")
    plt.plot(trainingOY,'b-',label='true')
    plt.plot(trainYPOther2,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'trainOther.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()
    
    # mod针对测试集表现
    testYP = mod.predict(testX).reshape(-1)

    testYPMedia1 = mediaMod1.predict(testX).reshape(-1)
    testYPMedia2 = mediaMod2.predict(testX).reshape(-1)
    testYPOther1 = otherMod1.predict(testX).reshape(-1)
    testYPOther2 = otherMod2.predict(testX).reshape(-1)

    yp = (testYP.reshape(-1))
    pd.DataFrame(data={
        'true':list(testY),
        'pred':list(testYP),
        'predM1':list(testYPMedia1),
        'predM2':list(testYPMedia2),
        'media':list(testingMY),
        'predO1':list(testYPOther1),
        'predO2':list(testYPOther2),
        'other':list(testingOY),
        'mape':list(np.abs((testYP - testY) / testY)*100),
        'mapeM':list(np.abs((testYPMedia2 - testingMY) / testingMY)*100),
        'mapeO':list(np.abs((testYPOther2 - testingOY) / testingOY)*100)
    }).to_csv(os.path.join(docDirname, 'test.csv'))

    testMape = mapeFunc(testY,yp)
    testMediaMape = mapeFunc(testingMY,testYPMedia2)
    testOtherMape = mapeFunc(testingOY,testYPOther2)

    retStr += 'test mape:%.2f%%\n'%(testMape)
    retStr += 'test media mape:%.2f%%\n'%(testMediaMape)
    retStr += 'test other mape:%.2f%%\n'%(testOtherMape)

    plt.title("test")
    plt.plot(testY,'b-',label='true')
    plt.plot(yp,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'test.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("media test")
    plt.plot(testingMY,'b-',label='true')
    plt.plot(testYPMedia2,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'testMeida.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    plt.title("test other")
    plt.plot(testingOY,'b-',label='true')
    plt.plot(testYPOther2,'r-',label='pred')
    plt.legend()
    filename = os.path.join(docDirname, 'testOther.png')
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
        afDf = getDataFromAF()
        afDf.to_csv(getFilename('afDataR7_20221001_20230201'))
        afDf = pd.read_csv(getFilename('afDataR7_20221001_20230201'))
        afDf = addMediaGroup(afDf)
        afDf = dataFill(afDf)
        afDf.to_csv(getFilename('afDataR7C_20221001_20230201'))

    afDf = pd.read_csv(getFilename('afDataR7C_20221001_20230201'))

    train(afDf,'test9 android debug switch m&o')
    
