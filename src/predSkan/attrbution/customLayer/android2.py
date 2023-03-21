# 添加一个媒体，snapchat

import datetime
import pandas as pd

import os
import sys
import numpy as np
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import afCvMapDataFrame
from src.predSkan.attrbution.customLayer.createMod import createModEasy05_l2
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
    {'name':'snapchat','codeList':['snapchat_int'],'sname':'Sc'},
    {'name':'unknown','codeList':[],'sname':'Og'}
]

# def addMediaGroup(df):
#     # 所有不属于mediaList的都算是unknown，和自然量一起处理
#     df.insert(df.shape[1],'media_group','unknown')
#     for media in mediaList:
#         name = media['name']
#         for code in media['codeList']:
#             df.loc[df.media == code,'media_group'] = name
#     return df

def addMediaGroup(df):
    # Initialize the media_group column with default value 'unknown'
    df['media_group'] = 'unknown'

    # Iterate through the mediaList and update the media_group column accordingly
    for group in mediaList:
        df.loc[df['media'].isin(group['codeList']), 'media_group'] = group['name']

    return df

def getDataFromAF():
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

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
                    and day >= 20220501
                    and day <= 20230301
                    and install_time >= '2022-05-01'
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


# 重做install_date_group
# 不再是7天汇总成一个值，而是每天都是前7天的均值
def step0():
    # def dataFill(dataDf3):
    #     # 每天补充满64组数据，没有的补0
    #     install_date_list = dataDf3['install_date'].unique()
    #     for install_date in install_date_list:
    #         # print(install_date)
    #         df = dataDf3.loc[(dataDf3.install_date == install_date)]
    #         dataNeedAppend = {
    #             'install_date':[],
    #             'cv':[],
    #             'media_group':[]
    #         }
    #         for i in range(64):
    #             for media in mediaList:
    #                 name = media['name']
    #                 if df.loc[
    #                     (df.media_group == name) &
    #                     (df.cv == i)
    #                 ]['count'].sum() == 0:
    #                     dataNeedAppend['install_date'].append(install_date)
    #                     dataNeedAppend['cv'].append(i)
    #                     dataNeedAppend['media_group'].append(name)

    #         dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    #     dataDf3 = dataDf3.sort_values(by=['install_date','cv','media_group']).reset_index(drop=True)
    #     dataDf3 = dataDf3.fillna(0)
    #     return dataDf3

    def dataFill(df):
        # Get unique values of 'install_date' and 'media_group'
        install_dates = df['install_date'].unique()
        media_groups = df['media_group'].unique()

        # Create a new DataFrame with all possible combinations of 'install_date', 'cv', and 'media_group'
        new_df = pd.DataFrame(columns=['install_date', 'cv', 'media_group'])
        for install_date in install_dates:
            for media_group in media_groups:
                for cv in range(64):
                    new_df = new_df.append({'install_date': install_date, 'cv': cv, 'media_group': media_group}, ignore_index=True)

        # Merge the original DataFrame with the new DataFrame, filling missing values with 0
        merged_df = pd.merge(new_df, df, on=['install_date', 'cv', 'media_group'], how='left').fillna(0)

        return merged_df
    # afDf = getDataFromAF()
    # afDf.to_csv(getFilename('androidAfR7_20220501_20230201'))

    afDf = pd.read_csv(getFilename('androidAfR7_20220501_20230201'))
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]
    
    afDf3 = addMediaGroup(afDf)
    afDf3 = dataFill(afDf3)
    if __debug__:
        # print(afDf3.columns)
        order = ['install_date','media_group','cv','count','sumr1usd','sumr7usd','media']
        afDf3 = afDf3[order]
        
        afDf3.to_csv(getFilename('a_step0_afDf3'))

    afDf4 = afDf3.groupby(by=['install_date','media_group','cv'],as_index=False).agg({
        'count':'sum',
        'sumr1usd':'sum',
        'sumr7usd':'sum',
    }).sort_values(by=['media_group','cv','install_date'],ignore_index=True)

    if __debug__:
        afDf4.to_csv(getFilename('a_step0_afDf4'))

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
    afDf4 = afDf4.loc[afDf4.install_date_group > '2022-05-06'].reset_index(drop=True)

    if __debug__:
        afDf4.to_csv(getFilename('a_step0_afDf4_rolling7'))
    
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
    
    afDf4 = pd.read_csv(getFilename('a_step0_afDf4_rolling7'))

    afUsdDf = addUSD(afDf4)
    afUsdSumDf = afUsdDf.groupby(by = ['install_date_group','media_group'],as_index=False).agg({
        'usd':'sum',
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    })

    afUsdSumDf.loc[afUsdSumDf.usd < 0.1,'usd'] = 0
    afUsdSumDf.loc[afUsdSumDf.sumr1usd < 0.1,'sumr1usd'] = 0
    afUsdSumDf.loc[afUsdSumDf.sumr7usd < 0.1,'sumr7usd'] = 0

    if __debug__:
        afUsdSumDf.to_csv(getFilename('a_step3_SumDf'))


# 获得总收入，作为y
def step4():
    afDf = pd.read_csv(getFilename('a_step0_afDf4_rolling7'))
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]

    afSumDf = afDf.groupby(by = ['install_date_group'],as_index=False).agg({
        'sumr7usd':'sum'
    })

    if __debug__:
        afSumDf.to_csv(getFilename('a_step4_SumDf'))
    return afSumDf

# 将不同媒体的64标准化
def step5():
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

    afDf = pd.read_csv(getFilename('a_step0_afDf4_rolling7'))
    afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]

    for media in mediaList:
        mediaName = media['name']
        mediaDf = afDf.loc[afDf.media_group == mediaName]
        mediaDf = mediaDf.loc[mediaDf.install_date_group < '2023-02-01']
        mediaDf = mediaDf.sort_values(by=['install_date_group','cv']).reset_index(drop=True)
        if __debug__:
            mediaDf.to_csv(getFilename('a_%s_step5'%(mediaName)))
        mediaNp64 = getTrainX(mediaDf)
        # 按顺序加入
        np64List.append(mediaNp64)
    
    return np64List

# 将训练数据拼起来
# 部分数据读取自之前步骤的数据文件
def step6(np64List):
    np1List = []
    a_step3_SumDf = pd.read_csv(getFilename('a_step3_SumDf'))
    for media in mediaList:
        mediaName = media['name']

        mediaSumDf = a_step3_SumDf.loc[a_step3_SumDf.media_group == mediaName]
        mediaSumDf = mediaSumDf.loc[mediaSumDf.install_date_group < '2023-02-01']
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
        np64List[3],np1List[3],
        np64List[4],np1List[4],
        )
    ,axis=1)
    
    # print(x.shape)

    return x


from tensorflow import keras
epochMax = 200
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
    for _ in range(100):
        filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
        mod = createModEasy05_l2(10000)

        modPath = '/src/src/predSkan/androidTotal/mod/%s%s/'%(name,filenameSuffix)
        checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.3f}-{val_loss:.3f}.h5')
    
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=False,
            monitor='val_loss',
            mode='min',
            save_best_only=True
        )

        trainingX = x[::2]
        trainingY = y[::2]
        testingX = x[1::2]
        testingY = y[1::2]

        print(trainingX.shape,trainingY.shape)
        print(testingX.shape,testingY.shape)

        history = mod.fit(trainingX, trainingY, epochs=epochMax, validation_data=(testingX,testingY)
        # history = mod.fit(x, y, epochs=epochMax, validation_split = .4
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
        val_loss = createDoc(modPath,x,y,history,docDirname,message)

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
def createDoc(modPath,x,y,history,docDirname,message):
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
    
    # bdMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-0').output)
    # fbMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-1').output)
    # ggMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-2').output)
    # ogMod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-3').output)
    
    # bdMod2 = keras.models.Model(mod.input,mod.get_layer('muti-0').output)
    # fbMod2 = keras.models.Model(mod.input,mod.get_layer('muti-1').output)
    # ggMod2 = keras.models.Model(mod.input,mod.get_layer('muti-2').output)
    # ogMod2 = keras.models.Model(mod.input,mod.get_layer('muti-3').output)

    retStr = '%s\n'%message
    retStr += '%s\n'%bestMod['path']
    # mod针对训练集表现
    
    trainX = x
    trainY = y

    trainY = trainY.reshape(-1)
    trainYP = mod.predict(trainX).reshape(-1)

    # trainYPBd1 = bdMod1.predict(trainX).reshape(-1)
    # trainYPFb1 = fbMod1.predict(trainX).reshape(-1)
    # trainYPGg1 = ggMod1.predict(trainX).reshape(-1)
    # trainYPOg1 = ogMod1.predict(trainX).reshape(-1)

    # trainYPBd2 = bdMod2.predict(trainX).reshape(-1)
    # trainYPFb2 = fbMod2.predict(trainX).reshape(-1)
    # trainYPGg2 = ggMod2.predict(trainX).reshape(-1)
    # trainYPOg2 = ogMod2.predict(trainX).reshape(-1)
    

    trainDf = pd.DataFrame(data={
        'true':list(trainY),
        'pred':list(trainYP),
        # 'predBd1':list(trainYPBd1),
        # 'predFb1':list(trainYPFb1),
        # 'predGg1':list(trainYPGg1),
        # 'predOg1':list(trainYPOg1),
        # 'predBd2':list(trainYPBd2),
        # 'predFb2':list(trainYPFb2),
        # 'predGg2':list(trainYPGg2),
        # 'predOg2':list(trainYPOg2),
        'mape':list(np.abs((trainYP - trainY) / trainY)*100)
    })
    
    for i in range(len(mediaList)):
        sname = mediaList[i]['sname']
        mod1 = keras.models.Model(mod.input,mod.get_layer('AddOne-%d'%i).output)
        mod2 = keras.models.Model(mod.input,mod.get_layer('muti-%d'%i).output)

        trainYP1 = mod1.predict(trainX).reshape(-1)
        trainYP2 = mod2.predict(trainX).reshape(-1)

        trainDf.loc[:,'pred%s1'%sname] = list(trainYP1)
        trainDf.loc[:,'pred%s2'%sname] = list(trainYP2)

    trainDf.to_csv(os.path.join(docDirname, 'train.csv'))

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
    for i in range(len(mediaList)):
        sname = mediaList[i]['sname']
        name = mediaList[i]['name']
        trainDf['pred%s1'%sname].plot(label=name)
        
    # plt.plot(trainYPBd1,label='bd')
    # plt.plot(trainYPFb1,label='fb')
    # plt.plot(trainYPGg1,label='gg')
    # plt.plot(trainYPOg1,label='og')
    plt.legend()
    filename = os.path.join(docDirname, 'trainR7R1.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    # a_step3_SumDf = pd.read_csv(getFilename('a_step3_SumDf'))



    testMape = mapeFunc(trainY,trainYP)

    for i in range(len(mediaList)):
        sname = mediaList[i]['sname']
        if np.all(trainDf['pred%s1'%sname].to_numpy() == 1.0):
            testMape += 100

    # if np.all(trainYPBd1 == 1.0) or \
    #     np.all(trainYPFb1 == 1.0) or \
    #     np.all(trainYPGg1 == 1.0) or \
    #     np.all(trainYPOg1 == 1.0):
    #     testMape += 100


    retStr += 'test mape:%.2f%%\n'%(testMape)

    # TODO:分媒体的倍率在这里
    # 要计算分媒体的倍率 MAPE
    # 相关性


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

    report(docDirname)
    retStr += report2(docDirname)

    logFilename = os.path.join(docDirname, 'log.txt')
    with open(logFilename, 'w') as f:
        f.write(retStr)

    return testMape

def cleanDf(df):
    df = df.loc[:,~df.columns.str.match('Unnamed')]
    return df

def getAdCost():
    sql = '''
        select 
            sum(cost) as cost,
            media_source as media,
            to_char(to_date(day,"yyyymmdd"),"yyyy-mm-dd") as install_date
        from ods_platform_appsflyer_masters
        where 
            app_id = 'com.topwar.gp'
            and day >= '20220501'
            and day < '20230201'
        group by
            media_source,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df



def report(docDirname):
    trainDf = pd.read_csv(os.path.join(docDirname, 'train.csv'))
    trainDf = trainDf.loc[:,~trainDf.columns.str.match('Unnamed')]
    # testDf = pd.read_csv(os.path.join(docDirname, 'test.csv'))
    # testDf = testDf.loc[:,~testDf.columns.str.match('Unnamed')]
    # totalDf = trainDf.append(testDf)
    # totalDf = totalDf.reset_index(drop=True)
    totalDf = trainDf

    # print(len(trainDf),len(testDf),len(totalDf))
    # 添加安装日期
    install_date_group = pd.read_csv(getFilename('a_step4_SumDf'))['install_date_group']
    totalDf.loc[:,'install_date_group'] = install_date_group
    # print(totalDf)
    # totalDf.to_csv(getFilename('reportTotalDf'))
    # 添加广告花费
    # adCostDf = getAdCost()
    # adCostDf.to_csv(getFilename('a_adCost2'))

    adCostDf = pd.read_csv(getFilename('a_adCost2'))
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

    adCostDf4 = adCostDf4.loc[(adCostDf4.install_date_group > '2022-05-06') & (adCostDf4.install_date_group < '2023-02-01')].reset_index(drop=True)
    adCostDf4.to_csv(os.path.join(docDirname,'a_reportAdCost.csv'))

    # 做一些简单修正
    # 1、计算各媒体ROI、总ROI
    totalDf.loc[:,'cost'] = 0
    totalDfOrder = ['install_date_group']

    a_step3_SumDf = pd.read_csv(getFilename('a_step3_SumDf'))

    for media in mediaList:
        name = media['name']
        if name != 'unknown':
            totalDf.loc[:,'%s cost'%(media['name'])] = adCostDf4.loc[adCostDf4.media_group == name].reset_index(drop=True)['cost']
            totalDf.loc[:,'%s roi'%(media['name'])] = totalDf['pred%s2'%(media['sname'])]/totalDf['%s cost'%(media['name'])]
            totalDf.loc[:,'cost'] += totalDf['%s cost'%(media['name'])]
            totalDf.loc[:,'true_roi'] = totalDf['true']/totalDf['cost']
            totalDf.loc[:,'pred_roi'] = totalDf['pred']/totalDf['cost']
            
            totalDf = totalDf.rename(columns={
                '%s roi'%(media['name']) : '%s roi7(pred)'%(media['name'])
            })

            totalDfOrder.append('%s cost'%(media['name']))
            totalDfOrder.append('%s roi7(pred)'%(media['name']))
            
        totalDf = totalDf.rename(columns={
                'pred%s1'%(media['sname']) : '%s r7/r1(pred)'%(media['name']),
                'pred%s2'%(media['sname']) : '%s revenue7(pred)'%(media['name'])
        })

        totalDfOrder.append('%s r7/r1(pred)'%(media['name']))
        totalDfOrder.append('%s revenue7(pred)'%(media['name']))
        
        mediaDf = a_step3_SumDf.loc[a_step3_SumDf.media_group == name].reset_index(drop=True)
        totalDf.loc[:,'%s revenue1(real)'%(media['name'])] = mediaDf['sumr1usd']
        totalDf.loc[:,'%s revenue7(real)'%(media['name'])] = mediaDf['sumr7usd']
        totalDf.loc[:,'%s r7/r1(real)'%(media['name'])] = mediaDf['sumr7usd']/mediaDf['sumr1usd']

        totalDf.loc[:,'%s r7 MAPE'%(media['name'])] = (totalDf['%s revenue7(real)'%(media['name'])] - totalDf['%s revenue7(pred)'%(media['name'])])/totalDf['%s revenue7(real)'%(media['name'])]
        totalDf.loc[totalDf['%s r7 MAPE'%(media['name'])]<0,'%s r7 MAPE'%(media['name'])] *= -1

        totalDf.loc[:,'%s r7/r1 MAPE'%(media['name'])] = (totalDf['%s r7/r1(real)'%(media['name'])] - totalDf['%s r7/r1(pred)'%(media['name'])])/totalDf['%s r7/r1(real)'%(media['name'])]
        totalDf.loc[totalDf['%s r7/r1 MAPE'%(media['name'])]<0,'%s r7/r1 MAPE'%(media['name'])] *= -1

        totalDfOrder.append('%s revenue1(real)'%(media['name']))
        totalDfOrder.append('%s revenue7(real)'%(media['name']))
        totalDfOrder.append('%s r7/r1(real)'%(media['name']))
        totalDfOrder.append('%s r7 MAPE'%(media['name']))
        totalDfOrder.append('%s r7/r1 MAPE'%(media['name']))

        # tmpOrder = [
        #     '%s revenue1(real)'%(media['name']),
        #     '%s revenue7(real)'%(media['name']),
        #     '%s revenue7(pred)'%(media['name']),
        #     '%s r7/r1(real)'%(media['name']),
        #     '%s r7/r1(pred)'%(media['name'])
        # ]
        # print(totalDf[tmpOrder].corr())
        # print(name,'r7 MAPE:\t%.2f%%'%(totalDf['%s r7 MAPE'%(media['name'])].mean()*100))
        # print(name,'r7/r1 MAPE:\t%.2f%%'%(totalDf['%s r7/r1 MAPE'%(media['name'])].mean()*100))

    totalDf = totalDf.rename(columns={
        'true':'revenue7',
        'pred':'revenue7(pred)',
        'true_roi':'roi7',
        'pred_roi':'roi7(pred)'
    })
    totalDfOrder.append('revenue7')
    totalDfOrder.append('revenue7(pred)')
    totalDfOrder.append('cost')
    totalDfOrder.append('roi7')
    totalDfOrder.append('roi7(pred)')
    totalDfOrder.append('mape')
    # print(totalDf.columns)
    totalDf = totalDf[totalDfOrder]
    totalDf.to_csv(os.path.join(docDirname,'reportTotalDf.csv'))

    # 计算ROI
    plt.title("7d ROI")
    plt.figure(figsize=(10.8, 3.2))

    totalDf.set_index(["install_date_group"], inplace=True)
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue

        totalDf['%s roi7(pred)'%(media['name'])].plot(label = name)
    
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig(os.path.join(docDirname,'roi.png'))
    plt.clf()
        

def report2(docDirname):
    totalDf = pd.read_csv(os.path.join(docDirname,'reportTotalDf.csv'))
    totalDf.set_index(["install_date_group"], inplace=True)

    retStr = '\n'

    for media in mediaList:
        name = media['name']
        retStr += '%s MAPE:\t%.2f%%\n'%(name,totalDf['%s r7 MAPE'%(media['name'])].mean()*100)

        # 计算ROI
        plt.title("%s revenue"%name)
        plt.figure(figsize=(10.8, 3.2))
    
        totalDf['%s revenue7(pred)'%(name)].plot(label = '%s revenue7(pred)'%(name))
        totalDf['%s revenue7(real)'%(name)].plot(label = '%s revenue7(real)'%(name))
        totalDf['revenue7'].plot(label = 'total revenue7(real)')
        totalDf['revenue7(pred)'].plot(label = 'total revenue7(pred)')
        
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.tight_layout()
        plt.savefig(os.path.join(docDirname,'%s.png'%name))
        plt.clf()

    return retStr

def main():
    step0()
    
    # step3()
    # step4()
    # np64List = step5()
    # x = step6(np64List)
    # y = pd.read_csv(getFilename('a_step4_SumDf'))['sumr7usd'].to_numpy().reshape(-1,1)

    # np.save('/src/data/customLayer/a5_xR7.npy',x)
    # np.save('/src/data/customLayer/a5_yR7.npy',y)

    # x = np.load('/src/data/customLayer/a5_xR7.npy')
    # y = np.load('/src/data/customLayer/a5_yR7.npy')

    # print(x.shape,y.shape)

    # train(x,y,'android 5 ::2 1622')

def report3(docDirname):
    totalDf = pd.read_csv(os.path.join(docDirname,'reportTotalDf.csv'))

    for media in mediaList:
        name = media['name']        
        r7 = totalDf['%s revenue7(real)'%(name)]
        r7p = totalDf['%s revenue7(real)'%(name)]/totalDf['revenue7']
        
        print('%10s r7 var:\t\t%.0f'%(name,r7.var()))
        print('%10s r7p var:\t\t%f'%(name,r7p.var()))


if __name__ == '__main__':
    main()
    # report('/src/data/doc/customLayer//iOSCustom_20230316_082543')
    # report2('/src/data/doc/customLayer//iOSCustom_20230316_082543')
    # report('/src/data/doc/customLayer//iOSCustom_20230316_060009')
    # report2('/src/data/doc/customLayer//iOSCustom_20230316_060009')
    # report3('/src/data/doc/customLayer//iOSCustom_20230316_082543')