# 与2P的区别是标准化的方式不同，2P是max与min，3P是trainXSs = (trainX - mean)/std
import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.tools import afCvMapDataFrame,cvToUSD2
from src.maxCompute import execSql
from src.tools import getFilename
from src.predSkan.tools.ai import purgeRetCsv,logUpdate,createDoc,mapeFunc,filterByMediaNameS,filterByMediaNameS2

import datetime

def getIdfaData(sinceTimeStr,unitlTimeStr):
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
    unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d") + ' 23:59:59'

    # 为了获得完整的7日回收，需要往后延长7天
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

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
            install_date,
            media
        from
            (
                select
                    customer_user_id,
                    case
                        when r1usd = 0
                        or r1usd is null then 0 % s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date,
                    media
                from
                    (
                        SELECT
                            t0.customer_user_id,
                            t0.install_date,
                            t1.r1usd,
                            t1.r7usd,
                            t0.media
                        FROM
                            (
                                select
                                    customer_user_id,
                                    install_date,
                                    media
                                from
                                    (
                                        select
                                            customer_user_id,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date,
                                            media_source as media
                                        from
                                            ods_platform_appsflyer_events
                                        where
                                            app_id = 'id1479198816'
                                            and idfa is not null
                                            and event_name = 'install'
                                            and zone = 0
                                            and day >= % s
                                            and day <= % s
                                            and install_time >= "%s"
                                            and install_time <= "%s"
                                        union
                                        all
                                        select
                                            customer_user_id,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date,
                                            media_source as media
                                        from
                                            tmp_ods_platform_appsflyer_origin_install_data
                                        where
                                            app_id = 'id1479198816'
                                            and idfa is not null
                                            and zone = '0'
                                            and install_time >= "%s"
                                            and install_time <= "%s"
                                    )
                                group by
                                    customer_user_id,
                                    install_date,
                                    media
                            ) as t0
                            LEFT JOIN (
                                select
                                    customer_user_id,
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
                                    ) as r7usd
                                from
                                    ods_platform_appsflyer_events
                                where
                                    app_id = 'id1479198816'
                                    and event_name = 'af_purchase'
                                    and zone = 0
                                    and day >= % s
                                    and day <= % s
                                    and install_time >= "%s"
                                    and install_time <= "%s"
                                group by
                                    install_date,
                                    customer_user_id
                            ) as t1 ON t0.customer_user_id = t1.customer_user_id
                    )
            )
        group by
            cv,
            install_date,
            media
    ;
    '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

    print(sql)
    pd_df = execSql(sql)
    return pd_df

def getSkanData(sinceTimeStr,unitlTimeStr):
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
    unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d")

    # 为了获得完整的7日回收，需要往后延长7天
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

    sql = '''
        select
            skad_conversion_value as cv,
            install_date,
            media_source as media,
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = "id1479198816"
            and event_name  in ('af_skad_redownload','af_skad_install')
            and day >= % s
            and day <= % s
            and install_date >= "%s"
            and install_date <= "%s"
        group by
            skad_conversion_value,
            install_date,
            media_source
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

    print(sql)
    pd_df = execSql(sql)
    return pd_df

# 为df添加media cv
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
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
            'sumr7usd':[],
            'media_group':[]
        }
        for i in range(64):
            for media in mediaList:
                name = media['name']
                # 之前为啥要看7日回收
                # if df.loc[(df.cv == i) & (df.media_group == name),'sumr7usd'].sum() == 0 \
                #     and df.loc[(df.cv == i) & (df.media_group == name),'count'].sum() == 0:
                if df.loc[(df.cv == i) & (df.media_group == name),'count'].sum() == 0:
                    # if name == 'google' and install_date == '2022-05-01':
                    #     print('fill:',name,i)
                    dataNeedAppend['install_date'].append(install_date)
                    dataNeedAppend['cv'].append(i)
                    dataNeedAppend['count'].append(0)
                    dataNeedAppend['sumr7usd'].append(0)
                    dataNeedAppend['media_group'].append(name)

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
    # mod.compile(optimizer='adadelta',loss='mse')
    # mod.summary()
    return mod

epochMax = 1000
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

def train(dataDf3,message):
    global lossAndErrorPrintingCallbackSuffixStr
    
    # earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta = .1,patience=300)
    for media in mediaList:
        name = media['name']
        logDir = '/src/data/doc/media2/%s'%(name)
        os.makedirs(logDir,exist_ok=True)
        for _ in range(5):
            # 各种命名都用这个后缀，防止重名
            filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')

            # mod1
            modPath = '/src/src/predSkan/media/mod/1/%s/'%filenameSuffix
            
            checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.2f}-{val_loss:.2f}.h5')
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = name

            # 每次都重新建立mod
            mod = createModFunc1()

            trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1, mean1, std1 = filterByMediaNameS(dataDf3,name)

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
            
            # 将每次的明细结果放进去，
            docDirname = '%s/1/%s'%(logDir,name+filenameSuffix)

            val_loss = createDoc(modPath,trainX, trainY0,trainY1,testX,testY0, testY1,history,docDirname,message)

            min1Filename = os.path.join(docDirname,'mean.npy')
            np.save(min1Filename, mean1)
            max1Filename = os.path.join(docDirname,'std.npy')
            np.save(max1Filename, std1)

            # 将结果写入到国家日志里
            # retCsvFilename 记录所有结果
            retCsvFilename = os.path.join(logDir,'ret1.csv')
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
                'message':[message],
                'f':['meanAndStd']
            }
            retDf = retDf.append(pd.DataFrame(data=logData))
            # 将本次的结果添加，然后重新写文件，这个方式有点丑，暂时这样。
            
            retDf.to_csv(retCsvFilename)
            print('save ret:',retCsvFilename)
            purgeRetCsv(retCsvFilename)
            logFilename = os.path.join(logDir,'log1.txt')
            logUpdate(retCsvFilename,logFilename,name)


            # mod2
            modPath = '/src/src/predSkan/media/mod/2/%s/'%filenameSuffix
            
            checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.2f}-{val_loss:.2f}.h5')
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = name + 'other'

            # 每次都重新建立mod
            mod = createModFunc1()
                    
            trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1, mean2, std2 = filterByMediaNameS2(dataDf3,name)

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

            # 将每次的明细结果放进去，
            docDirname = '%s/2/%s'%(logDir,name+filenameSuffix)
            val_loss = createDoc(modPath,trainX, trainY0,trainY1,testX,testY0, testY1,history,docDirname,message)

            min2Filename = os.path.join(docDirname,'mean.npy')
            np.save(min2Filename, mean2)
            max2Filename = os.path.join(docDirname,'std.npy')
            np.save(max2Filename, std2)

            # 将结果写入到国家日志里
            # retCsvFilename 记录所有结果
            retCsvFilename = os.path.join(logDir,'ret2.csv')
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
                'message':[message],
                'f':['meanAndStd']
            }
            retDf = retDf.append(pd.DataFrame(data=logData))
            # 将本次的结果添加，然后重新写文件，这个方式有点丑，暂时这样。
            
            retDf.to_csv(retCsvFilename)
            purgeRetCsv(retCsvFilename)
            logFilename = os.path.join(logDir,'log2.txt')
            logUpdate(retCsvFilename,logFilename,name)

        docDirname = '%s/3'%(logDir)
        os.makedirs(docDirname,exist_ok=True)
        mape,path1,path2 = createDocTotal(logDir,name)
        logData = {
            'path':['%s %s'%(path1,path2)],
            'val_loss':[mape],
            'message':[message]
        }
        # 将本次的结果添加，然后重新写文件，这个方式有点丑，暂时这样。
        
        retCsvFilename = os.path.join(logDir,'ret3.csv')
        if os.path.exists(retCsvFilename):
            retDf = pd.read_csv(retCsvFilename)
        else:
            retDf = pd.DataFrame(data = {
                'path':[],
                'val_loss':[],
                'message':[]
            })
        retDf = retDf.append(pd.DataFrame(data=logData))
        retDf.to_csv(retCsvFilename)
        purgeRetCsv(retCsvFilename)
        logFilename = os.path.join(logDir,'log3.txt')
        logUpdate(retCsvFilename,logFilename,name)


mediaSkanDf = pd.read_csv(getFilename('mediaSkan2_20220501_20220930'))
totalDf = pd.read_csv(getFilename('totalData_20220501_20220930'))

import matplotlib.pyplot as plt
# 尝试用idfa的数据去预测一下skan整体数据 
def createDocTotal(docDirname,mediaName):
    # 首先获得skan中的media数据
    global mediaSkanDf
    ret1Filename = os.path.join(docDirname, 'ret1.csv')
    ret1Df = pd.read_csv(ret1Filename)
    ret1Df = ret1Df.sort_values(by=['val_loss'])
    mod1Filename = os.path.join(ret1Df.iloc[0].at['path'], 'bestMod.h5')
    mod1 = tf.keras.models.load_model(mod1Filename)

    df = mediaSkanDf.loc[
        (mediaSkanDf.media_group == mediaName) & 
        (pd.isna(mediaSkanDf.cv)==False) &
        (mediaSkanDf.install_date >= '2022-08-01')
        ].sort_values(by=['install_date','cv'])
    x = df['count'].to_numpy().reshape((-1,64))
    xSum = x.sum(axis=1).reshape(-1,1)
    x = np.nan_to_num(x/xSum)

    f = ret1Df.iloc[0].at['f']
    if f == 'minAndMax':
        min1Filename = os.path.join(ret1Df.iloc[0].at['path'], 'min.npy')
        min1 = np.load(min1Filename)
        max1Filename = os.path.join(ret1Df.iloc[0].at['path'], 'max.npy')
        max1 = np.load(max1Filename)

        x = (x-min1)/(max1-min1)
        x[x == np.inf] = 0
        x[x == -np.inf] = 0
        x1 = np.nan_to_num(x)
    else:
        mean1Filename = os.path.join(ret1Df.iloc[0].at['path'], 'mean.npy')
        mean1 = np.load(mean1Filename)
        std1Filename = os.path.join(ret1Df.iloc[0].at['path'], 'std.npy')
        std1 = np.load(std1Filename)

        x = (x - mean1)/std1
        x[x == np.inf] = 0
        x[x == -np.inf] = 0
        x1 = np.nan_to_num(x)

    yP1 = mod1.predict(x1)
    # print(yP1)
    dfUsd = cvToUSD2(df)
    dfUsdSum = dfUsd.groupby('install_date').agg({'usd':'sum'})
    
    y1 = (yP1.reshape(-1) + 1)*dfUsdSum['usd'].to_numpy().reshape(-1)

    # 然后获得其他所有数据
    ret2Filename = os.path.join(docDirname, 'ret2.csv')
    ret2Df = pd.read_csv(ret2Filename)
    ret2Df = ret2Df.sort_values(by=['val_loss'])
    mod2Filename = os.path.join(ret2Df.iloc[0].at['path'], 'bestMod.h5')
    mod2 = tf.keras.models.load_model(mod2Filename)

    # 这里做简便处理
    global totalDf
    dfTotal = totalDf.loc[(totalDf.install_date >= '2022-08-01')].sort_values(by=['install_date','cv'])
    dfTotal['count'] = dfTotal['count'] - df['count'].to_numpy()
    
    # 直接用大盘的cv减去这个媒体skan中的cv，可能不太准确，但是先看看大致
    x = dfTotal['count'].to_numpy().reshape((-1,64))
    x[x<0]=0
    xSum = x.sum(axis=1).reshape(-1,1)
    x = x/xSum

    f = ret2Df.iloc[0].at['f']
    if f == 'minAndMax':
        min2Filename = os.path.join(ret2Df.iloc[0].at['path'], 'min.npy')
        min2 = np.load(min2Filename)
        max2Filename = os.path.join(ret2Df.iloc[0].at['path'], 'max.npy')
        max2 = np.load(max2Filename)

        x = (x-min2)/(max2-min2)
        x[x == np.inf] = 0
        x[x == -np.inf] = 0
        x2 = np.nan_to_num(x)
    else:
        mean2Filename = os.path.join(ret2Df.iloc[0].at['path'], 'mean.npy')
        mean2 = np.load(mean2Filename)
        std2Filename = os.path.join(ret2Df.iloc[0].at['path'], 'std.npy')
        std2 = np.load(std2Filename)

        x = (x - mean2)/std2
        x[x == np.inf] = 0
        x[x == -np.inf] = 0
        x2 = np.nan_to_num(x)
    yP2 = mod2.predict(x2)

    # print(yP2)
    dfUsd = cvToUSD2(dfTotal)
    dfUsdSum = dfUsd.groupby('install_date').agg({'usd':'sum'})
    
    y2 = (yP2.reshape(-1) + 1)*dfUsdSum['usd'].to_numpy().reshape(-1)

    y_pred = y1+y2
    dfTotalByDay = dfTotal.groupby('install_date').agg({'sumr7usd':'sum'})
    y_true = dfTotalByDay['sumr7usd'].to_numpy()
    
    mape = mapeFunc(y_true,y_pred)

    logDf1 = pd.DataFrame(data = {
        'yp1':list(yP1.reshape(-1)),
        'yp2':list(yP2.reshape(-1)),
        'y1':list(y1),
        'y2':list(y2),
        'y_pred':list(y_pred),
        'y_true':list(y_true),
        'mape':np.abs((y_pred - y_true) / y_true)*100,
        'mod1':ret1Df.iloc[0].at['path'],
        'mape1':ret1Df.iloc[0].at['val_loss'],
        'massge1':ret1Df.iloc[0].at['message'],
        'mod2':ret2Df.iloc[0].at['path'],
        'mape2':ret2Df.iloc[0].at['val_loss'],
        'massge2':ret2Df.iloc[0].at['message']
    })
    logFilename = os.path.join(docDirname, 'detail3.csv')
    logDf1.to_csv(logFilename)

    plt.title("total")
    plt.plot(y_true,'-',label='true')
    plt.plot(y_pred,'-',label='pred')
    plt.plot(y1,'-',label='pred1')
    plt.legend()
    filename = os.path.join(docDirname, 'total.png')
    plt.savefig(filename)
    print('save pic',filename)
    plt.clf()

    return mape,ret1Df.iloc[0].at['path'],ret2Df.iloc[0].at['path']
            
if __name__ == '__main__':
    # df = pd.read_csv(getFilename('media_20220501_20220930'))
    # df1 = addMediaGroup(df)
    # df2 = dataFill(df1)
    # df2.to_csv(getFilename('mediaIdfa_20220501_20220930'))
    df2 = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    train(df2,'3p test')
