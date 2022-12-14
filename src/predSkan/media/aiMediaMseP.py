# 不再用cv count做input
# 用cv count的百分比来做input，这样更符合最终预测比率，至少直觉上是这样的
# 然后再尝试针对先有个input进行标准化
import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.tools import afCvMapDataFrame
from src.maxCompute import execSql
from src.tools import getFilename
from src.predSkan.tools.ai import purgeRetCsv,logUpdate,createDoc,filterByMediaName

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

# 为df添加media cv
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    # {'name':'applovin','codeList':['applovin_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    # {'name':'unity','codeList':['unityads_int']},
    # {'name':'apple','codeList':['Apple Search Ads']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
    # {'name':'snapchat','codeList':['snapchat_int']},
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
            # layers.Dropout(0.3),
            layers.Dense(128, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            # layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    mod.compile(optimizer='RMSprop',loss='mse')
    mod.summary()
    return mod


def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(64,kernel_initializer='random_normal',bias_initializer='random_normal', activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            # layers.Dense(512, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
            # layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
        ]
    )
    # mod.compile(optimizer='adadelta',loss='mape')
    mod.compile(optimizer='adadelta',loss='mape')
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

# 获得X的最大值和最小值
# def getXMinAndMax(df,mediaName):
#     trainDf = df.loc[
#         (df.media_group == mediaName)
#     ].sort_values(by=['install_date','cv'])
#     trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
#     trainX = trainDf['count'].to_numpy().reshape((-1,64))
#     trainXSum = trainX.sum(axis=1).reshape(-1,1)
#     trainX = trainX/trainXSum

#     min = trainX.T.min(axis=1)
#     max = trainX.T.max(axis=1)
#     print(min.shape,max.shape)
#     return min,max

# def getTrainingData(df,mediaName,sinceTimeStr,unitlTimeStr,min,max):
#     trainDf = df.loc[
#         (df.install_date >= sinceTimeStr) & (df.install_date < unitlTimeStr) & (df.media_group == mediaName)
#     ].sort_values(by=['install_date','cv'])
#     trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
#     trainX = trainDf['count'].to_numpy().reshape((-1,64))
#     trainXSum = trainX.sum(axis=1).reshape(-1,1)
#     trainX = trainX/trainXSum
#     trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
#     trainY0 = trainSumByDay['sumr7usd'].to_numpy()
#     trainY1 = trainSumByDay['sumr1usd'].to_numpy()
#     # 这里为了解决部分0数据
#     trainY0[trainY0 <= 0] = 1
#     trainY1[trainY1 <= 0] = 1
#     trainY = trainY0/trainY1 - 1

#     # # 尝试标准化
#     # mean = np.mean(trainX,axis=0)
#     # std = np.std(trainX,axis=0)
#     # std[std == 0 ] = 1
#     # print(mean)
#     # print(std)
#     # trainXSs = (trainX - mean)/std

#     trainXSs = np.nan_to_num((trainX-min)/(max-min))
    

#     return trainXSs,trainY, trainY0, trainY1

# def getTestingData(df,mediaName,sinceTimeStr,unitlTimeStr,min,max):
#     trainDf = df.loc[
#         (df.install_date >= sinceTimeStr) & (df.install_date < unitlTimeStr) & (df.media_group == mediaName)
#     ].sort_values(by=['install_date','cv'])
#     trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
#     trainX = trainDf['count'].to_numpy().reshape((-1,64))
#     trainXSum = trainX.sum(axis=1).reshape(-1,1)
#     trainX = trainX/trainXSum
#     trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
#     trainY0 = trainSumByDay['sumr7usd'].to_numpy()
#     trainY1 = trainSumByDay['sumr1usd'].to_numpy()
#     # 这里为了解决部分0数据
#     trainY0[trainY0 <= 0] = 1
#     trainY1[trainY1 <= 0] = 1
#     trainY = trainY0/trainY1 - 1

#     # 尝试标准化
#     # trainXSs = (trainX - mean)/std
#     trainXSs = np.nan_to_num((trainX-min)/(max-min))

#     return trainXSs,trainY, trainY0, trainY1


def train(dataDf3,message):
    global lossAndErrorPrintingCallbackSuffixStr
    
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta = .1,patience=300)
    for _ in range(5):
        for media in mediaList:
            name = media['name']
            # min,max = getXMinAndMax(dataDf3,name)
            # print(name,min,max)
            # 各种命名都用这个后缀，防止重名
            filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
            # 每次都重新建立mod
            mod = createModFunc1()

            modPath = '/src/src/predSkan/media/mod/%s/'%filenameSuffix
            checkpoint_filepath = os.path.join(modPath,'mod_{epoch:05d}-{loss:.2f}-{val_loss:.2f}.h5')
        
            model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_filepath,
                save_weights_only=False,
                monitor='val_loss',
                mode='min',
                save_best_only=True
            )

            lossAndErrorPrintingCallbackSuffixStr = name

            # trainX,trainY,trainY0,trainY1 = getTrainingData(dataDf3,name,'2022-05-01','2022-07-30',min,max)
            # testX, testY, testY0, testY1 = getTestingData(dataDf3,name,'2022-08-01','2022-09-01',min,max)
            trainX, trainY, testX, testY, trainY0, testY0, trainY1, testY1 = filterByMediaName(dataDf3,name)
            
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
            logDir = '/src/data/doc/media/%s'%(name)
            os.makedirs(logDir,exist_ok=True)
            # 将每次的明细结果放进去，
            docDirname = '/src/data/doc/media/%s/%s'%(name,name+filenameSuffix)
            val_loss = createDoc(modPath,trainX, trainY0,trainY1,testX,testY0, testY1,history,docDirname,message)
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
    # df = pd.read_csv(getFilename('media_20220501_20220930'))
    # df1 = addMediaGroup(df)
    # df2 = dataFill(df1)
    # df2.to_csv(getFilename('mediaIdfa_20220501_20220930'))
    df2 = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    train(df2,'mse media PPP5')
