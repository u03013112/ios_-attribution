# 尝试按照idfa进行分媒体预测
# 首先用idfa数据进行训练和测试
# 将效果比较好的模型保存
# 获得skan的媒体数据，包括自然量
# 用模型对此进行预测，并于总量做比对

# 可能可以将小媒体划入自然量一起计算，主要针对大媒体进行拆分和预测
# 这里可以效仿geo，现将所media都做出来，然后再分组
import datetime
import pandas as pd
import numpy as np
import sys
sys.path.append('/src')

from src.tools import afCvMapDataFrame
from src.maxCompute import execSql
from src.tools import getFilename
# fun1，获得媒体数据
# 按install date，media进行汇总，cv、count、sunR7Usd
# 在需求统计媒体以外的部分算作自然量，由于不是来自一个表，这个自然量会有较大的误差

# fun2，获得idfa的媒体数据
# 与上面的差异是这些都是有明确idfa的，包括自然量
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
    {'name':'applovin','codeList':['applovin_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'unity','codeList':['unityads_int']},
    {'name':'apple','codeList':['Apple Search Ads']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
    {'name':'snapchat','codeList':['snapchat_int']},
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

def addCvUsd(dataDf3):
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    dataDf3.insert(dataDf3.shape[1],'cv_usd',0)
    for i in range(64):
        min_event_revenue = afCvMapDataFrame.iloc[i].at['min_event_revenue']
        max_event_revenue = afCvMapDataFrame.iloc[i].at['max_event_revenue']
        if pd.isna(max_event_revenue):
            avg = 0
        else:
            avg = (min_event_revenue + max_event_revenue)/2

        count = dataDf3.loc[dataDf3.cv==i,'count']
        dataDf3.loc[dataDf3.cv == i,'cv_usd'] = count * avg
    
    return dataDf3

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

epochMax = 3000

def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu", input_shape=(129,)),
            layers.Dropout(0.3),
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

def train(dataDf3):
    # n是向前取n天的数据进行预测
    n = 14
    data = {
        'media':[],
        'install_date':[],
        'pred':[]
    }
    for media in mediaList:
        name = media['name']
        sinceTimeStr = '2022-09-01'
        unitlTimeStr = '2022-09-30'
        sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y-%m-%d')
        unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y-%m-%d')
        
        for i in range((unitlTime - sinceTime).days + 1):
            day = sinceTime + datetime.timedelta(days=i)
            dayStr = day.strftime('%Y-%m-%d')
            # 由于需要7日数据，所以最近的满7日数据应该是T-6
            # n日数据需要从T-6-n+1开始，至T-6
            day0 = day - datetime.timedelta(days= n+6-1)
            day1 = day - datetime.timedelta(days= 6)
            day0Str = day0.strftime('%Y-%m-%d')
            day1Str = day1.strftime('%Y-%m-%d')
            print('针对%s的预测，训练数据取自：%s~%s'%(dayStr,day0Str,day1Str))

            trainDf = dataDf3.loc[
                (dataDf3.install_date >= day0Str) & (dataDf3.install_date <= day1Str) & (dataDf3.media_group == name)
            ].sort_values(by=['install_date','cv'])
            # print('1',trainDf)
            trainDf.to_csv(getFilename('google'))
            trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
            # print('2',trainDf)
            trainX = trainDf[['count','cv_usd']].to_numpy().reshape((-1,64*2))
            trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','cv_usd':'sum'})
            cv_usd = trainSumByDay['cv_usd'].to_numpy().reshape((-1,1))
            trainX = np.append(trainX,cv_usd,axis=1)

            trainSumByDay = dataAddEMA(trainSumByDay,3)
            trainY = trainSumByDay['ema'].to_numpy()

            # 训练3次，找到最好的mod
            bestMod = None
            bestLoss = 9e9
            for j in range(3):
                
                mod = createModFunc3()
                
                history = mod.fit(trainX, trainY, epochs=epochMax
                    ,batch_size=128
                    ,verbose=0
                )
                loss = history.history['loss'][-1]
                if loss < bestLoss:
                    bestLoss = loss
                    bestMod = mod
            
            testDf = dataDf3.loc[
                (dataDf3.install_date == dayStr) & (dataDf3.media_group == name)
            ].sort_values(by=['install_date','cv'])
            testDf = testDf.groupby(['install_date','cv']).agg('sum')
            # print(testDf)
            testX = testDf[['count','cv_usd']].to_numpy().reshape((-1,64*2))
            testSumByDay = testDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','cv_usd':'sum'})
            cv_usd = testSumByDay['cv_usd'].to_numpy().reshape((-1,1))
            testX = np.append(testX,cv_usd,axis=1)
            yTrue = testSumByDay['sumr7usd'].to_numpy().reshape(-1)[0]
            # print('test shape:',testX.shape)
            # print(testX)
            yPred = bestMod.predict(testX).reshape(-1)[0]
            print('%s%s loss:%.2f 预测结果：%.2f，真实结果：%.2f，mape=%.2f%%'%(name,dayStr,loss,yPred,yTrue,np.abs((yPred - yTrue) / yTrue)* 100))
            # if yPred < 10:
            #     print('retry')
            #     continue

            data['media'].append(name)
            data['install_date'].append(dayStr)
            data['pred'].append(yPred)
            
            tf.keras.backend.clear_session()
            del bestMod
            
            
    
    return pd.DataFrame(data = data)

if __name__ == '__main__':
    # df = getIdfaData('20220820','20220820')
    # df.to_csv(getFilename('media_20220820'))
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getIdfaData('20220501','20220930')
        df.to_csv(getFilename('media_20220501_20220930'))
        # df = pd.read_csv(getFilename('media20220501'))
        # print('123')
        df1 = addMediaGroup(df)
        df2 = dataFill(df1)
        df3 = addCvUsd(df2)
        df3.to_csv(getFilename('mediaIdfa_20220501_20220930'))

    df = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))

    retDf = train(df)
    retDf.to_csv(getFilename('totalMediaR3_20220701_20220930'))