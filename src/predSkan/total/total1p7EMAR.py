# 大盘整体预测程序
# 用于放到dataworks上
# 改为EMA数据来做预测
# 测试集仍用原始数据
# 与v2的区别在于添加新的输入，添加cv*金额后的值，添加首日支付总金额
import datetime
import pandas as pd

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import datetime
import numpy as np

dayStr = args['dayStr']

def execSql(sql):
    with o.execute_sql(sql).open_reader() as reader:
        pd_df = reader.to_pandas()
        return pd_df

sql = '''
    select 
        * 
    from ods_skan_cv_map
    where
        app_id="id1479198816"
;
'''
afCvMapDataFrame = execSql(sql)

groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]

def getTotalData(dayStr):
    day = datetime.datetime.strptime(dayStr,'%Y%m%d')
    # dayStr2 是%Y-%m-%d格式的
    # dayStr2 = day.strftime("%Y-%m-%d")
    
    # 由于这个算法需要一些旧数据来预测，所以往前取30天的数据
    sinceTime = day-datetime.timedelta(days=30)
    sinceTimeStr = sinceTime.strftime('%Y%m%d')
    sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")

    # 往后加7天也是保险起见
    untilTime = day+datetime.timedelta(days=7)
    unitlTimeStr = untilTime.strftime('%Y%m%d')
    unitlTimeStr2 = day.strftime("%Y-%m-%d") + ' 23:59:59'

    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        try:
            min_event_revenue = int(afCvMapDataFrame.min_event_revenue[i])
            max_event_revenue = int(afCvMapDataFrame.max_event_revenue[i])
        except :
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    sql='''
        select
            cv,
            count(*) as count,
            sum(r1usd) as sumR1usd,
            sum(r7usd) as sumR7usd,
            install_date
        from
            (
                select
                    customer_user_id,
                    case
                        when r1usd = 0
                        or r1usd is null then 0 
                        % s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date
                from
                    (
                        SELECT
                            t0.customer_user_id,
                            t0.install_date,
                            t1.r1usd,
                            t1.r7usd
                        FROM
                            (
                                select
                                    customer_user_id,
                                    install_date
                                from
                                    (
                                        select
                                            customer_user_id,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            ods_platform_appsflyer_events
                                        where
                                            app_id = 'id1479198816'
                                            and event_name = 'install'
                                            and zone = 0
                                            and day >= %s
                                            and day <= %s
                                            and install_time >= "%s"
                                            and install_time <= "%s"
                                        union
                                        all
                                        select
                                            customer_user_id,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            tmp_ods_platform_appsflyer_origin_install_data
                                        where
                                            app_id = 'id1479198816'
                                            and zone = '0'
                                            and install_time >= "%s"
                                            and install_time <= "%s"
                                    )
                                group by
                                    customer_user_id,
                                    install_date
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
                                    and day >= %s
                                    and day <= %s
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
            install_date
        ;
    '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    pd_df = execSql(sql)
    return pd_df

# 对每组数据分别整理
def dataStep2(dataDf1):
    dataDf1.insert(dataDf1.shape[1],'group',0)
    for i in range(len(groupList)):
        l = groupList[i]
        for cv in l:
            dataDf1.loc[dataDf1.cv == cv,'group'] = i
    return dataDf1

# 对数据做基础处理
def dataStep3(dataDf2):
    # print(dataDf2.sort_values(by=['install_date','group']))
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf2['install_date'].unique()
    for install_date in install_date_list:
        df = dataDf2.loc[(dataDf2.install_date == install_date)]
        for i in range(len(groupList)):
            if df.loc[df.group == i,'sumr7usd'].sum() == 0 and df.loc[df.group == i,'count'].sum() == 0:
                dataDf2 = dataDf2.append(pd.DataFrame(data={
                    'install_date':[install_date],
                    'count':[0],
                    'sumr7usd':[0],
                    'group':[i]
                }),ignore_index=True)
                # print('补充：',install_date,i)
    # print(dataDf2.sort_values(by=['install_date','group']))
    return dataDf2

def dataStep4(dataDf3):
    global afCvMapDataFrame
    dataDf3.insert(dataDf3.shape[1],'cv_usd',0)
    for i in range(64):
        min_event_revenue = afCvMapDataFrame.iloc[i].at['min_event_revenue']
        max_event_revenue = afCvMapDataFrame.iloc[i].at['max_event_revenue']
        if pd.isna(max_event_revenue):
            avg = 0
        else:
            avg = (min_event_revenue + max_event_revenue)/2

        count = dataDf3.loc[dataDf3.group==i,'count']
        dataDf3.loc[dataDf3.group == i,'cv_usd'] = count * avg
    
    return dataDf3
    


# 尝试对数据进行EMA计算，这个要在
def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512, activation="relu", input_shape=(129,)),
            layers.Dropout(0.3),
            layers.Dense(512, activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

epochMax = 1000

def predict(dayStr='20220901'):
    # n是向前取n天的数据进行预测
    n = 7

    df = getTotalData(dayStr)
    df2 = dataStep2(df)
    df3 = dataStep3(df2)
    dataDf3 = dataStep4(df3)
    
    day = datetime.datetime.strptime(dayStr,'%Y%m%d')
    day0 = day - datetime.timedelta(days= n+6-1)
    day1 = day - datetime.timedelta(days= 6)
    day0Str = day0.strftime('%Y-%m-%d')
    day1Str = day1.strftime('%Y-%m-%d')
    print('针对%s的预测，训练数据取自：%s~%s'%(dayStr,day0Str,day1Str))
        
    trainDf = dataDf3.loc[
        (dataDf3.install_date >= day0Str) & (dataDf3.install_date <= day1Str)
    ].sort_values(by=['install_date','group'])
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
            (dataDf3.install_date == dayStr)
        ].sort_values(by=['install_date','group'])
        testX = testDf[['count','cv_usd']].to_numpy().reshape((-1,64*2))
        testSumByDay = testDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','cv_usd':'sum'})
        cv_usd = testSumByDay['cv_usd'].to_numpy().reshape((-1,1))
        testX = np.append(testX,cv_usd,axis=1)
        
        yPred = bestMod.predict(testX).reshape(-1)[0]
        return yPred
        
from odps.models import Schema, Column, Partition
def createTable():
    columns = [
        Column(name='p7usd', type='double', comment='predict d7 revenue in usd')
    ]
    partitions = [
        Partition(name='install_date', type='string', comment='like 20221018')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('topwar_iosglobal_total1p7_v2', schema, if_not_exists=True)
    return table

# import pyarrow as pa
def writeTable(df,dayStr):
    t = o.get_table('topwar_iosglobal_total1p7_v2')
    t.delete_partition('install_date=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='install_date=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        writer.write(df)

createTable()
yp = predict(dayStr=dayStr)
usd = yp[0][0]
print ('pred %s usd:%.2f'%(dayStr,usd))
df = pd.DataFrame(
    data={
        'install_date':[dayStr],
        'p7usd':[usd]
    }
)
print (df)
writeTable(df,dayStr)
