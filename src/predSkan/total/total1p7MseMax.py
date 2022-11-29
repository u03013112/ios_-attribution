# 大盘
# 削弱大R
# 用于放到dataworks上
import datetime
import pandas as pd
import numpy as np

import tensorflow as tf

import zipfile

##@resource_reference{"totalMaxMod20221124.zip"}


# dayStr = '20220902'

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

def getTotalData(dayStr):
    max = 200.0
    day = datetime.datetime.strptime(dayStr,'%Y%m%d')
    # dayStr2 是%Y-%m-%d格式的
    # dayStr2 = day.strftime("%Y-%m-%d")
    sinceTimeStr = dayStr
    sinceTimeStr2 = day.strftime("%Y-%m-%d")

    unitlTimeStr = dayStr
    unitlTimeStr2 = day.strftime("%Y-%m-%d") + ' 23:59:59'

    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

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
            sum(
                case when r1usd_max is null then 0
                    else r1usd_max
                end) as sumR1usd,
            sum(
                case when r7usd_max is null then 0
                    else r7usd_max
                end) as sumR7usd,
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
                    case
                        when r1usd >= %f
                        then %f
                        else r1usd
                    end as r1usd_max,
                    case
                        when r7usd >= %f
                        then %f
                        else r7usd
                    end as r7usd_max,
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
    '''%(whenStr,max,max,max,max,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    print(sql)
    pd_df = execSql(sql)
    return pd_df

# 对数据做基础处理
def dataStep4(dataDf3):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf3['install_date'].unique()
    for install_date in install_date_list:
        print(install_date)
        df = dataDf3.loc[(dataDf3.install_date == install_date)]
        dataNeedAppend = {
            'install_date':[],
            'count':[],
            'sumr7usd':[],
            'cv':[]
        }
        for i in range(64):
            if df.loc[(df.cv == i),'sumr7usd'].sum() == 0 \
                and df.loc[(df.cv == i),'count'].sum() == 0:
                dataNeedAppend['install_date'].append(install_date)
                dataNeedAppend['count'].append(0)
                dataNeedAppend['sumr7usd'].append(0)
                dataNeedAppend['cv'].append(i)

        dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    return dataDf3

def predict(dayStr):
    name = 'total'
    with zipfile.ZipFile('totalMaxMod20221124.zip') as myzip:
        with open('%sMod.h5'%name,'wb') as f:
            with myzip.open('%sMod.h5'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sMod.h5'%name)
        with open('%sMean.npy'%name,'wb') as f:
            with myzip.open('%sMean.npy'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sMean.npy'%name)
        with open('%sStd.npy'%name,'wb') as f:
            with myzip.open('%sStd.npy'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sStd.npy'%name)

    df = getTotalData(dayStr)
    df3 = dataStep4(df)

    retData={
        'install_date':[],
        'p7usd':[]
    }
    
    mod = tf.keras.models.load_model('%sMod.h5'%name)
    mod.summary()

    mean = np.load('%sMean.npy'%name)
    std = np.load('%sStd.npy'%name)

    day = datetime.datetime.strptime(dayStr,'%Y%m%d')
    # dayStr2 是%Y-%m-%d格式的
    dayStr2 = day.strftime("%Y-%m-%d")
    
    trainDf = df3.loc[
        (df3.install_date == dayStr2)
    ].sort_values(by=['install_date','cv'])
    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    # 尝试标准化
    trainXSs = (trainX - mean)/std
    yp = mod.predict(trainXSs)

    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    trainY1 = trainSumByDay['sumr1usd'].to_numpy()

    usd = trainY1.reshape(-1)[0] * (yp.reshape(-1)[0] + 1)

    retData['install_date'].append(dayStr)
    retData['p7usd'].append(usd)
    
    return retData

from odps.models import Schema, Column, Partition
def createTable():
    columns = [
        Column(name='p7usd', type='double', comment='predict d7 revenue in usd')
    ]
    partitions = [
        Partition(name='install_date', type='string', comment='like 20221018')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('topwar_iosglobal_total1p7_max200', schema, if_not_exists=True)
    return table

# import pyarrow as pa
def writeTable(df,dayStr):
    t = o.get_table('topwar_iosglobal_total1p7_max200')
    t.delete_partition('install_date=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='install_date=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        writer.write(df)


createTable()
retData = predict(dayStr)
# print ('pred ret:',retData)
df = pd.DataFrame(
    data=retData
)
print (df)
writeTable(df,dayStr)