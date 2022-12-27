# 大盘
# 用于放到dataworks上
# 
# 按照media的方式，将cv先做成百分比
# 然后在标准化
# 最后再进行倍率（增长幅度）预测

# 用dwd_base_event_purchase_afattribution表的数据与之前的数据进行结合
# 这是按照uid重新进行归因后的数据
import datetime
import pandas as pd
import numpy as np

import tensorflow as tf

import zipfile

##@resource_reference{"totalMod20221223.zip"}


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
    print(sql)
    pd_df = execSql(sql)
    return pd_df

def getTotalData2(dayStr):
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
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
                    uid,
                    case
                        when r1usd = 0
                        or r1usd is null then 0 % s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date
                from
                    (
                        select
                            install_date,
                            uid,
                            sum(if(life_cycle <= 0, revenue_value_usd, 0)) as r1usd,
                            sum(if(life_cycle <= 6, revenue_value_usd, 0)) as r7usd
                        from
                            (
                                select
                                    game_uid as uid,
                                    to_char(
                                        to_date(install_day, "yyyymmdd"),
                                        "yyyy-mm-dd"
                                    ) as install_date,
                                    revenue_value_usd,
                                    DATEDIFF(
                                        to_date(day, 'yyyymmdd'),
                                        to_date(install_day, 'yyyymmdd'),
                                        'dd'
                                    ) as life_cycle
                                from
                                    dwd_base_event_purchase_afattribution
                                where
                                    app_package = "id1479198816"
                                    and app = 102
                                    and zone = 0
                                    and window_cycle = 9999
                                    and install_day = %s
                            )
                        group by
                            install_date,
                            uid

        )
        )group by
            cv,
            install_date
        ;
    '''%(whenStr,dayStr)
    print(sql)
    pd_df = execSql(sql)
    return pd_df

def dataStep12(df,df2):
    # 1、将df中的count 求和，即AF中一共的安装人数
    # 2、将df2中的count 求和，即重新归因后的安装人数
    # 3、将df的count-df2的count，获得应该补充的安装人数
    # 4、将这部分数据补充进去

    dfCountSum0 = df.groupby('install_date',as_index=False).agg({'count':'sum'}).sort_values(by=['install_date'])
    dfCountSum2 = df2.groupby('install_date',as_index=False).agg({'count':'sum'}).sort_values(by=['install_date'])

    dfCountDiff = dfCountSum0['count'] - dfCountSum2['count']

    dfRet = pd.DataFrame({
        'install_date':dfCountSum0['install_date'],
        'count':dfCountDiff
    })
    dfRet['cv'] = 0
    dfRet['sumr1usd'] = 0
    dfRet['sumr7usd'] = 0
    # print(dfRet)
    df2 = df2.append(dfRet,ignore_index=True)
    return df2

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
    with zipfile.ZipFile('totalMod20221223.zip') as myzip:
        with open('%sMod.h5'%name,'wb') as f:
            with myzip.open('%sMod.h5'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sMod.h5'%name)
        with open('%sMin.npy'%name,'wb') as f:
            with myzip.open('%sMin.npy'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sMin.npy'%name)
        with open('%sMax.npy'%name,'wb') as f:
            with myzip.open('%sMax.npy'%name) as zipF:
                f.write(zipF.read())
            print('unzip %sMax.npy'%name)

    df = getTotalData(dayStr)
    df2 = getTotalData2(dayStr)
    df3 = dataStep12(df,df2)
    
    df3 = dataStep4(df3)

    retData={
        'install_date':[],
        'p7usd':[]
    }
    
    mod = tf.keras.models.load_model('%sMod.h5'%name)
    mod.summary()

    min = np.load('%sMin.npy'%name)
    max = np.load('%sMax.npy'%name)

    day = datetime.datetime.strptime(dayStr,'%Y%m%d')
    # dayStr2 是%Y-%m-%d格式的
    dayStr2 = day.strftime("%Y-%m-%d")
    
    trainDf = df3.loc[
        (df3.install_date == dayStr2)
    ].sort_values(by=['install_date','cv'])
    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    # trainX = trainDf['count'].to_numpy().reshape((-1,64))
    # 尝试标准化
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainXSum = trainX.sum(axis=1).reshape(-1,1)
    trainX = trainX/trainXSum

    x = (trainX-min)/(max-min)
    x[x == np.inf] = 0
    x[x == -np.inf] = 0
    trainXSs = np.nan_to_num(x)

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
    table = o.create_table('topwar_iosglobal_total1p7_mse_p2', schema, if_not_exists=True)
    return table

# import pyarrow as pa
def writeTable(df,dayStr):
    t = o.get_table('topwar_iosglobal_total1p7_mse_p2')
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