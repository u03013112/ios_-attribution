# 大盘整体预测程序
# 用于放到dataworks上
import datetime
import pandas as pd

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers


# dayStr = '20220902'
# modName = '/src/src/predSkan/mod/mod02582-23.44.h5'
# import sys
# sys.path.append('/src')
# from src.maxCompute import execSql
# from src.tools import getFilename


dayStr = args['dayStr']
modName = 'mod02582-23.44.h5'
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
    # print(sql)
    pd_df = execSql(sql)
    return pd_df

def dataStep2(dataDf1):
    dataDf1.insert(dataDf1.shape[1],'group',0)
    for i in range(len(groupList)):
        l = groupList[i]
        for cv in l:
            dataDf1.loc[dataDf1.cv == cv,'group'] = i
    return dataDf1


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


def predict(dayStr):
    mod = tf.keras.models.load_model(modName)

    df = getTotalData(dayStr)
    df.to_csv(getFilename('predict'))

    df = pd.read_csv(getFilename('predict'))
    df2 = dataStep2(df)
    df3 = dataStep3(df2)

    testX = df3['count'].to_numpy().reshape((-1,64))
    yp = mod.predict(testX)

    return yp

from odps.models import Schema, Column, Partition
def createTable():
    columns = [
        Column(name='p7usd', type='double', comment='predict d7 revenue in usd')
    ]
    partitions = [
        Partition(name='install_date', type='string', comment='like 20221018')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('topwar_iosglobal_total1p7', schema, if_not_exists=True)
    return table

# import pyarrow as pa
def writeTable(df,dayStr):
    t = o.get_table('topwar_iosglobal_total1p7')
    t.delete_partition('install_date=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='install_date=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        # batch = pa.RecordBatch.from_pandas(df)
        # writer.write(batch)
        print(df)
        writer.write(df)


createTable()
py = predict(dayStr)[0][0]
writeTable(pd.DataFrame(data={
    'install_date':dayStr,
    'p7usd':py
}),dayStr)