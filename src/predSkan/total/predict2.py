# 使用指定模型做预测
# 

import os
import numpy as np
import tensorflow as tf

# docPath 绝对路径，比如/src/data/doc/total/xxx
# 要求路径里面包括bestMod.h5，min.npy和max.npy
# inputNpArray为对应的输入np结构，直接将结论print到终端
def predict(docPath,inputNpArray):
    mod = tf.keras.models.load_model(os.path.join(docPath,'bestMod.h5'))
    min = np.load(os.path.join(docPath,'min.npy'))
    max = np.load(os.path.join(docPath,'max.npy'))

    # print(min,max)
    # sum = inputNpArray.sum(axis=1).reshape(-1,1)
    # inputNpArray = inputNpArray/sum

    x = (inputNpArray-min)/(max-min)
    x[x == np.inf] = 0
    x[x == -np.inf] = 0
    input = np.nan_to_num(x)

    yp = mod.predict(input)

    # return yp,input
    return yp


import datetime
import pandas as pd
import sys
sys.path.append('/src')
from src.tools import afCvMapDataFrame
from src.maxCompute import execSql
def getTotalData(dayStr):
    day = datetime.datetime.strptime(dayStr,'%Y%m%d')

    sinceTimeStr = dayStr
    unitlTimeStr = dayStr
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

    sinceTimeStr2 = day.strftime("%Y-%m-%d")
    unitlTimeStr2 = day.strftime("%Y-%m-%d") + ' 23:59:59'

    sql = '''
        select
            count(distinct customer_user_id) as count,
            substr(install_time,1,10) as install_date
        from
            ods_platform_appsflyer_push_event_realtime_v2
        where
            app_id = 'id1479198816'
            and event_name = 'install'
            and ds >= "%s"
            and ds <= "%s"
            and install_time >= "%s"
            and install_time <= "%s"
            and event_time_selected_timezone like '%%+0000'
            and to_char(to_date(substr(install_time,1,10),'yyyy-mm-dd'),'yyyymmdd') = ds
            and customer_user_id REGEXP '^[0-9]*$' 
        group by
            install_date
        ;
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    

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
                        or r1usd is null then 0 %s
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
                            sum(if(life_cycle <= 2, revenue_value_usd, 0)) as r1usd,
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
                                    dwd_base_event_purchase_afattribution_realtime
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



if __name__ == '__main__':
    # a = np.ones(64)*100
    # a[0] = 10000
    # a = a.reshape(-1,64)
    # # print(a)
    # print(predict('/src/data/doc/total/total_20221228_100638',a))
    # print(predict('/src/data/doc/total/total_20221228_105554',a))
    # print(predict('/src/data/doc/total/total_20221228_110747',a))

    # a = np.ones(128)*100
    # a[0] = 10000
    # a[64] = 10000
    # a = a.reshape(-1,128)
    # print(predict('/src/data/doc/total/total_20221229_040113',a))

    # androidTotalMod = '/src/data/doc/androidTotal/total_20230113_083842'

    # df = getTotalData('20220630')
    # df2 = getTotalData2('20220630')
    # df3 = dataStep12(df,df2)
    # df3 = dataStep4(df3)

    # df3.to_csv('/src/data/tmp.csv')

    df3 = pd.read_csv('/src/data/tmp.csv')

    trainDf = df3.sort_values(by=['install_date','cv'])
    trainDf = trainDf.groupby(['install_date','cv']).agg('sum')
    
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainXSum = trainX.sum(axis=1).reshape(-1,1)
    trainX = trainX/trainXSum

    print(trainX)


    print(predict('/src/data/doc/total//total_20230209_103322',trainX))


