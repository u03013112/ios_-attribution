# 将doc/media2里面的结果中选出较好的进行组合，并得到结果，写日志
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

# def getIdfaData(sinceTimeStr,unitlTimeStr):
#     sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
#     unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
#     sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
#     unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d") + ' 23:59:59'

#     # 为了获得完整的7日回收，需要往后延长7天
#     unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

#     whenStr = ''
#     for i in range(len(afCvMapDataFrame)):
#         min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
#         max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
#         if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
#             continue
#         whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

#     sql = '''
#         select
#             cv,
#             count(*) as count,
#             sum(r1usd) as sumR1usd,
#             sum(r7usd) as sumR7usd,
#             install_date,
#             media
#         from
#             (
#                 select
#                     customer_user_id,
#                     case
#                         when r1usd = 0
#                         or r1usd is null then 0 % s
#                         else 63
#                     end as cv,
#                     r1usd,
#                     r7usd,
#                     install_date,
#                     media
#                 from
#                     (
#                         SELECT
#                             t0.customer_user_id,
#                             t0.install_date,
#                             t1.r1usd,
#                             t1.r7usd,
#                             t0.media
#                         FROM
#                             (
#                                 select
#                                     customer_user_id,
#                                     install_date,
#                                     media
#                                 from
#                                     (
#                                         select
#                                             customer_user_id,
#                                             to_char(
#                                                 to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
#                                                 "yyyy-mm-dd"
#                                             ) as install_date,
#                                             media_source as media
#                                         from
#                                             ods_platform_appsflyer_events
#                                         where
#                                             app_id = 'id1479198816'
#                                             and idfa is not null
#                                             and event_name = 'install'
#                                             and zone = 0
#                                             and day >= % s
#                                             and day <= % s
#                                             and install_time >= "%s"
#                                             and install_time <= "%s"
#                                         union
#                                         all
#                                         select
#                                             customer_user_id,
#                                             to_char(
#                                                 to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
#                                                 "yyyy-mm-dd"
#                                             ) as install_date,
#                                             media_source as media
#                                         from
#                                             tmp_ods_platform_appsflyer_origin_install_data
#                                         where
#                                             app_id = 'id1479198816'
#                                             and idfa is not null
#                                             and zone = '0'
#                                             and install_time >= "%s"
#                                             and install_time <= "%s"
#                                     )
#                                 group by
#                                     customer_user_id,
#                                     install_date,
#                                     media
#                             ) as t0
#                             LEFT JOIN (
#                                 select
#                                     customer_user_id,
#                                     to_char(
#                                         to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
#                                         "yyyy-mm-dd"
#                                     ) as install_date,
#                                     sum(
#                                         case
#                                             when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
#                                             else 0
#                                         end
#                                     ) as r1usd,
#                                     sum(
#                                         case
#                                             when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
#                                             else 0
#                                         end
#                                     ) as r7usd
#                                 from
#                                     ods_platform_appsflyer_events
#                                 where
#                                     app_id = 'id1479198816'
#                                     and event_name = 'af_purchase'
#                                     and zone = 0
#                                     and day >= % s
#                                     and day <= % s
#                                     and install_time >= "%s"
#                                     and install_time <= "%s"
#                                 group by
#                                     install_date,
#                                     customer_user_id
#                             ) as t1 ON t0.customer_user_id = t1.customer_user_id
#                     )
#             )
#         group by
#             cv,
#             install_date,
#             media
#     ;
#     '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

#     print(sql)
#     pd_df = execSql(sql)
#     return pd_df

# def getSkanData(sinceTimeStr,unitlTimeStr):
#     sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
#     unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
#     sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
#     unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d")

#     # 为了获得完整的7日回收，需要往后延长7天
#     unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

#     sql = '''
#         select
#             skad_conversion_value as cv,
#             install_date,
#             media_source as media,
#             count(*) as count
#         from
#             ods_platform_appsflyer_skad_details
#         where
#             app_id = "id1479198816"
#             and event_name  in ('af_skad_redownload','af_skad_install')
#             and day >= % s
#             and day <= % s
#             and install_date >= "%s"
#             and install_date <= "%s"
#         group by
#             skad_conversion_value,
#             install_date,
#             media_source
#     '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

#     print(sql)
#     pd_df = execSql(sql)
#     return pd_df

# 为df添加media cv
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
]


import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

def main(message):
    for media in mediaList:
        name = media['name']
        logDir = '/src/data/doc/media2/%s'%(name)
        
        createDocTotal(logDir,name)

mediaSkanDf = pd.read_csv(getFilename('mediaSkan2_20220501_20220930'))
totalDf = pd.read_csv(getFilename('totalData_20220501_20220930'))

import matplotlib.pyplot as plt
# 尝试用idfa的数据去预测一下skan整体数据 
def createDocTotal(logDir,mediaName):
    # 首先获得skan中的media数据
    global mediaSkanDf
    
    df1 = mediaSkanDf.loc[
        (mediaSkanDf.media_group == mediaName) & 
        (pd.isna(mediaSkanDf.cv)==False) &
        (mediaSkanDf.install_date >= '2022-08-01')
        ].sort_values(by=['install_date','cv'])
    input1 = df1['count'].to_numpy().reshape((-1,64))
    xSum1 = input1.sum(axis=1).reshape(-1,1)
    input1 = np.nan_to_num(input1/xSum1)

    dfUsd1 = cvToUSD2(df1)
    dfUsdSum1 = dfUsd1.groupby('install_date').agg({'usd':'sum'})

    global totalDf
    dfTotal = totalDf.loc[(totalDf.install_date >= '2022-08-01')].sort_values(by=['install_date','cv'])
    dfTotal['count'] = dfTotal['count'] - df1['count'].to_numpy()
    
    # 直接用大盘的cv减去这个媒体skan中的cv，可能不太准确，但是先看看大致
    input2 = dfTotal['count'].to_numpy().reshape((-1,64))
    input2[input2<0]=0
    xSum2 = input2.sum(axis=1).reshape(-1,1)
    input2 = input2/xSum2

    dfUsd2 = cvToUSD2(dfTotal)
    dfUsdSum2 = dfUsd2.groupby('install_date').agg({'usd':'sum'})

    ret1Filename = os.path.join(logDir, 'ret1.csv')
    ret1Df = pd.read_csv(ret1Filename)
    ret1Df = ret1Df.loc[ret1Df.groupby('message').val_loss.idxmin()].reset_index(drop=True)
    ret1Df = ret1Df.sort_values(by=['val_loss'])

    ret2Filename = os.path.join(logDir, 'ret2.csv')
    ret2Df = pd.read_csv(ret2Filename)
    ret2Df = ret2Df.loc[ret2Df.groupby('message').val_loss.idxmin()].reset_index(drop=True)
    ret2Df = ret2Df.sort_values(by=['val_loss'])

    logData = {
        'mape':[],
        'message1':[],
        'mape1':[],
        'path1':[],
        'message2':[],
        'mape2':[],
        'path2':[],
    }
    for i in range(len(ret1Df)):
        message1 = ret1Df.iloc[i].at['message']
        mape1 = ret1Df.iloc[i].at['val_loss']
        mod1Filename = os.path.join(ret1Df.iloc[i].at['path'], 'bestMod.h5')
        mod1 = tf.keras.models.load_model(mod1Filename)
        f = ret1Df.iloc[i].at['f']
        if f == 'minAndMax':
            min1Filename = os.path.join(ret1Df.iloc[i].at['path'], 'min.npy')
            min1 = np.load(min1Filename)
            max1Filename = os.path.join(ret1Df.iloc[i].at['path'], 'max.npy')
            max1 = np.load(max1Filename)

            x1 = (input1-min1)/(max1-min1)
            x1[x1 == np.inf] = 0
            x1[x1 == -np.inf] = 0
            x1 = np.nan_to_num(x1)
        else:
            mean1Filename = os.path.join(ret1Df.iloc[i].at['path'], 'mean.npy')
            mean1 = np.load(mean1Filename)
            std1Filename = os.path.join(ret1Df.iloc[i].at['path'], 'std.npy')
            std1 = np.load(std1Filename)

            x1 = (input1 - mean1)/std1
            x1[x1 == np.inf] = 0
            x1[x1 == -np.inf] = 0
            x1 = np.nan_to_num(x1)

        yP1 = mod1.predict(x1)
        # print(yP1)
        
        y1 = (yP1.reshape(-1) + 1)*dfUsdSum1['usd'].to_numpy().reshape(-1)
        
        for j in range(len(ret2Df)):
            message2 = ret2Df.iloc[j].at['message']
            mape2 = ret2Df.iloc[j].at['val_loss']
            mod2Filename = os.path.join(ret2Df.iloc[j].at['path'], 'bestMod.h5')
            mod2 = tf.keras.models.load_model(mod2Filename)

            f = ret2Df.iloc[j].at['f']
            if f == 'minAndMax':
                min2Filename = os.path.join(ret2Df.iloc[j].at['path'], 'min.npy')
                min2 = np.load(min2Filename)
                max2Filename = os.path.join(ret2Df.iloc[j].at['path'], 'max.npy')
                max2 = np.load(max2Filename)

                x2 = (input2-min2)/(max2-min2)
                x2[x2 == np.inf] = 0
                x2[x2 == -np.inf] = 0
                x2 = np.nan_to_num(x2)
            else:
                mean2Filename = os.path.join(ret2Df.iloc[j].at['path'], 'mean.npy')
                mean2 = np.load(mean2Filename)
                std2Filename = os.path.join(ret2Df.iloc[j].at['path'], 'std.npy')
                std2 = np.load(std2Filename)

                x2 = (input2 - mean2)/std2
                x2[x2 == np.inf] = 0
                x2[x2 == -np.inf] = 0
                x2 = np.nan_to_num(x2)
            yP2 = mod2.predict(x2)

            y2 = (yP2.reshape(-1) + 1)*dfUsdSum2['usd'].to_numpy().reshape(-1)

            y_pred = y1+y2
            dfTotalByDay = dfTotal.groupby('install_date').agg({'sumr7usd':'sum'})
            y_true = dfTotalByDay['sumr7usd'].to_numpy()
            
            mape = mapeFunc(y_true,y_pred)

            logData['mape'].append(mape)
            logData['message1'].append(message1)
            logData['mape1'].append(mape1)
            logData['path1'].append(ret1Df.iloc[i].at['path'])
            logData['message2'].append(message2)
            logData['mape2'].append(mape2)
            logData['path2'].append(ret2Df.iloc[j].at['path'])

            

            # plt.title("total")
            # plt.plot(y_true,'-',label='true')
            # plt.plot(y_pred,'-',label='pred')
            # plt.plot(y1,'-',label='pred1')
            # plt.legend()
            # filename = os.path.join(logDir, 'total.png')
            # plt.savefig(filename)
            # print('save pic',filename)
            # plt.clf()
    
    logDf = pd.DataFrame(data = logData)
    logDf.to_csv(os.path.join(logDir,'log3.csv'))

    
            
if __name__ == '__main__':
    main('total test')
    # createDocTotal('/src/data/doc/media2/google','google')
