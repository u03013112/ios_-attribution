# 计算用户付费次数与CV的对应值
# 用iOS用户数据来做验证
# 1、用平均值
# 2、用中位数值
# 然后计算偏差，偏差暂时用MAPE？

import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

def getDataFromMC():
    sql = '''
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
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then 1
                    else 0
                end
            ) as r1pc
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20230401
            and day < 20230601
        group by
            install_date,
            customer_user_id
        ;
    '''

    df = execSql(sql)
    return df


def addCV(df,cvMapDf):
    df.loc[:,'cv'] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        df.loc[
            (df.r1usd > min_event_revenue) & (df.r1usd <= max_event_revenue),
            'cv'
        ] = i
    df.loc[
        (df.r1usd > max_event_revenue),
        'cv'
    ] = len(cvMapDf)-1
    return df

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
        mapData['max_event_revenue'].append(levels[i])

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def resaultWithMean(df):
    retDf = df.groupby(by=['cv'],as_index=False).agg({
        'r1pc':'mean'
    })
    # 对retDf的r1pc列四舍五入取整，并改为int类型
    retDf['r1pc'] = retDf['r1pc'].round().astype(int)
    return retDf

def resaultWithMedian(df):
    # 对retDf进行groupby by 'cv' ，r1pc 列 取中位数
    retDf = df.groupby(by=['cv'],as_index=False).agg({
        'r1pc':'median'
    })
    # 对retDf的r1pc列四舍五入取整，并改为int类型
    retDf['r1pc'] = retDf['r1pc'].round().astype(int)
    return retDf

# 计算付费次数的mape，按cv来分
def resaultMape(cvDf,retDf):
    # 计算媒体的估算付费次数
    cvMergeDf = pd.merge(cvDf, retDf, on='cv', how='left', suffixes=('', '_ret'))
    # 计算每日的真实付费次数
    # 计算MAPE
    cvMergeDfByDate = cvMergeDf.groupby(by=['install_date'],as_index=False).agg({
        'r1pc':'sum',
        'r1pc_ret':'sum'
    })
    
    # 计算dfByInstallDate里面'r1pc'列 和'r1pc_ret'列每一行的MAPE，并记录在'r1pc_mape'列
    cvMergeDfByDate['r1pc_mape'] = abs((cvMergeDfByDate['r1pc'] - cvMergeDfByDate['r1pc_ret']) / cvMergeDfByDate['r1pc']) * 100

    return cvMergeDfByDate

def afCheck():
    df = pd.read_csv(getFilename('iosCvCount20230401_20230601'))
    df = df.loc[df['install_date'] >= '2023-04-01']
    df = df.loc[df['r1usd'] > 0]

    cvMapDf = pd.read_csv('/src/afCvMap2304.csv')
    cvMapDf = cvMapDf.loc[cvMapDf.conversion_value < 32]
    # cvMapDf 拥有列 app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
    # 将cvMapDf 拆分为两个df，一个是付费金额的cvRevenueMapDf，一个是付费次数的cvCounterMapDf
    cvRevenueMapDf = cvMapDf.loc[cvMapDf.event_name == 'af_skad_revenue']
    cvRevenueMapDf = cvRevenueMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvCounterMapDf = cvMapDf.loc[cvMapDf.event_name == 'af_purchase']
    cvCounterMapDf = cvCounterMapDf[['conversion_value','min_event_counter','max_event_counter']]

    # cvRevenueMapDf 添加一行，conversion_value = 0, min_event_revenue = -1, max_event_revenue = 0
    cvRevenueMapDf = cvRevenueMapDf.append({'conversion_value':0,'min_event_revenue':-1,'max_event_revenue':0},ignore_index=True)
    # cvCounterMapDf 添加一行，conversion_value = 0, min_event_counter = 0, max_event_counter = 0
    cvCounterMapDf = cvCounterMapDf.append({'conversion_value':0,'min_event_counter':0,'max_event_counter':0},ignore_index=True)
    # cvCounterMapDf 添加一行，conversion_value = 1, min_event_counter = 0, max_event_counter = 1
    cvCounterMapDf = cvCounterMapDf.append({'conversion_value':1,'min_event_counter':0,'max_event_counter':1},ignore_index=True)

    df['conversion_value'] = 0
    for cv1 in cvRevenueMapDf['conversion_value'].values:
        min_event_revenue = cvRevenueMapDf.loc[cvRevenueMapDf['conversion_value'] == cv1, 'min_event_revenue'].mean()
        max_event_revenue = cvRevenueMapDf.loc[cvRevenueMapDf['conversion_value'] == cv1, 'max_event_revenue'].mean()
        print('cv1:',cv1,'min_event_revenue:',min_event_revenue,'max_event_revenue:',max_event_revenue)
        df.loc[
            (df['r1usd']>min_event_revenue) & (df['r1usd']<=max_event_revenue),'conversion_value'
        ] = cv1
    
    max_event_revenue = cvRevenueMapDf.loc[cvRevenueMapDf['conversion_value'] == 31, 'max_event_revenue'].mean()
    df.loc[df['r1usd']>max_event_revenue,'conversion_value'] = 31

    df = df.merge(cvCounterMapDf,on='conversion_value',how='left')

    # df 添加一列 'drop',值为1
    # 如果 df 列 ‘r1pc’ 介于 min_event_counter 和 max_event_counter 之间，那么 'drop' 列的值为0
    # 按天分组，计算每天的 'drop' ==1 的‘r1usd’占比
    df['drop'] = 1
    df.loc[
        (df['r1pc'] >= df['min_event_counter']) & (df['r1pc'] <= df['max_event_counter']), 'drop'
    ] = 0

    df.to_csv(getFilename('iosCvCount20230401_20230601_afCheck'),index=False)
    # daily_drop_percentage = df[df['drop'] == 1].groupby('install_date')['r1usd'].sum() / df.groupby('install_date')['r1usd'].sum()
    # print(daily_drop_percentage)
    dfGroup = df.groupby('install_date',as_index=False).agg({'r1usd':'sum'})
    dfDropGroup = df[df['drop'] == 1].groupby('install_date',as_index=False).agg({'r1usd':'sum'})
    dfGroup = dfGroup.merge(dfDropGroup,on='install_date',how='left',suffixes=('_total','_drop'))
    dfGroup['drop_percentage'] = dfGroup['r1usd_drop'] / dfGroup['r1usd_total']
    print(dfGroup)
    dfGroup.to_csv(getFilename('iosCvCount20230401_20230601_afCheck_group'),index=False)

def afCheckDebug():
    df = pd.read_csv(getFilename('iosCvCount20230401_20230601_afCheck'))
    df.loc[df['install_date'] == '2023-05-01'].to_csv(getFilename('iosCvCount20230401_20230601_afCheck_0701'),index=False)
    
    



    

if __name__ == '__main__':
    # if __debug__:
    #     print('debug 模式，并未真的sql')
    # else:
    #     df = getDataFromMC()
    #     df.to_csv(getFilename('iosCvCount20230401_20230601'))

    # afCheck()
    afCheckDebug()

    # df = pd.read_csv(getFilename('iosCvCount20230401_20230601'))
    # df = df.loc[df.install_date >= '2022-07-01']
    # df = df.sort_values(['install_date','r1usd'])

    # # 31
    # levels31 = [
    #     1.6448,3.2418,5.3475,7.7988,10.7114,14.465,18.992,24.2942,31.0778,40.2628,51.5247,61.2463,70.1597,82.5565,97.3848,111.5657,125.2677,142.6695,161.6619,184.4217,204.8459,239.7421,264.9677,306.9067,355.154,405.6538,458.3643,512.6867,817.0817,1819.0253,2544.7372
    # ]

    # # 63
    # levels63 = [
    #     0.648,1.2907,1.9176,2.7844,3.726,4.7867,5.8386,6.8154,7.8049,8.8,9.7343,10.6687,11.7101,12.9774,14.3594,15.9967,17.8537,19.7527,21.7289,23.9905,26.2146,28.44,30.7644,33.3238,36.032,38.895,41.9714,44.8508,47.8631,51.1492,54.6564,58.8205,63.3812,68.2377,73.3717,77.9856,83.3221,89.6763,95.7669,100.728,108.7487,115.0222,120.7689,126.2867,131.7948,138.5602,147.513,154.6161,162.5357,169.5177,175.2142,187.0416,198.3921,209.4255,224.2466,254.2566,279.3109,322.7375,364.6856,438.3132,614.4384,1308.8256,2544.7372
    # ]

    # levelsList = [
    #     levels31
    #     # ,levels63
    # ]

    # for levels in levelsList:
    #     # cvMapDf = makeCvMap(levels)
    #     # cvMapDf.to_csv('/src/data/cvMap32.csv')

    #     cvMapDf = pd.read_csv('/src/afCvMap2303.csv')
        
    #     cvDf = addCV(df,cvMapDf)
    #     # print('111:',cvDf.loc[cvDf['cv']==2])
    #     # exit()
    #     meanRet = resaultWithMean(cvDf)
    #     print(meanRet)
    #     meanMape = resaultMape(cvDf,meanRet)
    #     # meanRet 中 列 'r1pc' 改名为 'pay counts'
    #     meanRet.rename(columns={'r1pc': 'pay counts'}, inplace=True)
        
    #     meanRet.loc[:,'min_event_revenue'] = cvMapDf['min_event_revenue']
    #     meanRet.loc[:,'max_event_revenue'] = cvMapDf['max_event_revenue']
    #     # print(meanRet)
    #     meanRet.to_csv('/src/data/cvCountMean%d.csv'%(len(levels)))
    #     print('%d mean MAPE:%.2f%%'%(len(levels),meanMape['r1pc_mape'].mean()))

    #     medianRet = resaultWithMedian(cvDf)
    #     medianMape = resaultMape(cvDf,medianRet)
    #     # medianRet 中 列 'r1pc' 改名为 'pay counts'
    #     medianRet.rename(columns={'r1pc': 'pay counts'}, inplace=True)
        
    #     medianRet.loc[:,'min_event_revenue'] = cvMapDf['min_event_revenue']
    #     medianRet.loc[:,'max_event_revenue'] = cvMapDf['max_event_revenue']
    #     print(medianRet)
    #     medianRet.to_csv('/src/data/cvCountMedian%d.csv'%(len(levels)))
    #     print('%d Median MAPE:%.2f%%'%(len(levels),medianMape['r1pc_mape'].mean()))




