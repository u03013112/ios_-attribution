# 推测自然量
import datetime
import pandas as pd
import sys
sys.path.append('/src')
from src.smartCompute import SmartCompute

def getFilename(filename):
    return '/src/data/%s.csv'%(filename)

# cvMap here
afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')

# 流程思路
# 1、找到指定日期的af events安装数
# 2、找到对应日的skan安装数
# 3、差值就是自然量，暂时这么认为
# 4、找到前n日含有idfa的自然量cv分布
# 5、预测当日cv分布并进行记录
# 6、获得一段时间的af events cv对应金额
# 7、获得一段时间的skan cv对应金额
# 8、skan金额+sum 预测金额 与 af 金额作比较

def getAFInstallCount(sinceTimeStr,unitlTimeStr):
    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2) + ' 23:59:59'

    sql='''
        select
            count(distinct customer_user_id) as count
        from (
            select
                customer_user_id
            from ods_platform_appsflyer_events
            where
                app_id='id1479198816'
                and event_name='install'
                and zone=0
                and day>=%s and day<=%s
            union all
            select
                customer_user_id
                from tmp_ods_platform_appsflyer_origin_install_data
            where
                app_id='id1479198816'
                and zone='0'
                and install_time >="%s" and install_time<="%s"
        )
        ;
        '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    return pd_df['count'].get(0)

def getSkanInstallCount(sinceTimeStr,unitlTimeStr):
    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2)

    # 由于skan报告普遍要晚2~3天，所以unitlTimeStr要往后延长3天
    unitlTime = datetime.datetime.strptime(unitlTimeStr2,'%Y-%m-%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=3)).strftime('%Y%m%d')

    sql='''
        select
            count(*) as count
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and event_name in ('af_skad_redownload','af_skad_install')
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    return pd_df['count'].get(0)
    

def getIdfaCv(sinceTimeStr,unitlTimeStr):
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when sum(event_revenue_usd)>%d and sum(event_revenue_usd)<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2) + ' 23:59:59'

    sql='''
        select 
            cv,media_source,count(*) as count
        from (
            select
                customer_user_id,
                media_source,
                case
                    when sum(event_revenue_usd)=0 or sum(event_revenue_usd) is null then 0
                    %s
                    else 63
                end as cv
            from (
                select
                    customer_user_id,media_source,cast (event_revenue_usd as double )
                from ods_platform_appsflyer_events
                where
                    app_id='id1479198816'
                    and event_timestamp-install_timestamp<=24*3600
                    and event_name in ('af_purchase','install')
                    and idfa is not null
                    and zone=0
                    and day>=%s and day<=%s
                union all
                select
                    customer_user_id,media_source, cast (event_revenue_usd as double )
                from tmp_ods_platform_appsflyer_origin_install_data
                where
                    app_id='id1479198816'
                    and zone='0'
                    and idfa is not null
                    and install_time >="%s" and install_time<="%s"
                )
            group by
            customer_user_id,
            media_source
        )
        group by
            cv,
            media_source
        ;
        '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    return pd_df

def predictCv(idfaCvRet,organicCount):
    data = {'cv':[],'count':[]}
    df = idfaCvRet
    idfaOrganicTotalCount = df[(pd.isna(df.media_source))]['count'].sum()
    for i in range(0,64):
        indexes = df[(df.cv == i) & pd.isna(df.media_source)].index
        if len(indexes) == 1:
            index = indexes[0]
            count = df['count'].get(index)
        else:
            count = 0
        
        c = organicCount * (count/idfaOrganicTotalCount)
        data['cv'].append(i)
        data['count'].append(round(c))
    return pd.DataFrame(data = data)

# cv转成usd，暂时用最大值来做
def cvToUSD(retDf):
    for i in range(len(afCvMapDataFrame)):
        # min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            max_event_revenue = 0
        # retDf.iloc[i]*=max_event_revenue
        print(i,max_event_revenue)
        retDf.loc[retDf.cv==i,'count']*=max_event_revenue
    return retDf

def getAFCvUsdSum(sinceTimeStr,unitlTimeStr):
    # 先要获得AF cv，然后再转成usd
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when sum(event_revenue_usd)>%d and sum(event_revenue_usd)<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    sql='''
        select
            cv,count(*) as count
        from (
            select
                case
                    when sum(event_revenue_usd)=0 or sum(event_revenue_usd) is null then 0
                    %s
                    else 63
                end as cv
            from ods_platform_appsflyer_events
            where
                app_id='id1479198816'
                and event_timestamp-install_timestamp<=24*3600
                and event_name='af_purchase'
                and zone=0
                and day>=%s and day<=%s
            group by
                customer_user_id
        )
        group by
            cv
        ;
    '''%(whenStr,sinceTimeStr,unitlTimeStr)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    df = cvToUSD(pd_df)
    return df['count'].sum()

def getSkanCvUsd(sinceTimeStr,unitlTimeStr):
    sql='''
        select
            skad_conversion_value as cv,count(*) as count
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and skad_conversion_value>0
            and event_name='af_skad_revenue'
            and day>=%s and day <=%s
        group by skad_conversion_value
    '''%(sinceTimeStr,unitlTimeStr)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    # 这里做的save+load是必须的，可能是什么奇怪的bug，只有这样才能有效的后续计算
    pd_df.to_csv(getFilename('tmp'))
    pd_df= pd.read_csv(getFilename('tmp'))
    df = cvToUSD(pd_df)
    return df['count'].sum()

# 预测，预测需要前7日数据
# 由于目前只有5月1~5月31的数据，所以sinceTimeStr,unitlTimeStr暂时只能是 '20220508','20220531'
def main(sinceTimeStr,unitlTimeStr):
    # 参考数值，n指的是利用前n天的数据作为参考数值
    n = 7
    predictCvSumDf = pd.DataFrame(data={
        'cv':[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63],
        'count':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        })
    # for sinceTimeStr->unitlTimeStr
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y%m%d')
        print('开始预测自然量：',dayStr)
        afInstallCount = getAFInstallCount(dayStr,dayStr)
        print('获得af安装数：',afInstallCount)
        skanInstallCount = getSkanInstallCount(dayStr,dayStr)
        print('获得skan安装数：',skanInstallCount)
        organicCount = afInstallCount - skanInstallCount
        print('自然量安装数：',organicCount)
        

        # 获得参考数值，应该是day-n~day-1，共n天
        day_n = day - datetime.timedelta(days=n)
        day_nStr = day_n.strftime('%Y%m%d')
        day_1 = day - datetime.timedelta(days=1)
        day_1Str = day_1.strftime('%Y%m%d')

        idfaCvRet = getIdfaCv(day_nStr,day_1Str)
        df = predictCv(idfaCvRet,organicCount)
        predictCvSumDf += df
        print('预测结果：',df)
        print('暂时汇总结果：',predictCvSumDf)
    predictCvSumDf.to_csv(getFilename('mainTmp'))
    predictUsdSumDf = cvToUSD(predictCvSumDf)
    print('cv->df:',predictUsdSumDf)
    predictUsdSum = predictUsdSumDf['count'].sum()
    print('预测自然量付费总金额：',predictUsdSum)
    
    afUsdSum = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    print('AF付费总金额：',afUsdSum)
    skanUsdSum = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    print('skan付费总金额：',skanUsdSum)

    print('总金额差（skan付费总金额 + 预测自然量付费总金额） / AF付费总金额：',(skanUsdSum + predictUsdSum)/afUsdSum)

if __name__ == "__main__":
    # # organicCount = getAFInstallCount('20220508','20220508') - getSkanInstallCount('20220508','20220508')
    # organicCount = 7444
    # print(organicCount)
    # # idfaCvRet = getIdfaCv('20220501','20220507')
    # # idfaCvRet.to_csv(getFilename('20220507IdfaCv'))
    # idfaCvRet=pd.read_csv(getFilename('20220507IdfaCv'))
    # df = predictCv(idfaCvRet,organicCount)
    # df.to_csv(getFilename('20220508predictCv'))
    # df = getAFCvUsdSum('20220508','20220531')
    # print(df)

    # df = getSkanCvUsd('20220508','20220531')
    # print(df)

    main('20220508','20220531')
    