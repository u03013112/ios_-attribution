# 推测自然量
import datetime
import pandas as pd
import sys
sys.path.append('/src')
from src.smartCompute import SmartCompute

from src.tools import afCvMapDataFrame
from src.tools import cvToUSD2,getFilename

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

    # sql='''
    #     select
    #         count(distinct customer_user_id) as count
    #     from (
    #         select
    #             customer_user_id
    #         from ods_platform_appsflyer_events
    #         where
    #             app_id='id1479198816'
    #             and event_name='install'
    #             and zone=0
    #             and day>=%s and day<=%s
    #         union all
    #         select
    #             customer_user_id
    #         from tmp_ods_platform_appsflyer_origin_install_data
    #         where
    #             app_id='id1479198816'
    #             and zone='0'
    #             and install_time >="%s" and install_time<="%s"
    #     )
    #     ;
    #     '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    
    sql='''
        select
            sum(count) as count
        from (
            select
                count(*) as count
            from ods_platform_appsflyer_events
            where
                app_id='id1479198816'
                and event_name='install'
                and zone=0
                and day>=%s and day<=%s
            union all
            select
                count(*) as count
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

# 新版本，group by install_time，这样可以一次取够数，不用来回sql
# 返回 df 2列 count 和 install_date
def getAFInstallCount2(sinceTimeStr,unitlTimeStr):
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
            sum(count) as count,install_date
        from (
            select
                count(*) as count,
                to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
            from ods_platform_appsflyer_events
            where
                app_id='id1479198816'
                and event_name='install'
                and zone=0
                and day>=%s and day<=%s
            group by
                install_date
            union all
            select
                count(*) as count,
                to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
            from tmp_ods_platform_appsflyer_origin_install_data
            where
                app_id='id1479198816'
                and zone='0'
                and install_time >="%s" and install_time<="%s"
            group by
                install_date
        )
        group by
            install_date
        ;
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)

    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    return pd_df

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
    
def getSkanInstallCount2(sinceTimeStr,unitlTimeStr):
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
            count(*) as count,
            install_date
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and event_name in ('af_skad_redownload','af_skad_install')
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
        group by
            install_date
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    return pd_df

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
            cv,
            media_source,
            count(*) as count
        from
            (
                select
                    customer_user_id,
                    media_source,
                    sum(
                        case
                            when idfa is not null then 1
                            else 0
                        end
                    ) as have_idfa,
                    case
                        when sum(event_revenue_usd) = 0
                        or sum(event_revenue_usd) is null then 0 
                        %s
                        else 63
                    end as cv
                from
                    (
                        select
                            customer_user_id,
                            media_source,
                            cast (event_revenue_usd as double),
                            idfa
                        from
                            ods_platform_appsflyer_events
                        where
                            app_id = 'id1479198816'
                            and event_timestamp - install_timestamp <= 24 * 3600
                            and event_name in ('af_purchase', 'install')
                            and zone = 0 
                            and day >= % s
                            and day <= % s
                        union
                        all
                        select
                            customer_user_id,
                            media_source,
                            cast (event_revenue_usd as double),
                            idfa
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
                    media_source
            )
        where
            have_idfa > 0
        group by
            cv,
            media_source;
        '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    # return
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    return pd_df

def getIdfaCv2(sinceTimeStr,unitlTimeStr):
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
            cv,
            media_source,
            count(*) as count,
            install_date
        from
            (
                select
                    customer_user_id,
                    media_source,
                    sum(
                        case
                            when idfa is not null then 1
                            else 0
                        end
                    ) as have_idfa,
                    case
                        when sum(event_revenue_usd) = 0
                        or sum(event_revenue_usd) is null then 0 
                        %s
                        else 63
                    end as cv,
                    install_date
                from
                    (
                        select
                            customer_user_id,
                            media_source,
                            cast (event_revenue_usd as double),
                            idfa,
                            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
                        from
                            ods_platform_appsflyer_events
                        where
                            app_id = 'id1479198816'
                            and event_timestamp - install_timestamp <= 24 * 3600
                            and event_name in ('af_purchase', 'install')
                            and zone = 0 
                            and day >= % s
                            and day <= % s
                        union
                        all
                        select
                            customer_user_id,
                            media_source,
                            cast (event_revenue_usd as double),
                            idfa,
                            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
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
                    media_source,
                    install_date
            )
        where
            have_idfa > 0
        group by
            cv,
            media_source,
            install_date
        ;
        '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    # return
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    return pd_df


def predictCv(idfaCvRet,organicCount):
    data = {'cv':[],'count':[]}
    df = idfaCvRet
    idfaOrganicTotalCount = df[(pd.isna(df.media_source))]['count'].sum()
    if idfaOrganicTotalCount > 0:
        for i in range(0,64):
            count = df.loc[(df.cv == i) & pd.isna(df.media_source),'count'].sum()
            # print(i,count,idfaOrganicTotalCount)
            c = organicCount * (count/idfaOrganicTotalCount)
            data['cv'].append(i)
            data['count'].append(round(c))
    return pd.DataFrame(data = data)

# 用sample来做，有时候有偏差
def predictCv2(idfaCvRet,organicCount):
    data = {'cv':[],'count':[]}
    
    idfaCvRetOrganicDf = idfaCvRet[pd.isna(idfaCvRet.media_source)]
    sampleRet = idfaCvRetOrganicDf.sample(n = organicCount,weights = idfaCvRetOrganicDf['count'],replace=True)

    for i in range(0,64):
        # 这里要取行数，因为是整行抽样，不要取count
        c = len(sampleRet.loc[(sampleRet.cv == i)])
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
        # print(i,max_event_revenue)
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
# 参考数值，n指的是利用前n天的数据作为参考数值
def main(sinceTimeStr,unitlTimeStr,n=7):
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
        # 由于预测出来的顺序是一致的，所以直接加就好了
        predictCvSumDf['count'] += df['count']
        # print('预测结果：',df)
        # print('暂时汇总结果：',predictCvSumDf)
    predictCvSumDf.to_csv(getFilename('mainTmp'))
    predictUsdSumDf = cvToUSD(predictCvSumDf)
    # print('cv->df:',predictUsdSumDf)
    predictUsdSum = predictUsdSumDf['count'].sum()
    # print('预测自然量付费总金额：',predictUsdSum)
    
    return predictUsdSum
    

# 更加快速，不用反复request
# 返回一个df，列 install_date,usd
def main2(sinceTimeStr,unitlTimeStr,n=7): 
    log = {
        'install_date':[],
        'af_install_count':[],
        'skan_install_count':[],
        'usd':[]
    }
    ret = {
        'install_date':[],
        'usd':[]
    }

    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        afInstallCountDf = getAFInstallCount2(sinceTimeStr,unitlTimeStr)
        skanInstallCountDf = getSkanInstallCount2(sinceTimeStr,unitlTimeStr)
        afInstallCountDf.to_csv(getFilename('afInstallCountDf'))
        skanInstallCountDf.to_csv(getFilename('skanInstallCountDf'))

    afInstallCountDf = pd.read_csv(getFilename('afInstallCountDf'))
    skanInstallCountDf = pd.read_csv(getFilename('skanInstallCountDf'))
    
    # for sinceTimeStr->unitlTimeStr
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

    day_n = sinceTime - datetime.timedelta(days=n)
    day_nStr = day_n.strftime('%Y%m%d')
    day_1 = unitlTime - datetime.timedelta(days=1)
    day_1Str = day_1.strftime('%Y%m%d')
    
    # 从起始日往前n天，到截止日往前1天的所有idfa数值
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        idfaCvRetDf = getIdfaCv2(day_nStr,day_1Str)
        idfaCvRetDf.to_csv(getFilename('idfaCvRetDf'))

    idfaCvRetDf = pd.read_csv(getFilename('idfaCvRetDf'))

    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        # print('开始预测自然量：',dayStr)
        log['install_date'].append(dayStr)
        afInstallCount = afInstallCountDf.loc[afInstallCountDf.install_date == dayStr,'count'].sum()
        skanInstallCount = skanInstallCountDf.loc[skanInstallCountDf.install_date == dayStr,'count'].sum()
        organicCount = afInstallCount - skanInstallCount
        # print('获得af安装数：',afInstallCount)
        log['af_install_count'].append(afInstallCount)
        # print('获得skan安装数：',skanInstallCount)
        log['skan_install_count'].append(skanInstallCount)
        
        # print('自然量安装数：',organicCount)

        # 获得参考数值，应该是day-n~day-1，共n天
        day_n = day - datetime.timedelta(days=n)
        day_nStr = day_n.strftime('%Y-%m-%d')
        day_1 = day - datetime.timedelta(days=1)
        day_1Str = day_1.strftime('%Y-%m-%d')
        # print(day_nStr,day_1Str)
        idfaCvRet = idfaCvRetDf[(idfaCvRetDf.install_date >= day_nStr) & (idfaCvRetDf.install_date <= day_1Str)]
        # print(idfaCvRet)
        predictCvDf = predictCv2(idfaCvRet,organicCount)
        # print(predictCvDf)
        # log cv
        for i in range(0,64):
            key = 'cv'+str(i)
            v = predictCvDf.loc[predictCvDf.cv == i,'count'].sum()
            if key in log:
                log[key].append(v)
            else:
                log[key]=[v]
        predictUsdDf = cvToUSD2(predictCvDf)
        # print('cv->df:',predictUsdDf)
        predictUsd = predictUsdDf['usd'].sum()
        # print('预测自然量付费总金额：',predictUsdSum)

        ret['install_date'].append(dayStr)
        ret['usd'].append(predictUsd)

        log['usd'].append(predictUsd)
    
    logDf = pd.DataFrame(data=log)
    logDf.to_csv(getFilename('log%s_%s_%d_%s_%s'%(sinceTimeStr,unitlTimeStr,n,'sample','organic')))
    return pd.DataFrame(data = ret)

# 用指定时间前一个月的时间，直接预测这一段时间的总值
def main3(sinceTimeStr,unitlTimeStr): 
    afInstallCount = getAFInstallCount(sinceTimeStr,unitlTimeStr)
    # print('获得af安装数：',afInstallCount)
    skanInstallCount = getSkanInstallCount(sinceTimeStr,unitlTimeStr)
    # print('获得skan安装数：',skanInstallCount)
    organicCount = afInstallCount - skanInstallCount
    # print('自然量安装数：',organicCount)

    n = 30
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    sinceTime = sinceTime - datetime.timedelta(days=n)
    sinceTimeStr2 = sinceTime.strftime('%Y%m%d')
    unitlTime = unitlTime - datetime.timedelta(days=1)
    unitlTimeStr2 = unitlTime.strftime('%Y%m%d')

    idfaCvRet = getIdfaCv(sinceTimeStr2,unitlTimeStr2)
    df = predictCv(idfaCvRet,organicCount)
    # print('预测结果：',df)

    predictUsdSumDf = cvToUSD(df)
    # print('cv->df:',predictUsdSumDf)
    predictUsdSum = predictUsdSumDf['count'].sum()
    return predictUsdSum


# 首日付费率计算
def test(sinceTimeStr,unitlTimeStr):
    print('首日付费率计算：',sinceTimeStr,unitlTimeStr)
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2)
    sql='''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase', 'install')
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        union
        all
        select
            customer_user_id,
            media_source,
            cast (event_revenue_usd as double)
        from
            tmp_ods_platform_appsflyer_origin_install_data
        where
            app_id = 'id1479198816'
            and zone = '0'
            and install_time >= "%s"
            and install_time <= "%s"
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    totalCount = pd_df['total_count'].get(0)
    print('总用户数：',totalCount)

    sql = '''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase')
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    payCount = pd_df['total_count'].get(0)
    print('付费用户数：',payCount)

    print('付费率：',payCount/totalCount)


# 拥有idfa首日付费率计算
def test2(sinceTimeStr,unitlTimeStr):
    print('拥有idfa首日付费率计算：',sinceTimeStr,unitlTimeStr)
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2)
    sql='''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase', 'install')
            and idfa is not null
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        union
        all
        select
            customer_user_id,
            media_source,
            cast (event_revenue_usd as double)
        from
            tmp_ods_platform_appsflyer_origin_install_data
        where
            app_id = 'id1479198816'
            and zone = '0'
            and idfa is not null
            and install_time >= "%s"
            and install_time <= "%s"
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    totalCount = pd_df['total_count'].get(0)
    print('总用户数：',totalCount)

    sql = '''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase')
            and idfa is not null
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)

    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    payCount = pd_df['total_count'].get(0)
    print('付费用户数：',payCount)

    print('付费率：',payCount/totalCount)

def test3(sinceTimeStr,unitlTimeStr):
    print('拥有idfa比率计算：',sinceTimeStr,unitlTimeStr)
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2)
    sql='''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase', 'install')
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        union
        all
        select
            customer_user_id,
            media_source,
            cast (event_revenue_usd as double)
        from
            tmp_ods_platform_appsflyer_origin_install_data
        where
            app_id = 'id1479198816'
            and zone = '0'
            and install_time >= "%s"
            and install_time <= "%s"
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    totalCount = pd_df['total_count'].get(0)
    print('总用户数：',totalCount)

    sql='''
        select
            count(distinct customer_user_id) as total_count
        from
        (
        select
            customer_user_id,
            media_source,
            cast (sum(event_revenue_usd) as double) as event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_timestamp - install_timestamp <= 24 * 3600
            and event_name in ('af_purchase', 'install')
            and idfa is not null
            and zone = 0
            and day >= %s
            and day <= %s
            and install_time >= "%s"
            and install_time <= "%s"
        group by
            customer_user_id,
            media_source
        union
        all
        select
            customer_user_id,
            media_source,
            cast (event_revenue_usd as double)
        from
            tmp_ods_platform_appsflyer_origin_install_data
        where
            app_id = 'id1479198816'
            and zone = '0'
            and idfa is not null
            and install_time >= "%s"
            and install_time <= "%s"
        )
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    idfaCount = pd_df['total_count'].get(0)
    print('idfa总用户数：',idfaCount)

    print('idfa比率:',idfaCount/totalCount)

# idfa数据中，各媒体对比
def test4(sinceTimeStr,unitlTimeStr): 
    print('idfa数据中，各媒体对比',sinceTimeStr,unitlTimeStr) 
    idfaCvRet = getIdfaCv(sinceTimeStr,unitlTimeStr)
    idfaCvRet.to_csv(getFilename('test4'))
    idfaCvRet = pd.read_csv(getFilename('test4'))

    idfaCvRet.loc[pd.isna(idfaCvRet.media_source),'media_source'] = 'organic'
    idfaUsdRet = cvToUSD2(idfaCvRet)
    medias = idfaUsdRet['media_source'].unique()
    
    for media in medias:
        if media == None:
            media = 'organic'
        
        
        # 用户数量
        userTotalCount = idfaUsdRet[(idfaUsdRet.media_source == media)]['count'].sum()
        # print('用户数量',userTotalCount)
        # 付费用户数量
        payUserTotalCount = idfaUsdRet[(idfaUsdRet.media_source == media) & (idfaUsdRet.usd > 0)]['count'].sum()
        # print('付费用户数量',payUserTotalCount)
        # 付费金额
        payTotalUsd = idfaUsdRet[(idfaUsdRet.media_source == media) & (idfaUsdRet.usd > 0)]['usd'].sum()
        # print('付费金额',payTotalUsd)
        
        if userTotalCount > 0:
            print('媒体',media)
            # 付费率
            payRate = payUserTotalCount/userTotalCount
            print('\t付费率%.2f%%'%(payRate*100))
            # 平均每用户付费金额
            arpu = payTotalUsd/userTotalCount
            print('\t平均每用户付费金额 $%.2f'%(arpu))
        if payUserTotalCount > 0:
            # 平均每付费用户付费金额
            arppu = payTotalUsd/payUserTotalCount
            print('\t平均每付费用户付费金额 $%.2f'%(arppu))


if __name__ == "__main__":
    # 预测自然量付费总金额： 7502
    # AF付费总金额： 66338
    # skan付费总金额： 47753
    # 总金额差（skan付费总金额 + 预测自然量付费总金额） / AF付费总金额： 0.8329313515632066
    # main('20220508','20220528')

    # 预测自然量付费总金额： 8706
    # AF付费总金额： 66338
    # skan付费总金额： 47753
    # 总金额差（skan付费总金额 + 预测自然量付费总金额） / AF付费总金额： 0.8510808284844282
    # main2('20220508','20220528')

    # test('20220508','20220528')
    # print(getAFInstallCount('20220601','20220630'))
    # print(getSkanInstallCount('20220601','20220630'))

    # 预测自然量付费总金额： 14217
    # AF付费总金额： 157988
    # skan付费总金额： 128188
    # 总金额差（skan付费总金额 + 预测自然量付费总金额） / AF付费总金额： 0.9013659265260653
    # main('20220601','20220630')

    # afInstallCount = getAFInstallCount('20220701','20220731')
    # print('获得af安装数：',afInstallCount)

    # afInstallCount = getAFInstallCount('20220801','20220831')
    # print('获得af安装数：',afInstallCount)

    # skanInstallCount = getSkanInstallCount('20220901','20220930')
    # print('获得skan安装数：',skanInstallCount)
    # organicCount = afInstallCount - skanInstallCount
    # print('自然量安装数：',organicCount)
    # main2('20220901','20220930')
    # main2('20220801','20220831')
    # main2('20220601','20220630')

    # idfaCvRet = getIdfaCv('20220901','20220930')
    # idfaCvRet.to_csv(getFilename('idfaCvRet'))

    # test('20220601','20220630')
    # test('20220701','20220731')
    # test('20220801','20220831')
    # test('20220901','20220930')

    # test2('20220601','20220630')
    # test2('20220701','20220731')
    # test2('20220801','20220831')
    # test2('20220901','20220930')

    # test3('20220601','20220630')
    # test3('20220701','20220731')
    # test3('20220801','20220831')
    # test3('20220901','20220930')

    # test4('20220601','20220630')
    # test4('20220701','20220731')
    # test4('20220801','20220831')
    # test4('20220901','20220930')
    # test4('20220601','20220930')
    # main3('20220901','20220930')


    ret = main2('20220901','20220930')
    print(ret['usd'].sum())

    # idfaCvRetDf = getIdfaCv2('20220825','20220929')
    # idfaCvRetDf.to_csv(getFilename('idfaCvRetDf'))
    # idfaCvRetDf = pd.read_csv(getFilename('idfaCvRetDf'))
    # idfaCvRetOrganicDf = idfaCvRetDf[pd.isna(idfaCvRetDf.media_source)]
    # print(idfaCvRetOrganicDf['count'].sum(),idfaCvRetOrganicDf.loc[idfaCvRetOrganicDf.cv == 0,'count'].sum())

    # pcv = idfaCvRetOrganicDf.sample(n = 1000,weights = idfaCvRetOrganicDf['count'],replace=True)
    # print(pcv)
    # cv0 = len(pcv.loc[(pcv.cv == 0)])
    # cv1 = len(pcv.loc[(pcv.cv == 1)])
    # cv2 = len(pcv.loc[(pcv.cv == 2)])
    # print(cv0,cv1,cv2)
    
    
    