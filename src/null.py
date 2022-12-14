# 推测null值
import random
import datetime
import pandas as pd
import sys
sys.path.append('/src')
from src.smartCompute import SmartCompute

from src.tools import afCvMapDataFrame
from src.tools import cvToUSD2,getFilename

# 流程思路
# 1、找到指定日的skan各媒体安装数，与各媒体null值安装数
# 2、找到各媒体前n日含有idfa的自然量cv分布
# 3、预测当日cv分布并进行记录
# 4、获得一段时间的af events cv对应金额
# 5、获得一段时间的skan cv对应金额
# 6、skan金额+sum 预测金额 与 af 金额
# 7、加上null金额进行验算

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
            count(*) as count,media_source,skad_conversion_value as cv
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and event_name in ('af_skad_redownload','af_skad_install')
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
        group by media_source,skad_conversion_value
        ;
        '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    return pd_df

# 新版本，group by install_time，这样可以一次取够数，不用来回sql
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
            media_source,
            skad_conversion_value as cv,
            install_date
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and event_name in ('af_skad_redownload','af_skad_install')
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
        group by 
            media_source,
            skad_conversion_value,
            install_date
        ;
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

def predictCv(idfaCvRet,skanInstallCountRet):
    # 预测
    # 1、遍历各媒体，找到cv null值数量
    # 2、找到各媒体参考比率
    # 3、进行预测
    skanInstallCountRet.to_csv(getFilename('skanInstallCountTmp'))
    skanInstallCountRet = pd.read_csv(getFilename('skanInstallCountTmp'))
    # print(len(skanInstallCountRet),skanInstallCountRet)
    
    data = {
        'cv':[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63],
        'count':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    }
    # totalCount = skanInstallCountRet[(pd.isna(skanInstallCountRet.cv))]['count'].sum()
    # print('共需预测人数：',totalCount)

    medias = skanInstallCountRet['media_source'].unique()
    for media in medias:
        indexes = skanInstallCountRet[(skanInstallCountRet.media_source == media) & (pd.isna(skanInstallCountRet.cv))].index
        if len(indexes) == 1:
            index = indexes[0]
            nullCount = skanInstallCountRet['count'].get(index)
            # print(media,'待预测用户数：',nullCount)
            # 这个数值如果太小，可能预测就会偏小，所以预测这个并不能直接用人数*比例，而是要尝试进行随机？
            # idfaOrganicTotalCount = idfaCvRet[(idfaCvRet.media_source == media)]['count'].sum()
            # ruler：一个尺子，直接随机一个上限数值，落在哪个区间，就给那个cv count+1，这个算法效率担忧
            ruler = []
            max = 0
            
            for i in range(0,64):
                indexes = idfaCvRet[(idfaCvRet.cv == i) & (idfaCvRet.media_source == media)].index
                if len(indexes) == 1:
                    index = indexes[0]
                    count = idfaCvRet['count'].get(index)
                else:
                    count = 0
                max += count
                ruler.append(count)
            # print(ruler,max)
            
            for i in range(nullCount):
                m = 0
                r = random.randint(0,max)
                for index in range(len(ruler)):
                    m += ruler[index]
                    if m >=r :
                        data['count'][index] += 1
                        break
            # print('暂时预测结论：',data['count'])
        else:
            # print(indexes)
            # print(media,'没有null值')
            continue
    
    return pd.DataFrame(data = data)

def predictCv2(idfaCvRet,skanInstallCountRet):
    # 预测
    # 1、遍历各媒体，找到cv null值数量
    # 2、找到各媒体参考比率
    # 3、进行预测
    skanInstallCountRet.to_csv(getFilename('skanInstallCountTmp'))
    skanInstallCountRet = pd.read_csv(getFilename('skanInstallCountTmp'))
    # print(len(skanInstallCountRet),skanInstallCountRet)
    predictCvSumDf = pd.DataFrame(data={
        'cv':[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63],
        'count':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    })

    # 额外添加一个分media的结论
    predictCvMedia = {
        'media':[],
        'cv':[],
        'count':[],
    }
    medias = skanInstallCountRet['media_source'].unique()
    for media in medias:
        # 每个media有个cv表,media没有null值
        data = {'cv':[],'count':[]}
        idfaCvRetMediaDf = idfaCvRet[(idfaCvRet.media_source == media)]
        nullCount = skanInstallCountRet.loc[pd.isna(skanInstallCountRet.cv) & (skanInstallCountRet.media_source == media),'count'].sum()
        if idfaCvRetMediaDf['count'].sum() == 0:
            continue
        sampleRet = idfaCvRetMediaDf.sample(n = nullCount,weights = idfaCvRetMediaDf['count'],replace=True)

        for i in range(0,64):
            # 这里要取行数，因为是整行抽样，不要取count
            c = len(sampleRet.loc[(sampleRet.cv == i)])
            data['cv'].append(i)
            data['count'].append(round(c))

            predictCvMedia['cv'].append(i)
            predictCvMedia['count'].append(c)
            predictCvMedia['media'].append(media)

        dataDf = pd.DataFrame(data = data)
        # 将不同media的预测数据加到一起
        predictCvSumDf['count'] += dataDf['count']
    return predictCvSumDf,pd.DataFrame(data = predictCvMedia)

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
        print('开始预测：',dayStr)
        # 找到指定日的skan各媒体安装数，与各媒体null值安装数
        skanInstallCount = getSkanInstallCount(dayStr,dayStr)

        # 获得参考数值，应该是day-n~day-1，共n天
        day_n = day - datetime.timedelta(days=n)
        day_nStr = day_n.strftime('%Y%m%d')
        day_1 = day - datetime.timedelta(days=1)
        day_1Str = day_1.strftime('%Y%m%d')

        idfaCvRet = getIdfaCv(day_nStr,day_1Str)
        df = predictCv(idfaCvRet,skanInstallCount)
        # 由于预测出来的顺序是一致的，所以直接加就好了
        predictCvSumDf['count'] += df['count']
        # print('预测结果：',df)
        # print('暂时汇总结果：',predictCvSumDf)
    predictCvSumDf.to_csv(getFilename('mainTmp'))
    predictUsdSumDf = cvToUSD(predictCvSumDf)
    # print('cv->df:',predictUsdSumDf)
    predictUsdSum = predictUsdSumDf['count'].sum()
    # print('预测null付费总金额：',predictUsdSum)
    return predictUsdSum

def main2(sinceTimeStr,unitlTimeStr,n=7):
    log = {
        'install_date':[],
        # 'af_install_count':[],
        # 'skan_install_count':[],
        'usd':[]
    }
    # 按照media细分的log
    logByMedia = {
        'install_date':[],
        'media':[],
        'revenueUsd':[],
        'nullUsd':[]
    }
    ret = {
        'install_date':[],
        'usd':[]
    }

    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        skanInstallCountDf = getSkanInstallCount2(sinceTimeStr,unitlTimeStr)        
        skanInstallCountDf.to_csv(getFilename('skanInstallCountDf'))

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

        # 获得参考数值，应该是day-n~day-1，共n天
        day_n = day - datetime.timedelta(days=n)
        day_nStr = day_n.strftime('%Y-%m-%d')
        day_1 = day - datetime.timedelta(days=1)
        day_1Str = day_1.strftime('%Y-%m-%d')
        
        idfaCvRet = idfaCvRetDf[(idfaCvRetDf.install_date >= day_nStr) & (idfaCvRetDf.install_date <= day_1Str)]
        # print(day_nStr,day_1Str,idfaCvRet)
        skanInstallCount = skanInstallCountDf[skanInstallCountDf.install_date == dayStr]
        predictCvDf,predictCvMediaDf = predictCv2(idfaCvRet,skanInstallCount)

        # log cv
        for i in range(0,64):
            key = 'cv'+str(i)
            v = predictCvDf.loc[predictCvDf.cv == i,'count'].sum()
            if key in log:
                log[key].append(v)
            else:
                log[key]=[v]
        # log by media
        predictUsdMediaDf = cvToUSD2(predictCvMediaDf)
        skanUsdDf = cvToUSD2(skanInstallCount)
        medias = predictUsdMediaDf['media'].unique()
        for media in medias:
            revenueUsd = skanUsdDf.loc[(skanUsdDf.media_source == media),'usd'].sum()
            nullUsd = predictUsdMediaDf.loc[(predictUsdMediaDf.media == media),'usd'].sum()
            logByMedia['install_date'].append(dayStr)
            logByMedia['media'].append(media)
            logByMedia['revenueUsd'].append(revenueUsd)
            logByMedia['nullUsd'].append(nullUsd)
                
        predictUsdDf = cvToUSD2(predictCvDf)
        # print('cv->df:',predictUsdDf)
        predictUsd = predictUsdDf['usd'].sum()
        # print('预测自然量付费总金额：',predictUsdSum)

        ret['install_date'].append(dayStr)
        ret['usd'].append(predictUsd)

        log['usd'].append(predictUsd)
    # print(log)
    logDf = pd.DataFrame(data=log)
    logDf.to_csv(getFilename('log%s_%s_%d_%s_%s'%(sinceTimeStr,unitlTimeStr,n,'sample','media')))

    logByMediaDf = pd.DataFrame(data=logByMedia)
    logByMediaDf.to_csv(getFilename('log%s_%s_%d_%s_%s_byMedia'%(sinceTimeStr,unitlTimeStr,n,'sample','media')))
    return pd.DataFrame(data = ret)

# 仿照自然量，粗算
def main3(sinceTimeStr,unitlTimeStr):
    n = 30
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    sinceTime = sinceTime - datetime.timedelta(days=n)
    sinceTimeStr2 = sinceTime.strftime('%Y%m%d')
    unitlTime = unitlTime - datetime.timedelta(days=1)
    unitlTimeStr2 = unitlTime.strftime('%Y%m%d')

    skanInstallCount = getSkanInstallCount(sinceTimeStr,unitlTimeStr)
    idfaCvRet = getIdfaCv(sinceTimeStr2,unitlTimeStr2)
    df = predictCv(idfaCvRet,skanInstallCount)
    predictUsdSumDf = cvToUSD(df)
    predictUsdSum = predictUsdSumDf['count'].sum()
    return predictUsdSum
 
def test():
    skanInstallCount = getSkanInstallCount('20220601','20220601')
    skanInstallCount.to_csv(getFilename('skanInstallCount'))
    skanInstallCount = pd.read_csv(getFilename('skanInstallCount'))
    print(len(skanInstallCount),skanInstallCount)
    return
    # idfaCvRet = getIdfaCv('20220601','20220607')
    # idfaCvRet.to_csv(getFilename('idfaCvRet'))
    idfaCvRet = pd.read_csv(getFilename('idfaCvRet'))
    ret = predictCv(idfaCvRet,skanInstallCount)
    print(ret)

def randomTest():
    data = {
        'cv':[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63],
        'count':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    }
    ruler = [2924, 13, 12, 7, 6, 5, 1, 2, 1, 1, 2, 1, 0, 1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    max = 2981
    
    for i in range(536):
        m = 0
        r = random.randint(0,max)
        for index in range(len(ruler)):
            m += ruler[index]
            if m >=r :
                data['count'][index] += 1
                break
    
    print(data)

    

if __name__ == "__main__":
    # 预测null付费总金额： 3567
    # main('20220601','20220630')
    # test()
    # randomTest()

    ret = main2('20220601','20220930',n=28)
    print(ret)
    