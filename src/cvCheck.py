# 尝试比如数数中的cv值总量与skan中的cv值总量是否有线性关系
import pandas as pd

import sys
sys.path.append('/src')

from src.smartCompute import SmartCompute

def getFilename(filename):
    return '/src/data/%s.csv'%(filename)

def getSkanDataFromSmartCompute(sinceTimeStr,unitlTimeStr,filename):
    sql='''
            select
                *
            from ods_platform_appsflyer_skad_details
            where
                app_id="id1479198816"
                and skad_conversion_value>0
                and event_name='af_skad_revenue'
                and day>=%s and day <=%s
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

# 与 getSkanDataFromSmartCompute 区别是包含cv是0的用户
def getSkanDataFromSmartCompute2(sinceTimeStr,unitlTimeStr,filename):
    sql='''
            select
                skad_conversion_value,count(*) as count
            from ods_platform_appsflyer_skad_details
            where
                app_id="id1479198816"
                and event_name in ('af_skad_redownload','af_skad_install')
                and day>=%s and day <=%s
            group by skad_conversion_value
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))
    # print('%s saved'%(getFilename(filename)))

def cvTotal(filename):
    df = pd.read_csv(getFilename(filename))
    retStr = filename
    for cv in range(1,64):
        count = len(df[(df.skad_conversion_value) == cv])
        # print('cv is %s total count:%s'%(cv,count))
        retStr += ',%d'%(count)
    print(retStr)

# 与getSkanDataFromSmartCompute2 配套使用，额外统计cv是0的数据
def cvTotal2(filename):
    df = pd.read_csv(getFilename(filename))
    retStr = filename
    for cv in range(0,64):
        index = df[(df.skad_conversion_value) == cv].index
        if len(index) > 0:
            count = df['count'].get(index)
        else:
            count = 0
        retStr += ',%d'%(count)
    # 最后再加一列是null值
    index = df[pd.isna((df.skad_conversion_value))].index
    count = df['count'].get(index)
    retStr += ',%d'%(count)
    print(retStr)

def getAFEventsDataFromSmartCompute(sinceTimeStr,unitlTimeStr,filename):
    sql='''
        select
            sum(event_revenue_usd) as sum_revenue_usd
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_timestamp-install_timestamp<=24*3600
            and event_name='af_purchase'
            and zone=0
            and day>=%s and day<=%s
        group by customer_user_id;
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

# 与getAFEventsDataFromSmartCompute 区别在于他额外统计了安装事件，也就是统计了无支付的情况
def getAFEventsDataFromSmartCompute2(sinceTimeStr,unitlTimeStr,filename):
    sql='''
        select
            sum(event_revenue_usd) as sum_revenue_usd
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_timestamp-install_timestamp<=24*3600
            and event_name in ('af_purchase','install')
            and zone=0
            and day>=%s and day<=%s
        group by customer_user_id;
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

def cvTotalForAF(filename):
    df = pd.read_csv(getFilename(filename))
    df['cv'] = 63
    # 按照map，重新映射
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        df.loc[(df.sum_revenue_usd >= min_event_revenue) & (df.sum_revenue_usd < max_event_revenue),'cv'] = i
    
    retStr = filename
    for cv in range(1,64):
        count = len(df[(df.cv == cv)])
        retStr += ',%d'%(count)
    print(retStr)

# 配套getAFEventsDataFromSmartCompute2使用
def cvTotalForAF2(filename):
    df = pd.read_csv(getFilename(filename))
    df['cv'] = 63

    df.loc[(pd.isna(df.sum_revenue_usd)),'cv'] = 0
    # 按照map，重新映射
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        df.loc[(df.sum_revenue_usd >= min_event_revenue) & (df.sum_revenue_usd < max_event_revenue),'cv'] = i
    
    retStr = filename
    for cv in range(0,64):
        count = len(df[(df.cv == cv)])
        retStr += ',%d'%(count)
    print(retStr)

# 获得拥有idfa用户的数据，主要是为了获得各种比例，这里直接只找支付用户，这样直接用支付用户互相映射
# 最终得到的是cv值，所以预测出来也是cv，之后用金额区间的平均值做金额差值比例
def getAFEventsDataFromSmartComputeIdfa(sinceTimeStr,unitlTimeStr,filename):
    whenStr = ''
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
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
            ;
        '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

# 与 getAFEventsDataFromSmartComputeIdfa 配套使用，输出的列中额外多出一列media，
# 这里没有进一步汇总成自然量&非自然量是因为后面还要根据media做null值填充
# 返回各媒体渠道的cv df
def cvTotalForAFIdfa(filename):
    df = pd.read_csv(getFilename(filename))
    # df = pd.read_csv('/src/data/202204idfa.csv')
    medias = df['media_source'].unique()
    ret = {}
    retStr = 'media,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63\n'
    for media in medias:
        if pd.isna(media):
            retStr += '%s'%('organic')
            ret['organic'] = []
            for cv in range(0,64):
                count = len(df[(df.cv == cv) & pd.isna(df.media_source)])
                retStr += ',%d'%(count)
                ret['organic'].append(count)
        else:
            retStr += '%s'%(media)
            ret[media] = []
            for cv in range(0,64):
                count = len(df[(df.cv == cv) & (df.media_source==media)])
                retStr += ',%d'%(count)
                ret[media].append(count)
        retStr += '\n'
    print(retStr)
    retDf = pd.DataFrame(data = ret)
    return retDf

# 输入以media为列名，cv为索引的df，输出将以金额替代cv
# 金额部分暂时用max值
def cvToUSD(retDf):
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    for i in range(len(retDf)):
        # min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            max_event_revenue = 0
        retDf.iloc[i]*=max_event_revenue
    return retDf


# 获取大盘所有cv，用于验证，不再区分media，因为区分不开
def getAFEventsDataFromSmartComputeTotal(sinceTimeStr,unitlTimeStr,filename):
    whenStr = ''
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
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
            customer_user_id,
            case
                when sum(event_revenue_usd)=0 or sum(event_revenue_usd) is null then 0
                %s
                else 63
            end as cv
            from (
                select
                    customer_user_id,cast (event_revenue_usd as double )
                from ods_platform_appsflyer_events
                where
                    app_id='id1479198816'
                    and event_timestamp-install_timestamp<=24*3600
                    and event_name in ('af_purchase','install')
                    and zone=0
                    and day>=%s and day<=%s
                union all
                select
                    customer_user_id, cast (event_revenue_usd as double )
                from tmp_ods_platform_appsflyer_origin_install_data
                where
                    app_id='id1479198816'
                    and zone='0'
                    and install_time >="%s" and install_time<="%s"
                )
            group by
            customer_user_id
            ;
        '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

# total汇总
def cvTotalForAFTotal(filename):
    df = pd.read_csv(getFilename(filename))
    ret = {
        'total':[]
    }
    
    for cv in range(0,64):
        count = len(df[(df.cv == cv)])
        ret['total'].append(count)
        
    retDf = pd.DataFrame(data = ret)
    return retDf


from src.ss import Data
def getCVFromSS(sinceTimeStr,unitlTimeStr):
    ret = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    data = Data(since=sinceTimeStr,until=unitlTimeStr).get24HPayUserInfoEasy()
    for usd in data:
        cv = len(afCvMapDataFrame.max_event_revenue)-1
        # 暂时不考虑开闭区间问题，卡在区间边缘的数据并不常见
        cvDataFrame = afCvMapDataFrame[(afCvMapDataFrame.min_event_revenue<=usd) & (afCvMapDataFrame.max_event_revenue>usd)]
        if len(cvDataFrame) == 1:
            # 这里索引值就是cv值
            cv = cvDataFrame.conversion_value.index[0]
        else:
            # print("付费金额%f找不到对应的cv值"%(usd))
            pass
        ret[cv] += 1
    retStr = ''
    for i in range(1,64):
        retStr += ',%d'%(ret[i])
    return retStr


if __name__ == '__main__':
    # taskList = [
    #     # 开始日期，结束日期，文件名
    #     ['20220401','20220430','202204'],
    #     ['20220501','20220531','202205'],
    #     ['20220601','20220630','202206'],
    #     ['20220701','20220731','202207'],
    #     ['20220801','20220831','202208'],
    # ]
    # for i in range(len(taskList)):
    #     getSkanDataFromSmartCompute(taskList[i][0],taskList[i][1],taskList[i][2])
    #     cvTotal(taskList[i][2])

    # taskList = [
    #     # 开始日期，结束日期，文件名
    #     ['20220401','20220430','202204cv0'],
    #     ['20220501','20220531','202205cv0'],
    #     ['20220601','20220630','202206cv0'],
    #     ['20220701','20220731','202207cv0'],
    #     ['20220801','20220831','202208cv0'],
    # ]
    # for i in range(len(taskList)):
    #     getSkanDataFromSmartCompute2(taskList[i][0],taskList[i][1],taskList[i][2])
    #     cvTotal2(taskList[i][2])

    # print('2022-04',getCVFromSS('2022-04-01','2022-04-30'))
    # print('2022-05',getCVFromSS('2022-05-01','2022-05-31'))
    # print('2022-06',getCVFromSS('2022-06-01','2022-06-30'))
    # print('2022-07',getCVFromSS('2022-07-01','2022-07-31'))
    # print('2022-08',getCVFromSS('2022-08-01','2022-08-31'))


    # afTaskList = [
    #     # 开始日期，结束日期，文件名
    #     # 3月21日
    #     ['20220401','20220430','AF202204'],
    #     ['20220501','20220531','AF202205'],
    #     ['20220601','20220630','AF202206'],
    #     ['20220701','20220731','AF202207'],
    #     ['20220801','20220831','AF202208'],
    # ]
    # for i in range(len(afTaskList)):
    #     getAFEventsDataFromSmartCompute(afTaskList[i][0],afTaskList[i][1],afTaskList[i][2])
    #     cvTotalForAF(afTaskList[i][2])

    # data = Data(since='2022-04-01',until='2022-04-30').get24HPayUserInfoEasy()
    # print(len(data))

    # afTaskList = [
    #     # 开始日期，结束日期，文件名
    #     # 3月21日
    #     ['20220401','20220430','AF202204cv0'],
    #     ['20220501','20220531','AF202205cv0'],
    #     ['20220601','20220630','AF202206cv0'],
    #     ['20220701','20220731','AF202207cv0'],
    #     ['20220801','20220831','AF202208cv0'],
    # ]
    # for i in range(len(afTaskList)):
    #     getAFEventsDataFromSmartCompute2(afTaskList[i][0],afTaskList[i][1],afTaskList[i][2])
    #     cvTotalForAF2(afTaskList[i][2])


    # getAFEventsDataFromSmartComputeIdfa('20220501','20220531','202205idfa')
    # ret = cvToUSD(cvTotalForAFIdfa('202205idfa'))
    # ret.to_csv(getFilename('202205idfaUsd'))
    
    # getAFEventsDataFromSmartComputeTotal('20220501','20220531','202205total')
    ret = cvTotalForAFTotal('202205total')
    ret.to_csv(getFilename('202205totalCv'))
    ret = cvToUSD(ret)
    ret.to_csv(getFilename('202205totalUsd'))