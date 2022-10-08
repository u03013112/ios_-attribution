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
    sql='''
        select
            sum(event_revenue_usd) as sum_revenue_usd,media_source
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_timestamp-install_timestamp<=24*3600
            and event_name='af_purchase'
            and idfa is not null
            and zone=0
            and day>=%s and day<=%s
        group by customer_user_id,media_source;
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

# 与 getAFEventsDataFromSmartComputeIdfa 配套使用，输出的列中额外多出一列media，
# 这里没有进一步汇总成自然量&非自然量是因为后面还要根据media做null值填充
def cvTotalForAFIdfa(filename):
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

    taskList = [
        # 开始日期，结束日期，文件名
        ['20220401','20220430','202204cv0'],
        ['20220501','20220531','202205cv0'],
        ['20220601','20220630','202206cv0'],
        ['20220701','20220731','202207cv0'],
        ['20220801','20220831','202208cv0'],
    ]
    for i in range(len(taskList)):
        getSkanDataFromSmartCompute2(taskList[i][0],taskList[i][1],taskList[i][2])
        cvTotal2(taskList[i][2])

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


    