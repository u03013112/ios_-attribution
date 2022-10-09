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
    print(sql)
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
    print(sql)
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
    print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    return pd_df

def predictCv(idfaCvRet,organicCount):
    ret = []
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
        ret.append(round(c))
    return pd.DataFrame(data = {'cv':ret})


def getAFCvUsd(sinceTimeStr,unitlTimeStr):
    pass

def getSkanCvUsd(sinceTimeStr,unitlTimeStr):
    pass

def main(sinceTimeStr,unitlTimeStr):
    pass

if __name__ == "__main__":
    # organicCount = getAFInstallCount('20220508','20220508') - getSkanInstallCount('20220508','20220508')
    organicCount = 7444
    print(organicCount)
    # idfaCvRet = getIdfaCv('20220501','20220507')
    # idfaCvRet.to_csv(getFilename('20220507IdfaCv'))
    idfaCvRet=pd.read_csv(getFilename('20220507IdfaCv'))
    df = predictCv(idfaCvRet,organicCount)
    df.to_csv(getFilename('20220508predictCv'))