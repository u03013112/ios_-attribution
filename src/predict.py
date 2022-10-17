# 整体预测，包括null预测和自然量预测，主要是整体预测，然后生成总体报告
import sys
sys.path.append('/src')
 
from src.tools import afCvMapDataFrame
from src.tools import cvToUSD2,getFilename

from src.smartCompute import SmartCompute

from src.organic import main as organicMain
from src.organic import main2 as organicMain2
from src.organic import main3 as organicMain3

from src.null import main as nullMain
from src.null import main2 as nullMain2
from src.null import main3 as nullMain3

import pandas as pd


def getAFRealUsdSum(sinceTimeStr,unitlTimeStr):
    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2) + ' 23:59:59'

    # 由于skan报告普遍要晚2~3天，所以unitlTimeStr要往后延长3天
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=3)).strftime('%Y%m%d')

    sql='''
        select
            sum(event_revenue_usd) as usd,
            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_timestamp-install_timestamp<=24*3600
            and event_name='af_purchase'
            and zone=0
            and day>=%s and day<=%s
            and install_time >="%s" and install_time<="%s"
        group by
            install_date
        ;
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    
    return pd_df


def getAFCvUsdSum(sinceTimeStr,unitlTimeStr):
    # 先要获得AF cv，然后再转成usd
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

    # 由于skan报告普遍要晚2~3天，所以unitlTimeStr要往后延长3天
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=3)).strftime('%Y%m%d')

    sql='''
        select
            cv,
            count(*) as count,
            install_date
        from (
            select
                case
                    when sum(event_revenue_usd)=0 or sum(event_revenue_usd) is null then 0
                    %s
                    else 63
                end as cv,
                to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date
            from ods_platform_appsflyer_events
            where
                app_id='id1479198816'
                and event_timestamp-install_timestamp<=24*3600
                and event_name='af_purchase'
                and zone=0
                and day>=%s and day<=%s
                and install_time >="%s" and install_time<="%s"
            group by
                customer_user_id,
                install_date
        )
        group by
            cv,
            install_date
        ;
    '''%(whenStr,sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    df = cvToUSD2(pd_df)
    return df

# 计算AF cv金额与实际金额差距
def AFCvAndRealDiff(sinceTimeStr,unitlTimeStr):
    realDf = getAFRealUsdSum(sinceTimeStr,unitlTimeStr)
    cvDf = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    realUsd = realDf['usd'].sum()
    cvUsd = cvDf['usd'].sum()
    print('%s~%s:真实付费：%.2f,cv付费：%.2f,(真实付费-cv付费)/真实付费=%.2f%%'%(sinceTimeStr,unitlTimeStr,realUsd,cvUsd,(realUsd-cvUsd)/realUsd*100))

def getSkanCvUsd(sinceTimeStr,unitlTimeStr):
    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2) + ' 23:59:59'

    # 由于skan报告普遍要晚2~3天，所以unitlTimeStr要往后延长3天
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=3)).strftime('%Y%m%d')

    sql='''
        select
            skad_conversion_value as cv,
            count(*) as count,
            install_date
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and skad_conversion_value>0
            and event_name='af_skad_revenue'
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
        group by 
            skad_conversion_value,
            install_date
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    # 这里做的save+load是必须的，可能是什么奇怪的bug，只有这样才能有效的后续计算
    pd_df.to_csv(getFilename('tmp'))
    pd_df= pd.read_csv(getFilename('tmp'))
    df = cvToUSD2(pd_df)
    return df

def main(sinceTimeStr,unitlTimeStr):
    retStr = sinceTimeStr + '~' + unitlTimeStr + '\n'
    predictOrganicUsdSum = organicMain(sinceTimeStr,unitlTimeStr)
    predictNullUsdSum = nullMain(sinceTimeStr,unitlTimeStr)
    retStr += '预测自然量付费总金额：'+str(predictOrganicUsdSum) +'\n'
    retStr += '预测null付费总金额：'+str(predictNullUsdSum)+'\n'
    print('预测自然量付费总金额：',predictOrganicUsdSum)
    print('预测null付费总金额：',predictNullUsdSum)
    afUsdSum = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    print('AF付费总金额：',afUsdSum)
    retStr += 'AF付费总金额：'+str(afUsdSum)+'\n'
    skanUsdSum = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    print('skan付费总金额：',skanUsdSum)
    retStr += 'skan付费总金额：'+str(skanUsdSum)+'\n'

    print('总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：',(skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum)
    retStr += '总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：'+str((skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum) + '\n'
    return retStr

import datetime
def main2(sinceTimeStr,unitlTimeStr,n=7):
    log = {
        'install_date':[],
        'skanUsd':[],
        'organicUsd':[],
        'nullUsd':[],
        'afUsd':[]
    }

    afUsdSumDf = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    skanUsdSumDf = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    predictOrganicUsdDf = organicMain2(sinceTimeStr,unitlTimeStr)
    predictNullUsdDf = nullMain2(sinceTimeStr,unitlTimeStr)

    # for sinceTimeStr->unitlTimeStr
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        log['install_date'].append(dayStr)
        log['skanUsd'].append(skanUsdSumDf.loc[skanUsdSumDf.install_date == dayStr,'usd'].sum())
        log['afUsd'].append(afUsdSumDf.loc[afUsdSumDf.install_date == dayStr,'usd'].sum())
        log['organicUsd'].append(predictOrganicUsdDf.loc[predictOrganicUsdDf.install_date == dayStr,'usd'].sum())
        log['nullUsd'].append(predictNullUsdDf.loc[predictNullUsdDf.install_date == dayStr,'usd'].sum())

    logDf = pd.DataFrame(data=log)
    logDf.to_csv(getFilename('log%s_%s_%d_%s_%s'%(sinceTimeStr,unitlTimeStr,n,'sample','predict')))

    predictOrganicUsd = predictOrganicUsdDf['usd'].sum()
    print('预测自然量付费总金额：',predictOrganicUsd)
    predictNullUsd = predictNullUsdDf['usd'].sum()
    print('预测null付费总金额：',predictNullUsd)
    afUsd = afUsdSumDf['usd'].sum()
    print('AF付费总金额：',afUsd)
    skanUsd = skanUsdSumDf['usd'].sum()
    print('skan付费总金额：',skanUsd)
    print('总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：',(skanUsd + predictOrganicUsd + predictNullUsd)/afUsd)

    return

def main3(sinceTimeStr,unitlTimeStr):
    retStr = sinceTimeStr + '~' + unitlTimeStr + '\n'
    predictOrganicUsdSum = organicMain3(sinceTimeStr,unitlTimeStr)
    predictNullUsdSum = nullMain3(sinceTimeStr,unitlTimeStr)
    retStr += '预测自然量付费总金额：'+str(predictOrganicUsdSum) +'\n'
    retStr += '预测null付费总金额：'+str(predictNullUsdSum)+'\n'
    print('预测自然量付费总金额：',predictOrganicUsdSum)
    print('预测null付费总金额：',predictNullUsdSum)
    afUsdSum = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    print('AF付费总金额：',afUsdSum)
    retStr += 'AF付费总金额：'+str(afUsdSum)+'\n'
    skanUsdSum = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    print('skan付费总金额：',skanUsdSum)
    retStr += 'skan付费总金额：'+str(skanUsdSum)+'\n'

    print('总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：',(skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum)
    retStr += '总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：'+str((skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum) + '\n'
    return retStr

if __name__ == "__main__":
    retStr = ''
    # retStr += main('20220901','20220930')
    # retStr += main('20220801','20220831')
    # retStr += main('20220701','20220731')
    # retStr += main('20220601','20220630')
    # retStr += main3('20220901','20220930')
    # retStr += main3('20220801','20220831')
    # retStr += main3('20220701','20220731')
    # retStr += main3('20220601','20220630')
    # print(retStr)

    # 20220701~20220731
    # 预测自然量付费总金额：14115
    # 预测null付费总金额：1404
    # AF付费总金额：153959
    # skan付费总金额：117721
    # 总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：0.865425210608019
    # 20220801~20220831
    # 预测自然量付费总金额：281
    # 预测null付费总金额：40
    # AF付费总金额：91945
    # skan付费总金额：67456
    # 总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：0.7371472075697428
    # 20220901~20220930
    # 预测自然量付费总金额：166
    # 预测null付费总金额：139
    # AF付费总金额：61311
    # skan付费总金额：41705
    # 总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：0.6851951525827339



    # print(getSkanCvUsd('20220901','20220930'))

    # main2('20220601','20220930',n=7)
    # main2('20220601','20220930',n=14)
    # main2('20220601','20220930',n=28)

    # AFCvAndRealDiff('20220601','20220930')
    AFCvAndRealDiff('20220601','20220630')
    AFCvAndRealDiff('20220701','20220731')
    AFCvAndRealDiff('20220801','20220831')
    AFCvAndRealDiff('20220901','20220930')