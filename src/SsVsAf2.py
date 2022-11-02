# 尝试将数数与AF的数进行完全匹配
# 方案是查询数数与AF的所有付费事件，然后针对时间相差不大，金额一致的事件进行重打点，并加入af属性：af id 与 af install 时间
# 然后用af属性进行重统计
# af属性统计应该可以和af数据完全对应
# af属性统计再和数数其他支付数据做覆盖率比对
# 本质上覆盖率应该是足够高的，因为流水是能对的上的

from ossaudiodev import SNDCTL_DSP_GETSPDIF
import sys
sys.path.append('/src')
from src.config import ssToken
from src.tools import getFilename

from src.maxCompute import execSql

import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json

import pandas as pd
import datetime

from tgasdk.sdk import TGAnalytics, BatchConsumer

# 开始日期与结束日期，utc0，自然日
# sinceTime = datetime.date(2022,6,1)
# unitlTime = datetime.date(2022,6,30)


def getSsPayUserData(sinceTime,unitlTime):
    # global sinceTime,unitlTime
    # 数数的日期需求格式是%Y-%m-%d
    sinceTimeStr = sinceTime.strftime('%Y-%m-%d')
    unitlTimeStr = unitlTime.strftime('%Y-%m-%d')
    t0 = (sinceTime+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
    t1 = (unitlTime+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    t_1 = (sinceTime+datetime.timedelta(days=-7)).strftime('%Y%m%d')

    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@usd_amount"), 0) as double) internal_amount_0 from (select *, "order_id" group_1,format_datetime(ta_date_trunc('minute',"@vpc_tz_#event_time",1),'yyyy-MM-dd HH:mm') group_2 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select * from (select "#event_name","paypf","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id")))))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_2) where "#event_date" > 20210725)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2021-07-31' and '2021-10-04') and ("@vpc_tz_#event_time" >= timestamp '2021-08-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2021-10-03'))) and ((ta_ev."paypf" NOT IN ('webgameglobal')) and (ta_ev."platform" IN ('appiosglobal')))) group by group_0,group_1,group_2,"$__Date_Time")) group by group_0,group_1,group_2)) ORDER BY total_amount DESC
    '''
    sql = sql.replace('2021-08-01','__since__')
    sql = sql.replace('2021-10-03','__unitl__')
    sql = sql.replace('2021-07-31','__t0__')
    sql = sql.replace('2021-10-04','__unitl1__')
    sql = sql.replace('20210725','__since_1__')

    sql = sql.replace('__since__',sinceTimeStr)
    sql = sql.replace('__unitl__',unitlTimeStr)
    sql = sql.replace('__t0__',t0)
    sql = sql.replace('__unitl1__',t1)
    sql = sql.replace('__since_1__',t_1)
    

    url = 'http://bishushukeji.rivergame.net/querySql'
    url += '?token='+ssToken

    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
    # 通过字典方式定义请求body
    formData = {"sql": sql, "format": 'json'}
    data = parse.urlencode(formData)
    # 请求方式
    s = requests.Session()
    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
    s.mount('https://',HTTPAdapter(max_retries=3))
    # 由于事件可能会比较长，暂时不设置timeout
    r = s.post(url=url, headers=headers, data=data)
    # print(r.text)
    lines = r.text.split('\n')
    # 多一行头，多一行尾巴
    lines = lines[1:-1]

    ret = []
    for line in lines:
        j = json.loads(line)
        uid = j[0]
        orderId = j[1]
        eventTime = j[2]
        usd = j[4]
        if usd == 0:
            continue
        ret.append( 
            {
                'uid':uid,
                'orderId':orderId,
                'eventTime':eventTime,
                'usd':usd
            }
        )
    return pd.DataFrame(data = ret)

def getAfPayUserData(sinceTime,unitlTime):
    # global sinceTime,unitlTime
    # 数数的日期需求格式是%Y-%m-%d
    sinceTimeStr = sinceTime.strftime('%Y%m%d')
    unitlTimeStr = unitlTime.strftime('%Y%m%d')
    sql = '''
        select
            customer_user_id as uid,
            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as af_install_date,
            to_char(to_date(event_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd hh:mi") as af_event_time,
            event_revenue_usd,
            appsflyer_id
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_name in ('af_purchase','af_purchase_oldusers')
            and zone=0
            and day>=%s and day<=%s
        ;
    '''%(sinceTimeStr,unitlTimeStr)
    df = execSql(sql)
    return df

# 直接获得订单id
def getAfPayUserDataWithOrderId(sinceTime,unitlTime):
    # global sinceTime,unitlTime
    # 数数的日期需求格式是%Y-%m-%d
    sinceTimeStr = sinceTime.strftime('%Y%m%d')
    unitlTimeStr = unitlTime.strftime('%Y%m%d')
    sql = '''
        select
            customer_user_id as uid,
            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as af_install_date,
            to_char(to_date(event_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd hh:mi") as af_event_time,
            event_revenue_usd,
            appsflyer_id,
            get_json_object(base64decode(event_value),'$.af_order_id') as order_id
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_name in ('af_purchase','af_purchase_oldusers')
            and zone=0
            and day>=%s and day<=%s
        ;
    '''%(sinceTimeStr,unitlTimeStr)
    df = execSql(sql)
    return df

def matchSsAndAf(ssDf,afDf):
    ssDf.insert(ssDf.shape[1],'afId','unknown')
    ssDf.insert(ssDf.shape[1],'afInstallDate','-')
    # 每条只能match一次
    afDf.insert(afDf.shape[1],'isMatched',0)

    ssDf = ssDf.sort_values(by=['uid','eventTime'])

    orderIds = ssDf['orderId'].unique()
    orderCount = len(orderIds)
    notMatchedCount = 0
    
    i = 0
    # 用order来做容易出现冒领情况，由于时间与金额都是范围值，所以防止冒领的简单方式是按照时间顺序来做
    for orderId in orderIds:
        i += 1
        df = ssDf.loc[ssDf.orderId == orderId]
        uid = df.iloc[0]['uid']
        eventTimeStr = df.iloc[0]['eventTime']
        usd = df.iloc[0]['usd']
        eventTime = datetime.datetime.strptime(eventTimeStr,'%Y-%m-%d %H:%M')
        eventTimeMinStr = (eventTime + datetime.timedelta(minutes = -1)).strftime('%Y-%m-%d %H:%M')
        eventTimeMaxStr = (eventTime + datetime.timedelta(minutes = +1)).strftime('%Y-%m-%d %H:%M')
        usdMin = usd * 0.85
        usdMax = usd * 1.15

        # find from afDf
        afDfFind = afDf[
            (afDf.uid == uid) &
            (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
            (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
            (afDf.isMatched == 0)
        ]

        if len(afDfFind) > 0:
            ssDf.loc[ssDf.orderId==orderId,'afId'] = afDfFind.iloc[0]['appsflyer_id']
            ssDf.loc[ssDf.orderId==orderId,'afInstallDate'] = afDfFind.iloc[0]['af_install_date']
            index = list(afDfFind.index)[0]
            # print(afDfFind)
            afDf.loc[index,'isMatched'] = 1
            # afDf.loc[
            #     (afDf.uid == uid) &
            #     (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
            #     (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
            #     (afDf.isMatched == 0),'isMatched'
            # ] = 1
        else:
            print('not match:',ssDf.iloc[[i]])
            notMatchedCount += 1
            print('not matched/total:%d/%d = %.2f%%'%(notMatchedCount,i, (notMatchedCount/i*100)))
            # print(df)
            if notMatchedCount >= 100:
                break
    return ssDf,afDf
        
def matchSsAndAf2(ssDf,afDf):
    ssDf.insert(ssDf.shape[1],'afId','unknown')
    ssDf.insert(ssDf.shape[1],'afInstallDate','-')
    # 每条只能match一次
    afDf.insert(afDf.shape[1],'isMatched',0)

    orderIds = ssDf['orderId'].unique()
    orderCount = len(orderIds)
    notMatchedCount = 0
    
    # 用order来做容易出现冒领情况，由于时间与金额都是范围值，所以防止冒领的简单方式是按照时间顺序来做
    # for orderId in orderIds:
    #     df = ssDf.loc[ssDf.orderId == orderId]
    #     uid = df.iloc[0]['uid']
    #     eventTimeStr = df.iloc[0]['eventTime']
    #     usd = df.iloc[0]['usd']

    ssDf = ssDf.sort_values(by=['uid','eventTime'])
    for i in range(len(ssDf)):
        uid = ssDf.iloc[i]['uid']
        eventTimeStr = ssDf.iloc[i]['eventTime']
        eventTime = datetime.datetime.strptime(eventTimeStr,'%Y-%m-%d %H:%M')
        eventTimeMinStr = (eventTime + datetime.timedelta(minutes = -1)).strftime('%Y-%m-%d %H:%M')
        eventTimeMaxStr = (eventTime + datetime.timedelta(minutes = +1)).strftime('%Y-%m-%d %H:%M')
        usd = ssDf.iloc[i]['usd']
        usdMin = usd * 0.85
        usdMax = usd * 1.15

        # find from afDf
        afDfFind = afDf[
            (afDf.uid == uid) &
            (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
            (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
            (afDf.isMatched == 0)
        ]

        if len(afDfFind) > 0:
            # print(ssDf.loc[i])
            ssDf.loc[i,'afId'] = afDfFind.iloc[0]['appsflyer_id']
            ssDf.loc[i,'afInstallDate'] = afDfFind.iloc[0]['af_install_date']
            # print(ssDf.loc[i])
            index = list(afDfFind.index)[0]
            # print(afDfFind)
            afDf.loc[index,'isMatched'] = 1
            # afDf.loc[
            #     (afDf.uid == uid) &
            #     (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
            #     (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
            #     (afDf.isMatched == 0),'isMatched'
            # ] = 1
        else:
            print('not match:',ssDf.iloc[[i]])
            notMatchedCount += 1
            print('not matched/total:%d/%d = %.2f%%'%(notMatchedCount,i, (notMatchedCount/i*100)))
            # print(df)
            if notMatchedCount >= 100:
                break
    return ssDf,afDf

def matchSsAndAf3(ssDf,afDf):
    ssDf.insert(ssDf.shape[1],'afId','unknown')
    ssDf.insert(ssDf.shape[1],'afInstallDate','-')
    # 每条只能match一次
    afDf.insert(afDf.shape[1],'isMatched',0)

    orderIds = ssDf['orderId'].unique()
    orderCount = len(orderIds)
    notMatchedCount = 0
    

    ssDf = ssDf.sort_values(by=['uid','eventTime','usd']).reset_index(drop=True)
    afDf = afDf.sort_values(by=['uid','af_event_time','event_revenue_usd']).reset_index(drop=True)

    uids = ssDf['uid'].unique()
    count = 0
    for uid in uids:
        ssUidDf = ssDf.loc[ssDf.uid == uid]
        afUidDf = afDf.loc[afDf.uid == uid]

        # print('ssUidDf:',ssUidDf)
        # print('afUidDf:',afUidDf)
        
        if (len(ssUidDf) != len(afUidDf)):
            print('uid:%s ssOrdersCount:%d != afOrdersCount:%d'%(uid,len(ssUidDf),len(afUidDf)))
            # continue

        for i in range(len(ssUidDf)):
            count += 1
            ssIndex = list(ssUidDf.iloc[[i]].index)[0]
            eventTimeStr = ssDf.iloc[ssIndex]['eventTime']
            usd = ssDf.iloc[ssIndex]['usd']
            eventTime = datetime.datetime.strptime(eventTimeStr,'%Y-%m-%d %H:%M')
            eventTimeMinStr = (eventTime + datetime.timedelta(minutes = -1)).strftime('%Y-%m-%d %H:%M')
            eventTimeMaxStr = (eventTime + datetime.timedelta(minutes = +1)).strftime('%Y-%m-%d %H:%M')
            usdMin = usd * 0.85
            usdMax = usd * 1.15

            # find from afDf
            afDfFind = afDf[
                (afDf.uid == uid) &
                (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
                (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
                (afDf.isMatched == 0)
            ]
            # print(uid,eventTimeStr,usd)
            if len(afDfFind) > 0:
                ssDf.loc[ssIndex,'afId'] = afDfFind.iloc[0]['appsflyer_id']
                ssDf.loc[ssIndex,'afInstallDate'] = afDfFind.iloc[0]['af_install_date']
                afIndex = list(afDfFind.index)[0]
                # print('afDfFind:',afDfFind)
                # print('afIndex:',afIndex)
                # print('BBB:',afDf.loc[afIndex])
                afDf.loc[afIndex,'isMatched'] = 1
                # print('AAA:',afDf.loc[afIndex])
            else:
                # print('not match:',ssDf.iloc[[ssIndex]])
                notMatchedCount += 1
                print('not matched/total:%d/%d = %.2f%%'%(notMatchedCount,count, (notMatchedCount/count*100)))
                # print(df)
    return ssDf,afDf


# 直接用数数与Af的orderId做匹配
def matchSsAndAfByOrderId(ssDf,afDf):
    afDf = afDf.rename(columns={'order_id':'orderId'})
    ssDf[['orderId']] = ssDf[['orderId']].astype(str)
    afDf[['orderId']] = afDf[['orderId']].astype(str)
    
    ssRet = ssDf.merge(afDf,how='left',on='orderId')

    ssLoss = ssRet.loc[pd.isna(ssRet.af_install_date)]
    print('ss能在af中找到对应订单的比例：%.2f%%'%(len(ssLoss)/len(ssRet)*100))

    ssDf2 = pd.DataFrame({'orderId':ssDf.loc[:,'orderId']})
    ssDf2.insert(ssDf2.shape[1],'isMatched',1)
    # print(ssDf2)
    afRet = afDf.merge(ssDf2,how='left',on='orderId')
    # print(afRet)
    afLoss = afRet.loc[pd.isna(afRet.isMatched)]
    print('af能在ss中找到对应订单的比例：%.2f%%'%(len(afLoss)/len(afRet)*100))

    merge = ssDf.merge(afDf,how='inner',on='orderId')
    # print('merge:',merge)
    ssTotalUsd = merge['usd'].sum()
    afTotalUsd = merge['event_revenue_usd'].sum()
    print('ssTotalUsd:%f,afTotalUsd:%f,(ssTotalUsd/afTotalUsd)=%.2f%%'%(ssTotalUsd,afTotalUsd,(ssTotalUsd/afTotalUsd)*100))

    return ssRet


def main(sinceTime,unitlTime):
    print(sinceTime,unitlTime)
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        afDf = getAfPayUserDataWithOrderId(sinceTime,unitlTime)
        afDf.to_csv(getFilename('getAfOrderData'))
        
        ssDf = getSsPayUserData(sinceTime,unitlTime)
        ssDf.to_csv(getFilename('getSsOrderData'))

    afDf = pd.read_csv(getFilename('getAfOrderData'))
    ssDf = pd.read_csv(getFilename('getSsOrderData'))

    print('got af order data :%d'%(len(afDf)))
    print('got ss order data :%d'%(len(ssDf)))

    afTotalUsd = afDf['event_revenue_usd'].sum()
    ssTotalUsd = ssDf['usd'].sum()
    print('ssTotalUsd:%f,afTotalUsd:%f,(ssTotalUsd/afTotalUsd)=%.2f%%'%(ssTotalUsd,afTotalUsd,(ssTotalUsd/afTotalUsd)*100))

    # 尝试进行match
    afDf = afDf.rename(columns={'order_id':'orderId'})
    ssDf[['orderId']] = ssDf[['orderId']].astype(str)
    afDf[['orderId']] = afDf[['orderId']].astype(str)
    
    ssRet = ssDf.merge(afDf,how='left',on='orderId')
    ssLoss = ssRet.loc[pd.isna(ssRet.af_install_date)]
    print('ss在af中找不到对应订单的比例：（%d/%d） = %.2f%%'%(len(ssLoss),len(ssRet),len(ssLoss)/len(ssRet)*100))

    ssDf2 = pd.DataFrame({'orderId':ssDf.loc[:,'orderId']})
    ssDf2.insert(ssDf2.shape[1],'isMatched',1)
    afRet = afDf.merge(ssDf2,how='left',on='orderId')
    afLoss = afRet.loc[pd.isna(afRet.isMatched)]
    print('af在ss中找不到对应订单的比例：（%d/%d） = %.2f%%'%(len(afLoss),len(afRet),len(afLoss)/len(afRet)*100))

    merge = ssDf.merge(afDf,how='inner',on='orderId')
    ssTotalUsd = merge['usd'].sum()
    afTotalUsd = merge['event_revenue_usd'].sum()
    print('在能匹配的订单中金额差异 ssTotalUsd:%f,afTotalUsd:%f,(ssTotalUsd/afTotalUsd)=%.2f%%'%(ssTotalUsd,afTotalUsd,(ssTotalUsd/afTotalUsd)*100))

    # 将匹配到af订单的支付数据重新打点给BI
    # uri = 'https://tatracker.rivergame.net/'
    # appid = 'cf7a0712b2e44e4882973fa137969fff'
    # batchConsumer = BatchConsumer(server_uri=uri, appid=appid,compress=False)
    # ta = TGAnalytics(batchConsumer)
    # event_name = 'afPurchase'
    # successCount = 0
    # for i in range(len(merge)):
    #     account_id = str(merge['uid_x'].get(i))
    #     time = datetime.datetime.strptime(merge['eventTime'].get(i),'%Y-%m-%d %H:%M')
    #     orderId = str(merge['orderId'].get(i))
    #     usd = float(merge['usd'].get(i))
    #     afUsd = float(merge['event_revenue_usd'].get(i))
    #     afId = str(merge['appsflyer_id'].get(i))
    #     afInstallDate = merge['af_install_date'].get(i)
        
    #     # print(type(orderId))
    #     # print(type(usd))
    #     # print(type(afUsd))
    #     # print(type(afId))
    #     # print(type(afInstallDate))

    #     properties = {
    #         "#time":time,
    #         "orderId":orderId,
    #         "usd":usd,
    #         "afId":afId,
    #         "afUsd":afUsd,
    #         "afInstallDate":afInstallDate
    #     }
    #     try:
    #         ta.track(account_id = account_id, event_name = event_name, properties = properties)
    #         successCount += 1
    #     except Exception as e:
    #         print(e)  
    # ta.flush()
    # print('发送事件成功:',successCount)
    # ta.close()

def test():
    afDf = pd.read_csv(getFilename('getAfOrderData'))
    ssDf = pd.read_csv(getFilename('getSsOrderData'))

    # 尝试进行match
    afDf = afDf.rename(columns={'order_id':'orderId'})
    ssDf[['orderId']] = ssDf[['orderId']].astype(str)
    afDf[['orderId']] = afDf[['orderId']].astype(str)
    
    ssDf2 = pd.DataFrame({'orderId':ssDf.loc[:,'orderId']})
    ssDf2.insert(ssDf2.shape[1],'isMatched',1)
    afRet = afDf.merge(ssDf2,how='left',on='orderId')
    afLoss = afRet.loc[pd.isna(afRet.isMatched)]
    print('af在ss中找不到对应订单的比例：%.2f%%'%(len(afLoss)/len(afRet)*100))
    print(afLoss)

    ssRet = ssDf.merge(afDf,how='left',on='orderId')
    ssLoss = ssRet.loc[pd.isna(ssRet.af_install_date)]
    print('ss在af中找不到对应订单的比例：%.2f%%'%(len(ssLoss)/len(ssRet)*100))
    print(ssLoss)

if __name__ == '__main__':
    sinceTime = datetime.date(2022,7,1)
    unitlTime = datetime.date(2022,7,31)

    main(sinceTime,unitlTime)

    # test()