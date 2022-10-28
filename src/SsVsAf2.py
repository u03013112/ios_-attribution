# 尝试将数数与AF的数进行完全匹配
# 方案是查询数数与AF的所有付费事件，然后针对时间相差不大，金额一致的事件进行重打点，并加入af属性：af id 与 af install 时间
# 然后用af属性进行重统计
# af属性统计应该可以和af数据完全对应
# af属性统计再和数数其他支付数据做覆盖率比对
# 本质上覆盖率应该是足够高的，因为流水是能对的上的

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


# 开始日期与结束日期，utc0，自然日
sinceTime = datetime.date(2022,6,1)
unitlTime = datetime.date(2022,6,2)


def getSsPayUserData():
    global sinceTime,unitlTime
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

def getAfPayUserData():
    global sinceTime,unitlTime
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


def matchSsAndAf(ssDf,afDf):
    ssDf.insert(ssDf.shape[1],'afId','unknown')
    ssDf.insert(ssDf.shape[1],'afInstallDate','-')
    # 每条只能match一次
    afDf.insert(afDf.shape[1],'isMatched',0)

    orderIds = ssDf['orderId'].unique()
    orderCount = len(orderIds)
    notMatchedCount = 0
    for orderId in orderIds:
        df = ssDf.loc[ssDf.orderId == orderId]
        uid = df.iloc[0]['uid']
        eventTimeStr = df.iloc[0]['eventTime']
        eventTime = datetime.datetime.strptime(eventTimeStr,'%Y-%m-%d %H:%M')
        eventTimeMinStr = (eventTime + datetime.timedelta(minutes = -1)).strftime('%Y-%m-%d %H:%M')
        eventTimeMaxStr = (eventTime + datetime.timedelta(minutes = +1)).strftime('%Y-%m-%d %H:%M')
        # print(eventTimeStr,eventTimeMinStr,eventTimeMaxStr)
        usd = df.iloc[0]['usd']
        usdMin = usd * 0.85
        usdMax = usd * 1.15
        # print(usd,usdMin,usdMax)
        
        # print(df)
        # print(afDf.loc[(afDf.uid == uid)])
        # break

        # find from afDf
        afDfFind = afDf[
            (afDf.uid == uid) &
            (afDf.af_event_time >= eventTimeMinStr) & (afDf.af_event_time <= eventTimeMaxStr) &
            (afDf.event_revenue_usd >= usdMin) & (afDf.event_revenue_usd <= usdMax) &
            (afDf.isMatched == 0)
        ]

        if len(afDfFind) > 0:
            # 可能会有更好的写法吧，暂时只会这么写，写得好啰嗦
            ssDf.loc[ssDf.orderId == orderId,'afId'] = afDfFind.iloc[0]['appsflyer_id']
            ssDf.loc[ssDf.orderId == orderId,'afInstallDate'] = afDfFind.iloc[0]['af_install_date']
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
            # print('order id:%s not match!'%(orderId))
            notMatchedCount += 1
            print('not matched/total:%d/%d = %.2f%%'%(notMatchedCount,orderCount, (notMatchedCount/orderCount*100)))
            # print(df)
            # if notMatchedCount >= 100:
            #     break
    return ssDf,afDf
        

if __name__ == '__main__':
    # df = getSsPayUserData()
    # df.to_csv(getFilename('getSsPayUserData202206'))

    # df = getAfPayUserData()
    # df.to_csv(getFilename('getAfPayUserData202206'))
    
    ssDf = pd.read_csv(getFilename('getSsPayUserData202206'))
    afDf = pd.read_csv(getFilename('getAfPayUserData202206'))
    ssDf,afDf = matchSsAndAf(ssDf, afDf)
    ssDf.to_csv(getFilename('getSsPayUserData202206-b'))
    afDf.to_csv(getFilename('getAfPayUserData202206-b'))