# 数数与Af对数
# 目前发现较大周期内（1个月）ss的付费用户数和付费金额均大于AF
# 幅度大概是10%

# 所以想要对一下，争取想办法把数数和AF进行同步

# 思路：
# 确定一个周期，比如2022-06月
# 1、从数数中找到 所有付费用户 uid + 首日付费金额
# 2、从AF找到 所有付费用户 uid + 首日付费金额
# 3、逐一比对，找到差异
# 4、尝试消除差异（比如增加过滤），看是否仍旧有较大差异，如果有，重复上述步骤

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


# 固定写死就是2022-06 Data
def getSsPayUserData():
    url = 'http://bishushukeji.rivergame.net/querySql'
    url += '?token='+ssToken

    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,group_1,"$__Date_Time",cast(coalesce(COUNT(DISTINCT if(( ( "$part_event" IN ( 'order_complete' ) ) ),ta_ev."#user_id")), 0) as double) internal_amount_0,cast(coalesce(SUM(if(( ( "$part_event" IN ( 'order_complete' ) ) ),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((8-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220525) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220525))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('order_complete')) and (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2022-05-31' and '2022-07-01') and ("@vpc_tz_#event_time" >= timestamp '2022-06-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-06-30'))) and ((ta_ev."platform" IN ('appiosglobal')) and (ta_ev."#vp@lifetime_hour" <= 24))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''

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
    # print(len(lines))
    # 多一行头，多一行尾巴
    lines = lines[1:-1]

    # print(lines[0:10])

    ret = []
    for line in lines:
        j = json.loads(line)
        uid = j[0]
        installDate = j[1]
        usd = j[3]['1981-01-01 00:00:00']
        ret.append( 
            {
                'uid':uid,
                'installDate':installDate,
                'usd':usd
            }
        )
    return pd.DataFrame(data = ret)

# 尝试去除openid中uid多的用户
def getSsPayUserData2():
    url = 'http://bishushukeji.rivergame.net/querySql'
    url += '?token='+ssToken

    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,group_1,"$__Date_Time",cast(coalesce(COUNT(DISTINCT if(( ( "$part_event" IN ( 'order_complete' ) ) ),ta_ev."#user_id")), 0) as double) internal_amount_0,cast(coalesce(SUM(if(( ( "$part_event" IN ( 'order_complete' ) ) ),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((8-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*,"@vpc_cluster_tag_20221024_1" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","open_id","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220525) b on a."#user_id"=b."#user_id") a left join (select "#varchar_id" "id",tag_value_num "@vpc_cluster_tag_20221024_1" from user_result_cluster_2 where cluster_name = 'tag_20221024_1') b0 on a."open_id"=b0."id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220525))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('order_complete')) and (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2022-05-31' and '2022-07-01') and ("@vpc_tz_#event_time" >= timestamp '2022-06-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-06-30'))) and ((ta_ev."platform" IN ('appiosglobal')) and (ta_ev."#vp@lifetime_hour" <= 24) and (ta_ev."@vpc_cluster_tag_20221024_1" IN (1)))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''

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
    # print(len(lines))
    # 多一行头，多一行尾巴
    lines = lines[1:-1]

    # print(lines[0:10])

    ret = []
    for line in lines:
        j = json.loads(line)
        uid = j[0]
        installDate = j[1]
        usd = j[3]['1981-01-01 00:00:00']
        ret.append( 
            {
                'uid':uid,
                'installDate':installDate,
                'usd':usd
            }
        )
    return pd.DataFrame(data = ret)

# 尝试用openid的注册时间替代uid的注册时间
def getSsPayUserData3():
    url = 'http://bishushukeji.rivergame.net/querySql'
    url += '?token='+ssToken

    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@usd_amount"), 0) as double) internal_amount_0 from (select *, format_datetime(ta_date_trunc('day',"@vpc_cluster_tag_20221024_3",1),'yyyy-MM-dd') group_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((8-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "@vpc_cluster_tag_20221024_3", "#event_time")) as double) "#vp@smzqbyopenid",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*,"@vpc_cluster_tag_20221024_3","@vpc_cluster_tag_20221025_1" from (select * from (select "#event_name","open_id","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a left join (select "#varchar_id" "id",arbitrary(if(cluster_name = 'tag_20221024_3', tag_value_tm, null)) "@vpc_cluster_tag_20221024_3",arbitrary(if(cluster_name = 'tag_20221025_1', tag_value, null)) "@vpc_cluster_tag_20221025_1" from user_result_cluster_2 where (cluster_name = 'tag_20221024_3') or (cluster_name = 'tag_20221025_1') group by "#varchar_id") b0 on a."open_id"=b0."id")))))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_2) where "#event_date" > 20220524)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2022-05-30' and '2022-07-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-31' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-07-01'))) and ((ta_ev."@vpc_cluster_tag_20221025_1" IN ('app-iosglobal')) and ((ta_ev."@vpc_cluster_tag_20221024_3" >= cast('2022-06-01 00:00:00' as timestamp) AND ta_ev."@vpc_cluster_tag_20221024_3" <= cast('2022-06-30 23:59:59' as timestamp))) and (ta_ev."#vp@smzqbyopenid" <= 24) and (ta_ev."#vp@smzqbyopenid" > 0))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''

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
    # print(len(lines))
    # 多一行头，多一行尾巴
    lines = lines[1:-1]

    # print(lines[0:10])
    # return

    ret = []
    for line in lines:
        j = json.loads(line)
        uid = j[0]
        installDate = j[1]
        usd = j[3]
        ret.append( 
            {
                'uid':uid,
                'installDate':installDate,
                'usd':usd
            }
        )
    return pd.DataFrame(data = ret)


def getAfPayUserData():
    sql = '''
        select
            customer_user_id as uid,
            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date,
            sum(event_revenue_usd) as usd
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            and event_timestamp-install_timestamp<=24*3600
            and event_name='af_purchase'
            and zone=0
            and day>=20220529 and day<=20220702
            and install_time > "2022-06-01" and install_time <= "2022-06-30 23:59:59"
        group by 
            customer_user_id,
            install_date
        ;
    '''
    df = execSql(sql)
    return df


def analyze(ssDf,afDf):
    # ssDf = pd.read_csv(getFilename('ss202206'))
    # afDf = pd.read_csv(getFilename('af202206'))
    
    # 总数量差异
    lenSs = len(ssDf)
    lenAf = len(afDf)
    print('len ss:%d,len af:%d,(ss-af)/ss:%.2f'%(lenSs,lenAf,(lenSs - lenAf)/lenSs))

    # uid差异
    # 交集
    mergeDf = pd.merge(ssDf,afDf,on=['uid'])
    lenMerge = len(mergeDf)
    print('len merge:%d,merge/af:%.2f'%(lenMerge,(lenMerge/lenAf)))
    print('len merge:%d,merge/ss:%.2f'%(lenMerge,(lenMerge/lenSs)))
    # 差集
    dfTmp = pd.concat([ssDf, afDf, afDf])
    dfTmp=dfTmp.drop_duplicates(subset=['uid'],keep=False)
    dfTmp.to_csv(getFilename('ss-afUid'))
    print('')

    dfTmp = pd.concat([afDf, ssDf, ssDf])
    dfTmp=dfTmp.drop_duplicates(subset=['uid'],keep=False)
    dfTmp.to_csv(getFilename('af-ssUid'))
    print('')

    # 相同uid的注册时间是否相同，精确到日


# 存在一些数数上 原始平台 为googleplay的，应该是注册时候实在google的，后来又用ios了
# uid：798056765565

def getUserInfoFromAF():
    sql = '''
        select
            customer_user_id as uid,
            to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date,
            sum(event_revenue_usd) as usd
        from ods_platform_appsflyer_events
        where
            app_id='id1479198816'
            -- and event_timestamp-install_timestamp<=24*3600
            and event_name='af_purchase'
            and zone=0
            and day>=20220501 and day<=20220730
            and customer_user_id in (803841676739,797845928041,813321078077,801022347373,798027294792,811247999133,808738977933,813865789591,811727141021,808965851267,813343873175,809250965635,808846899153,799579756652,800392386669,810629760586,802809776238,810407720603,802272408810,805820801959)
        group by 
            customer_user_id,
            install_date
        ;
    '''
    df = execSql(sql)
    print(len(df),df)

if __name__ == '__main__':
    # df = getSsPayUserData()
    # df.to_csv(getFilename('ss202206'))

    # df = getSsPayUserData3()
    # df.to_csv(getFilename('ss202206-3'))

    # df = getAfPayUserData()
    # df.to_csv(getFilename('af202206'))   
     
    ssDf = pd.read_csv(getFilename('ss202206-3'))
    afDf = pd.read_csv(getFilename('af202206'))

    analyze(ssDf,afDf)
    # getUserInfoFromAF()