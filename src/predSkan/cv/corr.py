# 尝试获得属性与付费金额（r7usd）的关联性

# 直接从数数获得 uid installDate r7usd 欲测试属性（首日）
# 大致获得5月到11月数据即可
# 将数据放入pandas，然后算关联性
# 或者尝试分开查询，最后用uid做left join？
import json
import time
import requests
from urllib import parse
from requests.adapters import HTTPAdapter

import pandas as pd

import sys
sys.path.append('/src')

from src.config import ssToken
from src.tools import getFilename,printProgressBar

def ssSql(sql):
    # url = 'http://bishushukeji.rivergame.net/querySql'
    url = 'http://123.56.188.109/querySql'
    url += '?token='+ssToken+'&timeoutSeconds=1800'+'&timeoutSecond=1800'
    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
    # 通过字典方式定义请求body
    formData = {"sql": sql, "format": 'json','timeoutSeconds':1800,'timeoutSecond':1800}
    data = parse.urlencode(formData)
    # 请求方式
    s = requests.Session()
    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
    s.mount('https://',HTTPAdapter(max_retries=3))
    # 由于事件可能会比较长，暂时不设置timeout
    r = s.post(url=url, headers=headers, data=data)
    # print(r.text)
    lines = r.text.split('\n')
    # print(lines[0])
    # 多一行头，多一行尾巴
    lines = lines[1:-1]
    return lines
    
# 异步分页
def ssSql2(sql):
    url = 'http://123.56.188.109/open/submit-sql'
    url += '?token='+ssToken

    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
    # 通过字典方式定义请求body
    formData = {"sql": sql, "format": 'json','pageSize':'100000'}
    data = parse.urlencode(formData)
    # 请求方式
    s = requests.Session()
    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
    s.mount('https://',HTTPAdapter(max_retries=3))
    
    r = s.post(url=url, headers=headers, data=data)
    try:
        j = json.loads(r.text)
        taskId = j['data']['taskId']
        print('taskId:',taskId)
    except Exception:
        print(r.text)
        return
    # taskId = '164f8187879e3000'
    printProgressBar(0, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
    for _ in range(60):
        time.sleep(10)
        url2 = 'http://123.56.188.109/open/sql-task-info'
        url2 += '?token='+ssToken+'&taskId='+taskId
        s = requests.Session()
        s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
        s.mount('https://',HTTPAdapter(max_retries=3))
        r = s.get(url=url2)
        try:
            j = json.loads(r.text)
            status = j['data']['status']
            if status == 'FINISHED':
                rowCount = j['data']['resultStat']['rowCount']
                pageCount = j['data']['resultStat']['pageCount']
                print('rowCount:',rowCount,'pageCount:',pageCount)
                # print(j)
                lines = []
                for p in range(pageCount):
                    url3 = 'http://123.56.188.109/open/sql-result-page'
                    url3 += '?token='+ssToken+'&taskId='+taskId+'&pageId=%d'%p
                    s = requests.Session()
                    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
                    s.mount('https://',HTTPAdapter(max_retries=3))
                    r = s.get(url=url3)
                    lines += r.text.split('\n')
                    # print('page:%d/%d'%(p,pageCount),'lines:',len(lines))
                    printProgressBar(p, pageCount, prefix = 'Progress:', suffix = 'page', length = 50)
                return lines
            else:
                # print('progress:',j['data']['progress'])
                printProgressBar(j['data']['progress'], 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            print(e)
            print(r.text)
            continue
        # 查询太慢了，多等一会再尝试
        time.sleep(10)


# uid,install_date,r1usd,r7usd
# df.to_csv(getFilename('baseDataFromSS'))
def getBaseDataFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_2 amount_2 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1,arbitrary(internal_amount_2) internal_amount_2 from (select group_0,group_1,"$__Date_Time",cast(coalesce(approx_distinct(ta_ev."#user_id",0.0040625), 0) as double) internal_amount_0,null internal_amount_1,null internal_amount_2 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'create_account' ) ) )) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime" <= 1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime" <= 7),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_2 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try((date_diff('day', date("internal_u@#reg_time"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."#reg_time" "internal_u@#reg_time" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","#reg_time" from v_user_2) where "#event_date" > 20220424) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('order_complete')) and (((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime" <= 1)) or ((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime" <= 7))) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql)
    # print(len(lines))
    # print(lines[0:100])
    uidList = []
    installDateList = []
    r1usdList = []
    r7usdList = []

    for line in lines:
        j = json.loads(line)
        uid = j[0]
        installDate = j[1]
        if j[3] == None:
            r1usd = 0
        else:
            r1usd = list(j[3].values())[0]
        if j[4] == None:
            r7usd = 0
        else:
            r7usd = list(j[4].values())[0]
        
        # print(uid,installDate,r1usd,r7usd)
        uidList.append(uid)
        installDateList.append(installDate)
        r1usdList.append(r1usd)
        r7usdList.append(r7usd)

    return pd.DataFrame(data = {
        'uid':uidList,
        'install_date':installDateList,
        'r1usd':r1usdList,
        'r7usd':r7usdList
    })

# uid,install_date,otime
# df.to_csv(getFilename('otimeDataFromSS'))
def getOtimeDataFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select group_0,group_1,"$__Date_Time",cast(coalesce(approx_distinct(ta_ev."#user_id",0.0040625), 0) as double) internal_amount_0,null internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'create_account' ) ) )) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(SUM(ta_ev."#duration"), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try((date_diff('day', date("internal_u@#reg_time"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."#reg_time" "internal_u@#reg_time" from (select "#event_name","#duration","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","#reg_time" from v_user_2) where "#event_date" > 20220424) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@lifetime" IN (1))) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    print(lines[0:10])
    uidList = []
    installDateList = []
    otimeList = []
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            print(line)
            continue
        uid = j[0]
        installDate = j[1]
        if j[3] == None:
            otime = 0
        else:
            otime = list(j[3].values())[0]
        uidList.append(uid)
        installDateList.append(installDate)
        otimeList.append(otime)
    return pd.DataFrame(data = {
        'uid':uidList,
        'install_date':installDateList,
        'otime':otimeList
    })

# uid,install_date,level
def getLevelDataFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select group_0,group_1,"$__Date_Time",cast(coalesce(approx_distinct(ta_ev."#user_id",0.0040625), 0) as double) internal_amount_0,null internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'create_account' ) ) )) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(1), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try((date_diff('day', date("internal_u@#reg_time"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."#reg_time" "internal_u@#reg_time" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","#reg_time" from v_user_2) where "#event_date" > 20220424) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'user_levelup' ) ) )) and (ta_ev."#vp@lifetime" IN (1))) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(lines[0:10])
    uidList = []
    installDateList = []
    levelList = []
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            print(line)
            continue
        uid = j[0]
        installDate = j[1]
        if j[3] == None:
            level = 0
        else:
            level = list(j[3].values())[0]
        uidList.append(uid)
        installDateList.append(installDate)
        levelList.append(level)
    return pd.DataFrame(data = {
        'uid':uidList,
        'install_date':installDateList,
        'level':levelList
    })

# uid,install_date,level
def getLaunchDataFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select group_0,group_1,"$__Date_Time",cast(coalesce(approx_distinct(ta_ev."#user_id",0.0040625), 0) as double) internal_amount_0,null internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'create_account' ) ) )) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(1), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try((date_diff('day', date("internal_u@#reg_time"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."#reg_time" "internal_u@#reg_time" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","#reg_time" from v_user_2) where "#event_date" > 20220424) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220424))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'app_launch' ) ) )) and (ta_ev."#vp@lifetime" IN (1))) and ((("$part_date" between '2022-04-30' and '2022-12-01') and ("@vpc_tz_#event_time" >= timestamp '2022-05-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-11-30'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(lines[0:10])
    uidList = []
    installDateList = []
    launchList = []
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            print(line)
            continue
        uid = j[0]
        installDate = j[1]
        if j[3] == None:
            launch = 0
        else:
            launch = list(j[3].values())[0]
        uidList.append(uid)
        installDateList.append(installDate)
        launchList.append(launch)
    return pd.DataFrame(data = {
        'uid':uidList,
        'install_date':installDateList,
        'launch':launchList
    })


if __name__ == '__main__':
    df = getLevelDataFromSS()
    df.to_csv(getFilename('levelDataFromSS'))

