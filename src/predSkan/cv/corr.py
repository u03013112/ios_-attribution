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
                    printProgressBar(p, pageCount-1, prefix = 'Progress:', suffix = 'page', length = 50)
                return lines
            else:
                # print('progress:',j['data']['progress'])
                printProgressBar(j['data']['progress'], 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            print('e:',e)
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
# df.to_csv(getFilename('levelDataFromSS'))
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
# df.to_csv(getFilename('launchDataFromSS'))
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

# 尝试将上面数据都合并到一起
# def merge():

# 从ss那边获得的数据做一个简单的整理
def getDataFromDict(d,default = 0):
    ret = default
    if d != None:
        ret = list(d.values())[0]
    return ret
        

# 干脆直接获取所有属性，省的再merge
# df.to_csv(getFilename('allDataFromSS'))
def getAllDataFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2,count(data_map_3) over () group_num_3,count(data_map_4) over () group_num_4,count(data_map_5) over () group_num_5,count(data_map_6) over () group_num_6,count(data_map_7) over () group_num_7,count(data_map_8) over () group_num_8,count(data_map_9) over () group_num_9 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", amount_3) filter (where amount_3 is not null and is_finite(amount_3) ) data_map_3,map_agg("$__Date_Time", amount_4) filter (where amount_4 is not null and is_finite(amount_4) ) data_map_4,map_agg("$__Date_Time", amount_5) filter (where amount_5 is not null and is_finite(amount_5) ) data_map_5,map_agg("$__Date_Time", amount_6) filter (where amount_6 is not null and is_finite(amount_6) ) data_map_6,map_agg("$__Date_Time", amount_7) filter (where amount_7 is not null and is_finite(amount_7) ) data_map_7,map_agg("$__Date_Time", amount_8) filter (where amount_8 is not null and is_finite(amount_8) ) data_map_8,map_agg("$__Date_Time", amount_9) filter (where amount_9 is not null and is_finite(amount_9) ) data_map_9,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_2 amount_2,internal_amount_3 amount_3,internal_amount_4 amount_4,internal_amount_5 amount_5,internal_amount_6 amount_6,internal_amount_7 amount_7,internal_amount_8 amount_8,internal_amount_9 amount_9 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1,arbitrary(internal_amount_2) internal_amount_2,arbitrary(internal_amount_3) internal_amount_3,arbitrary(internal_amount_4) internal_amount_4,arbitrary(internal_amount_5) internal_amount_5,arbitrary(internal_amount_6) internal_amount_6,arbitrary(internal_amount_7) internal_amount_7,arbitrary(internal_amount_8) internal_amount_8,arbitrary(internal_amount_9) internal_amount_9 from (select group_0,group_1,"$__Date_Time",cast(coalesce(approx_distinct(ta_ev."#user_id",0.0040625), 0) as double) internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_7,null internal_amount_8,null internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'create_account' ) ) )) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,cast(coalesce(COUNT(1), 0) as double) internal_amount_5,null internal_amount_6,null internal_amount_7,null internal_amount_8,null internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try((date_diff('day', date("internal_u@#reg_time"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."#reg_time" "internal_u@#reg_time" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","#reg_time" from v_user_2) where "#event_date" > 20220425) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'app_launch' ) ) )) and (ta_ev."#vp@lifetime" < 24)) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,cast(coalesce(SUM(ta_ev."#duration"), 0) as double) internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_7,null internal_amount_8,null internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#duration","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220425) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24)) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime_hour" < 168),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_7,null internal_amount_8,null internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220425) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('order_complete')) and (((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24)) or ((( ( "$part_event" IN ( 'order_complete' ) ) )) and (ta_ev."#vp@lifetime_hour" < 168))) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,cast(coalesce(COUNT(1), 0) as double) internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_7,null internal_amount_8,null internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220425) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'user_levelup' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24)) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,cast(coalesce(MAX(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24),ta_ev."#vp@lifetime_sec")), 0) as double) internal_amount_6,cast(coalesce(MIN(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24),ta_ev."#vp@lifetime_sec")), 0) as double) internal_amount_7,cast(coalesce(MIN(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_8,cast(coalesce(MAX(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_9 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220425) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220425))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('PAY_SUCCESS_REALTIME')) and ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_hour" < 24)) and ((("$part_date" between '2022-05-01' and '2022-12-02') and ("@vpc_tz_#event_time" >= timestamp '2022-05-02' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-12-01'))) and (ta_u."firstplatform" IN ('app-iosglobal'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(lines[0:10])
    uidList = []
    installDateList = []
    r1usdList = []
    r7usdList = []
    otimeList = []
    levelList = []
    launchList = []
    lastPayList = []
    firstPayList = []
    r1usdMinList = []
    r1usdMaxList = []

    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            print(line)
            continue
        uid = j[0]
        installDate = j[1]
        
        r1usd = getDataFromDict(j[3])
        r7usd = getDataFromDict(j[4])
        otime = getDataFromDict(j[5])
        level = getDataFromDict(j[6])
        launch = getDataFromDict(j[7])
        lastPay = getDataFromDict(j[8])
        firstPay = getDataFromDict(j[9])
        r1usdMin = getDataFromDict(j[10])
        r1usdMax = getDataFromDict(j[11])

        uidList.append(uid)
        installDateList.append(installDate)
        r1usdList.append(r1usd)
        r7usdList.append(r7usd)
        otimeList.append(otime)
        levelList.append(level)
        launchList.append(launch)
        lastPayList.append(lastPay)
        firstPayList.append(firstPay)
        r1usdMinList.append(r1usdMin)
        r1usdMaxList.append(r1usdMax)

    return pd.DataFrame(data = {
        'uid':uidList,
        'install_date':installDateList,
        'r1usd':r1usdList,
        'r7usd':r7usdList,
        'otime':otimeList,
        'level':levelList,
        'launch':launchList,
        'last_pay':lastPayList,
        'first_pay':firstPayList,
        'r1usd_min':r1usdMinList,
        'r1usd_max':r1usdMaxList
    })

# 特征分布
import seaborn as sns
def further():
    df = pd.read_csv(getFilename('allDataFromSS'))
    
    # 将数据做一些处理，将一些过大的值去掉
    otimeMax = 3600*2
    df.loc[df.otime > otimeMax,'otime'] = otimeMax
    levelMax = 50
    df.loc[df.level > levelMax,'level'] = levelMax
    launchMax = 30
    df.loc[df.launch > launchMax,'launch'] = launchMax
    r1usdMax = 100
    df.loc[df.r1usd > r1usdMax,'r1usd'] = r1usdMax
    r7usdMax = 100
    df.loc[df.r7usd > r7usdMax,'r7usd'] = r7usdMax
    
    df.loc[df.r1usd_min > r1usdMax,'r1usd_min'] = r1usdMax
    df.loc[df.r1usd_max > r1usdMax,'r1usd_max'] = r1usdMax

    for i in ['otime','level','launch','r1usd','r7usd','last_pay','first_pay','r1usd_min','r1usd_max']:
        fig = sns.distplot(df[i])
        fig_save = fig.get_figure()
        fig_save.savefig('{}.png'.format(i))
        fig_save.clear()
    print('1')
    sns.pairplot(df).savefig('pair.png')

if __name__ == '__main__':
    df = getAllDataFromSS()
    df.to_csv(getFilename('allDataFromSS'))

    df = pd.read_csv(getFilename('allDataFromSS'))
    print(df.corr())
    further()
    import numpy as np
    sns.set()
    np.random.seed(0)
    x = np.random.randn(100)
    sns.distplot(x).get_figure().savefig('a.png')
    sns.histplot(x).get_figure().savefig('b.png')
    

