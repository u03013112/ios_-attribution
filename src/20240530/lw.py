# lastwar 用户行为与付费数据分析

import time
import json

import requests
from urllib import parse
from requests.adapters import HTTPAdapter

import pandas as pd

import sys
sys.path.append('/src')

from src.config import ssToken2 as ssToken


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

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
        print('error1')
        print(r.text)
        return
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
            print('error2:',e)
            print(r.text)
            continue
        # 查询太慢了，多等一会再尝试
        time.sleep(10)

def getData():
    sql = '''
select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2,count(data_map_3) over () group_num_3,count(data_map_4) over () group_num_4,count(data_map_5) over () group_num_5,count(data_map_6) over () group_num_6,count(data_map_7) over () group_num_7,count(data_map_8) over () group_num_8,count(data_map_9) over () group_num_9,count(data_map_10) over () group_num_10 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", amount_3) filter (where amount_3 is not null and is_finite(amount_3) ) data_map_3,map_agg("$__Date_Time", amount_4) filter (where amount_4 is not null and is_finite(amount_4) ) data_map_4,map_agg("$__Date_Time", amount_5) filter (where amount_5 is not null and is_finite(amount_5) ) data_map_5,map_agg("$__Date_Time", cast(row(internal_amount_5,internal_amount_6) as row(internal_amount_5 DOUBLE,internal_amount_6 DOUBLE))) filter (where amount_5 is not null and is_finite(amount_5) ) data_part_map_5,map_agg("$__Date_Time", amount_6) filter (where amount_6 is not null and is_finite(amount_6) ) data_map_6,map_agg("$__Date_Time", amount_7) filter (where amount_7 is not null and is_finite(amount_7) ) data_map_7,map_agg("$__Date_Time", amount_8) filter (where amount_8 is not null and is_finite(amount_8) ) data_map_8,map_agg("$__Date_Time", amount_9) filter (where amount_9 is not null and is_finite(amount_9) ) data_map_9,map_agg("$__Date_Time", amount_10) filter (where amount_10 is not null and is_finite(amount_10) ) data_map_10,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_2 amount_2,internal_amount_3 amount_3,internal_amount_4 amount_4,internal_amount_7 amount_5,internal_amount_8 amount_6,internal_amount_9 amount_7,internal_amount_10 amount_8,internal_amount_11 amount_9,internal_amount_12 amount_10 from (select *, cast(coalesce(internal_amount_5, 0) as double)-cast(coalesce(internal_amount_6, 0) as double) internal_amount_7 from (select group_0,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1,arbitrary(internal_amount_2) internal_amount_2,arbitrary(internal_amount_3) internal_amount_3,arbitrary(internal_amount_4) internal_amount_4,arbitrary(internal_amount_5) internal_amount_5,arbitrary(internal_amount_6) internal_amount_6,arbitrary(internal_amount_8) internal_amount_8,arbitrary(internal_amount_9) internal_amount_9,arbitrary(internal_amount_10) internal_amount_10,arbitrary(internal_amount_11) internal_amount_11,arbitrary(internal_amount_12) internal_amount_12 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 's_hero_level_up' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(1), 0) as double) internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'app_launch' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,cast(coalesce(COUNT(if((( ( "$part_event" IN ( 's_login' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24),1)), 0) as double) internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,cast(coalesce(MAX(if((( ( "$part_event" IN ( 's_login' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24),ta_ev."lw_main_level")), 0) as double) internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "lw_main_level","#event_name","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('s_login')) and ((( ( "$part_event" IN ( 's_login' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,cast(coalesce(COUNT(1), 0) as double) internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 's_pay_action' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,cast(coalesce(COUNT(1), 0) as double) internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 's_plunder' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,cast(coalesce(SUM(if((( ( "$part_event" IN ( 's_gold_cost' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24),ta_ev."original_gold")), 0) as double) internal_amount_5,cast(coalesce(SUM(if((( ( "$part_event" IN ( 's_gold_cost' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24),ta_ev."remain_gold")), 0) as double) internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","remain_gold","#event_time","original_gold","#zone_offset","#user_id","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('s_gold_cost')) and ((( ( "$part_event" IN ( 's_gold_cost' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,cast(coalesce(SUM(ta_ev."#duration"), 0) as double) internal_amount_8,null internal_amount_9,null internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#duration","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,cast(coalesce(COUNT(1), 0) as double) internal_amount_10,null internal_amount_11,null internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 's_radar' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,null internal_amount_2,null internal_amount_3,null internal_amount_4,null internal_amount_5,null internal_amount_6,null internal_amount_8,null internal_amount_9,null internal_amount_10,cast(coalesce(SUM(if((( ( "$part_event" IN ( 's_pay_new' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24),ta_ev."#vp@closing_currency__price_local__USD")), 0) as double) internal_amount_11,cast(coalesce(SUM(if((( ( "$part_event" IN ( 's_pay_new' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 168),ta_ev."#vp@closing_currency__price_local__USD")), 0) as double) internal_amount_12 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select v_alias_currency.*, try_cast(try(ROUND((("price_local" / "ex@exchange") * "ex@target_map"['USD']), 4)) as double) "#vp@closing_currency__price_local__USD" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","price_local","#event_time","#zone_offset","#user_id","closing_currency","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) b on a."#user_id"=b."#user_id"))) v_alias_currency left join (select a.ex_date "ex@ex_date", a.currency "ex@currency", a.exchange "ex@exchange", b.target_map "ex@target_map" from (select ex_date, currency, exchange FROM ta_dim.ta_exchange where ex_date between '2024-03-30' and '2024-05-03') a join (select ex_date, map_agg(currency, exchange) target_map FROM ta_dim.ta_exchange where currency in ('USD') and ex_date between '2024-03-30' and '2024-05-03' group by ex_date) b on a.ex_date = b.ex_date) currency__data_tbl on v_alias_currency."closing_currency" = currency__data_tbl."ex@currency" and currency__data_tbl."ex@ex_date"=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))) ta_ev inner join (select *, "#account_id" group_0 from (select a.*,"@vpc_cluster_gm" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20240325) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_gm" from user_result_cluster_15 where cluster_name = 'gm') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('s_pay_new')) and (((( ( "$part_event" IN ( 's_pay_new' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 24)) or ((( ( "$part_event" IN ( 's_pay_new' ) ) )) and (ta_ev."#vp@lifetime_hour" <= 168))) and ((("$part_date" between '2024-03-31' and '2024-05-02') and ("@vpc_tz_#event_time" >= timestamp '2024-04-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-05-01'))) and (ta_u."@vpc_cluster_gm" IS NULL)) group by group_0,"$__Date_Time") group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC
    '''

    lines = ssSql2(sql)
    # print(lines[0])
    # ["1153604212000454",{"1981-01-01 00:00:00":1279.0},{"1981-01-01 00:00:00":10.0},{"1981-01-01 00:00:00":25.0},{"1981-01-01 00:00:00":287.0},null,{"1981-01-01 00:00:00":-254459.0},{"1981-01-01 00:00:00":{"internal_amount_5":2.46396477E8,"internal_amount_6":2.46650936E8}},{"1981-01-01 00:00:00":38848.386999999995},{"1981-01-01 00:00:00":25.0},{"1981-01-01 00:00:00":54.0},{"1981-01-01 00:00:00":9175.827400000004},{"1981-01-01 00:00:00":18787.816499999997},1279.0,2315656,5242025,5311220,636781,66976,5351684,4858402,5311220,1291576,274862,274862]


    date_str = "1981-01-01 00:00:00"

    data_list = []
    columns = ['uid', 'heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar', 'payNew24', 'payNew168']

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue

        uid = lineJ[0]
        # 英雄升级.总次数
        heroLevelUp = lineJ[1].get(date_str, 0) if lineJ[1] else 0
        # app_launch.总次数
        appLaunch = lineJ[2].get(date_str, 0) if lineJ[2] else 0
        # 登录.总次数
        login = lineJ[3].get(date_str, 0) if lineJ[3] else 0
        # 尝试支付.总次数
        payAction = lineJ[4].get(date_str, 0) if lineJ[4] else 0
        # 掠夺.总次数
        plunder = lineJ[5].get(date_str, 0) if lineJ[5] else 0
        # 金币花费
        goldCost = lineJ[6].get(date_str, 0) if lineJ[6] else 0
        # 在线时长
        onlineTime = lineJ[8].get(date_str, 0) if lineJ[8] else 0
        # 大本等级最大值
        mainLevel = lineJ[9].get(date_str, 0) if lineJ[9] else 0
        # 完成雷达领奖.总次数
        radar = lineJ[10].get(date_str, 0) if lineJ[10] else 0
        # 24小时付费 usd
        payNew24 = lineJ[11].get(date_str, 0) if lineJ[11] else 0
        # 168小时付费 usd
        payNew168 = lineJ[12].get(date_str, 0) if lineJ[12] else 0

        # 将数据添加到列表中
        data_list.append([uid, heroLevelUp, appLaunch, login, payAction, plunder, goldCost, onlineTime, mainLevel, radar, payNew24, payNew168])

    data = pd.DataFrame(data_list, columns=columns)

    data.to_csv('/src/data/lwData20240530.csv', index=False)

def getData2():
    sql = '''
select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."#duration"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "#event_name","#duration","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20231225) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20231225)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'ta_app_end' ) ) )) and ((("$part_date" between '2023-12-31' and '2024-05-01') and ("@vpc_tz_#event_time" >= timestamp '2024-01-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-04-30'))) and (ta_ev."#vp@lifetime_hour" <= 2E+1)) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(lines[0])

    date_str = "1981-01-01 00:00:00"

    data_list = []

    columns = ['uid', 'onlineTime']

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue

        uid = lineJ[0]
        onlineTime = lineJ[1].get(date_str, 0) if lineJ[1] else 0
        data_list.append([uid, onlineTime])

    data = pd.DataFrame(data_list, columns=columns)
    data.to_csv('/src/data/lwDataOT_20240101_20240430.csv', index=False)

def getData3():
    sql = '''
select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(MAX(ta_ev."lw_main_level"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', "internal_u@lwu_register_date", "#event_time")) as double) "#vp@lifetime_hour" from (select a.*, b."lwu_register_date" "internal_u@lwu_register_date" from (select "lw_main_level","#event_name","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_15) a join (select * from (select "lwu_register_date","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20231225) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20231225)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 's_pay_new' ) ) )) and ((("$part_date" between '2023-12-31' and '2024-05-01') and ("@vpc_tz_#event_time" >= timestamp '2024-01-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-04-30'))) and (ta_ev."#vp@lifetime_hour" <= 24)) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(lines[0])

    date_str = "1981-01-01 00:00:00"

    data_list = []

    columns = ['uid', 'level']

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue

        uid = lineJ[0]
        level = lineJ[1].get(date_str, 0) if lineJ[1] else 0
        data_list.append([uid, level])

    data = pd.DataFrame(data_list, columns=columns)
    data.to_csv('/src/data/lwDataLevel_20240101_20240430.csv', index=False)





def test():
    df = pd.read_csv('/src/data/lwData20240530.csv')
    print(len(df))
    print(df.corr())

    freeDf = df.loc[df['payNew24'] == 0]
    print(len(freeDf))
    print(freeDf.corr())

    freeDf2 = df.loc[df['payNew168'] == 0]
    print(len(freeDf2))
    print(freeDf2.corr())


def getXY():
    df = pd.read_csv('/src/data/lwData20240530.csv')
    # 拥有列 uid  heroLevelUp  appLaunch     login  payAction   plunder  goldCost  onlineTime  mainLevel     radar  payNew24  payNew168

    # 只分析24小时内不付费用户。因为付费用户的168小时付费金额与24小时付费金额有很强的相关性
    freeDf = df.loc[(df['payNew24'] == 0)]

    # 选取特征列
    X = freeDf[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]

    # 选取目标列
    y = freeDf['payNew168']

    return X, y

import pandas as pd  
from sklearn.model_selection import train_test_split  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.metrics import classification_report, accuracy_score  
  
def classify_and_evaluate():  
    # 加载数据并筛选特征和目标列  
    df = pd.read_csv('/src/data/lwData20240530.csv')  
      
    # 只分析24小时内不付费用户  
    freeDf = df.loc[(df['payNew24'] == 0)]  
      
    # 选取特征列  
    X = freeDf[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]  
      
    # 选取目标列并转换为二分类问题  
    y = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)  
      
    # 将数据集分为训练集和测试集  
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)  
      
    # 训练随机森林分类器  
    classifier = RandomForestClassifier(n_estimators=100, random_state=0)  
    classifier.fit(X_train, y_train)  
      
    # 在训练集上预测  
    y_train_pred = classifier.predict(X_train)  
      
    # 在测试集上预测  
    y_test_pred = classifier.predict(X_test)  
      
    # 输出训练集表现  
    print("Training Performance:")  
    print(classification_report(y_train, y_train_pred))  
    print('Training Accuracy:', accuracy_score(y_train, y_train_pred))  
      
    # 输出测试集表现  
    print("Test Performance:")  
    print(classification_report(y_test, y_test_pred))  
    print('Test Accuracy:', accuracy_score(y_test, y_test_pred)) 

if __name__ == '__main__':
    # getData()
    # print('done')

    # test()

    # classify_and_evaluate()


    # getData2()
    getData3()