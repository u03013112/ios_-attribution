# MC的Demo失败了，原因是数据太多，处理不了。那个之后可以再想办法。
# 这里用数数的数据先试试看，至少算出一版基线。
import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
import datetime
import matplotlib.pyplot as plt
import pickle

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler

import sys
sys.path.append('/src')

from src.config import ssToken
from src.predSkan.lize.userGroupByR2 import addCV
from src.tools import getFilename,printProgressBar

# 异步执行数数的查询
def ssSql(sql):
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
    # 通过taskId查询任务状态
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
                    lines += r.text.split('\r\n')
                    # print('page:%d/%d'%(p,pageCount),'lines:',len(lines))
                    printProgressBar(p+1, pageCount, prefix = 'Progress:', suffix = 'page', length = 50)
                return lines
            else:
                # print('progress:',j['data']['progress'])
                printProgressBar(j['data']['progress'], 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            # print('e:',e)
            # print(r.text)
            continue
        # 查询太慢了，多等一会再尝试
        time.sleep(10)

# 数数获得下面数据
# label：uid，r1usd,r7usd
# 合成建筑次数（24小时内），这里不再区分建筑类型，只要是合成建筑就算一次：uid，count
# 合成士兵，同上：uid，count
# 英雄升级，同上：uid，count
# 英雄升星，同上：uid，count
# 道具花费，这个可能是真的要拆分开的，这个可以稍后添加
# 登录次数，同上：uid，count
# 在线时间，同上：uid，count（秒）
# 资源，应该是分开资源类型：uid，type,count
# 其他，比如雷达之类，这个可以稍后添加

# 总体上是用uid做主键，将所需数据合并到一起，上面每一个都应该是生成独立的表，这样方便修改。
# 所有数据都应该采用相同的条件，目前使用首次登录平台是海外安卓，注册日期是2022-07-01~2023-02-01，观察时间是2022-07-01~2023-03-01，全部汇总后按uid分开。
# 返回都是用Df格式

# label
def getR1usdR7usd():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1 from (select group_0,"$__Date_Time",cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_0,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 6.048E+5),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('PAY_SUCCESS_REALTIME')) and (((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 6.048E+5))) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])

    # 返回值是类似：
    # [
    #     '["939863829273",{"1981-01-01 00:00:00":245.60999751091003},{"1981-01-01 00:00:00":245.60999751091003},245.60999751091003,1430,1430]',
    #     '["939788892954",{"1981-01-01 00:00:00":167.5113},{"1981-01-01 00:00:00":167.5113},167.5113,1430,1430]',
    #     '["939622316826",{"1981-01-01 00:00:00":136.02122441845415},{"1981-01-01 00:00:00":151.94160330686572},136.02122441845415,1430,1430]',
    #     '["939491728154",{"1981-01-01 00:00:00":120.45330000000001},{"1981-01-01 00:00:00":127.2843},120.45330000000001,1430,1430]',
    #     '["939906849563",{"1981-01-01 00:00:00":113.89000058174133},{"1981-01-01 00:00:00":113.89000058174133},113.89000058174133,1430,1430]',
    #     '["939710225177",{"1981-01-01 00:00:00":106.10820000000001},{"1981-01-01 00:00:00":106.10820000000001},106.10820000000001,1430,1430]',
    #     '["939642567450",{"1981-01-01 00:00:00":79.1266337204504},{"1981-01-01 00:00:00":79.1266337204504},79.1266337204504,1430,1430]',
    #     '["939659111193",{"1981-01-01 00:00:00":76.85999798774719},{"1981-01-01 00:00:00":76.85999798774719},76.85999798774719,1430,1430]',
    #     '["939612855065",{"1981-01-01 00:00:00":70.88999843597412},{"1981-01-01 00:00:00":75.87999820709229},70.88999843597412,1430,1430]',
    #     '["939800570650",{"1981-01-01 00:00:00":65.29579700000001},{"1981-01-01 00:00:00":65.29579700000001},65.29579700000001,1430,1430]'
    # ]
    # 也就是说，每一行是一个用户，每一行的第一个元素是用户id，第二个元素是一个字典，key是查询周期，value是r1usd，第三个元素是一个字典，key是查询周期，value是r7usd
    # 后面的元素忽略
    # 转成DataFrame格式返回
    uidList = []
    r1usdList = []
    r7usdList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        r1usd = list(lineJ[1].values())[0]
        r1usdList.append(r1usd)
        r7usd = list(lineJ[2].values())[0]
        r7usdList.append(r7usd)
    df = pd.DataFrame({'uid': uidList, 'r1usd': r1usdList, 'r7usd': r7usdList})
    return df

def getMergeBuilding():
    # 这个数字好大啊，比预想的要大很多
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'merge_building' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

def getMergeArmy():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'merge_army' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

def getHeroLevelUp():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'hero_level_up' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

def getHeroStarUp():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'hero_star_up' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

# 这个登录选用的是数数的登录事件，不是真实登录事件
def getLogin():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'app_login' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

def getOnlineTime():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."#duration"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#duration","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

def getPayCount():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

# 用户等级分布
def getUserLevelMax():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(MAX(ta_ev."user_level"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","user_level","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'user_levelup' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    
    uidList = []
    countList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        count = list(lineJ[1].values())[0]
        if count > 80:
            count = 80
        countList.append(count)
        
    df = pd.DataFrame({'uid': uidList, 'count': countList})
    return df

# 获得用户注册时间，按照utc0天进行区分
def getLoginByInstallDate():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"#vp@ctime_utc0",1),'yyyy-MM-dd') group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'app_login' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount
    '''
    lines = ssSql(sql=sql)
    print(lines[0:10])
    
    uidList = []
    # countList = []
    installDateList = []
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        uidList.append(uid)
        # count = list(lineJ[1].values())[0]
        # countList.append(count)
        installDate = lineJ[1]
        installDateList.append(installDate)
        
    df = pd.DataFrame({'uid': uidList, 'installDate': installDateList})
    return df



def getResource():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(SUM(ta_ev."resource_change"), 0) as double) internal_amount_0 from (select *, "resource_type" group_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","resource_type","resource_change","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'RESOURCE_COST' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2023-03-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-03-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    # 这个sql只获取1天的，是为了快速验证是不是正确
    # sql = '''
    #     select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(SUM(ta_ev."resource_change"), 0) as double) internal_amount_0 from (select *, "resource_type" group_1 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","resource_type","resource_change","#event_time","#user_id","platform","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20220624) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20220624))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'RESOURCE_COST' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-06-30' and '2022-07-02') and ("@vpc_tz_#event_time" >= timestamp '2022-07-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2022-07-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-07-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-01-31 23:59:59' as timestamp))))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    # '''
    lines = ssSql(sql=sql)
    print(lines[0:10])
    
    # 得到的数据大致是下面的样子
    # [
    # '["815439943832","OILA",{"1981-01-01 00:00:00":1.499E7},14990000,30339]', 
    # '["815439943832","SOIL",{"1981-01-01 00:00:00":1.499E7},14990000,30339]', 
    # '["815342307480","OILA",{"1981-01-01 00:00:00":3690000.0},3690000.0,30339]', 
    # '["815342307480","SOIL",{"1981-01-01 00:00:00":3690000.0},3690000.0,30339]', 
    # '["815375149208","SOIL",{"1981-01-01 00:00:00":2490000.0},2490000.0,30339]', 
    # '["815375149208","OILA",{"1981-01-01 00:00:00":2490000.0},2490000.0,30339]', 
    # '["815501101208","OILA",{"1981-01-01 00:00:00":1990000.0},1990000.0,30339]', 
    # '["815501101208","SOIL",{"1981-01-01 00:00:00":1990000.0},1990000.0,30339]', 
    # '["815453063342","OILA",{"1981-01-01 00:00:00":1490000.0},1490000.0,30339]', 
    # '["815453063342","SOIL",{"1981-01-01 00:00:00":1490000.0},1490000.0,30339]'
    # ]
    # 希望整理成uid,ENERGY,FREE_GOLD,MILITARY,OILA,PAID_GOLD,SOIL的形式,另外数量采用元素的第3个值

    # 用map是为了排重
    uidMap = {}

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        uid = lineJ[0]
        t = lineJ[1]
        value = lineJ[3]
        if uid not in uidMap:
            uidMap[uid] = {
                'ENERGY': 0,
                'FREE_GOLD': 0,
                'MILITARY': 0,
                'OILA': 0,
                'PAID_GOLD': 0,
                'SOIL': 0
            }
        
        uidMap[uid][t] = value

    # 重新转回list，方便转成dataframe
    uidList = list(uidMap.keys())
    energyList = [uidMap[uid]['ENERGY'] for uid in uidList]
    freeGoldList = [uidMap[uid]['FREE_GOLD'] for uid in uidList]
    militaryList = [uidMap[uid]['MILITARY'] for uid in uidList]
    oilaList = [uidMap[uid]['OILA'] for uid in uidList]
    paidGoldList = [uidMap[uid]['PAID_GOLD'] for uid in uidList]
    soilList = [uidMap[uid]['SOIL'] for uid in uidList]

    # 转成dataframe
    df = pd.DataFrame({
        'uid': uidList,
        'ENERGY': energyList,
        'FREE_GOLD': freeGoldList,
        'MILITARY': militaryList,
        'OILA': oilaList,
        'PAID_GOLD': paidGoldList,
        'SOIL': soilList
    })
        
    return df

# 将csvFileList合并，用uid做key，按照第一个csv为主，后面全部都是left join。
# 如果有重复的列名，会自动加上后缀，后缀用csvFileName，由于之前的命名都是demoSs开头，这里直接写死是文件名去除demoSs开头和结尾的".csv"的部分
def mergeCsv(csvFileList):
    df = pd.read_csv(csvFileList[0])
    for i in range(1, len(csvFileList)):
        df2 = pd.read_csv(csvFileList[i])
        # csvFileList[i] 是完全路径文件名，要去掉路径，只留文件名
        filename = csvFileList[i].split('/')[-1]
        df = df.merge(df2, how='left', on='uid', suffixes=('', filename[6:-4]))
    return df

# 打标签
def makeLabel(df):
    # 将读csv中的多余索引去掉
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # 打入标签CV1
    cvMapDf1 = pd.read_csv(getFilename('cvMapDf1'))
    df = addCV(df,cvMapDf1,usd = 'r1usd',cvName = 'cv1')

    # 打入标签CV7
    cv1List = list(cvMapDf1['cv'].unique())
    cv1List.sort()

    for cv1 in cv1List:
        cvMapDf7 = pd.read_csv(getFilename('cvMapDf7_%s'%cv1))
        
        df.loc[df.cv1 == cv1,'cv7'] = 0
        cv7List = list(cvMapDf7['cv'].unique())
        cv7List.sort()
        for cv7 in cv7List:
            min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
            max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
            
            df.loc[
                (df.cv1 == cv1) &
                (df['r7usd'] > min_event_revenue) & (df['r7usd'] <= max_event_revenue),
                'cv7'
            ] = cv7
        df.loc[
            (df.cv1 == cv1) &
            (df['r7usd'] > max_event_revenue),
            'cv7'
        ] = len(cvMapDf7)-1

    # cv1 != 0 的 cv7 == 0 的，cv7都改为1，因为这一档位本不该有人，可能是浮点数误差导致
    df.loc[(df.cv1 != 0) & (df.cv7 == 0),'cv7'] = 1

    # df['cv7']转成int
    df['cv7'] = df['cv7'].astype(int)
    # 显示df前几行
    # print(df.head())

    # labelDf = df[['uid','cv1','cv7']]

    # labelDf.to_csv(getFilename('labelDf'),index = False)

    return df


# df是一个完整的DF，必须包括列：uid,cv1,cv7,features
# df可以先通过cv1过滤，这样只针对一种cv1，进行cv7分类
# 除了uid,cv1,cv7，剩下的列都被认为是特征
def getXY(df,cv1):
    # 按照cv1过滤
    cv1Df = df[df['cv1'] == cv1]
    # 按照cv7分类
    y = cv1Df['cv7'].to_numpy().reshape(-1,1)
    # 将y形状变为(n_samples, )，用ravel
    y = y.ravel()

    # 深度拷贝一份df
    dfCopy = cv1Df.copy(deep = True)

    # print(dfCopy.head())

    # 去掉uid,cv1,cv7
    dfCopy.drop(['uid','cv1','cv7','r1usd','r7usd'],axis = 1,inplace = True)

    # 转成numpy
    x = dfCopy.to_numpy()

    return x,y

# 这个版本不再加入任何过滤，需要将cv1的过滤写在外面，这样在整理流程里看的更明白
def getXY2(df):
    # 按照cv7分类
    y = df['cv7'].to_numpy().reshape(-1,1)
    # 将y形状变为(n_samples, )，用ravel
    y = y.ravel()

    # 深度拷贝一份df
    dfCopy = df.copy(deep = True)

    # 去掉uid,cv1,cv7
    dfCopy.drop(['uid','cv1','cv7','r1usd','r7usd','installDate'],axis = 1,inplace = True)

    # 转成numpy
    x = dfCopy.to_numpy()

    return x,y

# 获得特征的名字
def getXName(df):
    # 深度拷贝一份df
    dfCopy = df.copy(deep = True)

    # 去掉uid,cv1,cv7
    dfCopy.drop(['uid','cv1','cv7','r1usd','r7usd'],axis = 1,inplace = True)

    # 将列名转成list
    xName = list(dfCopy.columns)
    return xName

# 头文件为了 SelectKBest 和 f_classif
from sklearn.feature_selection import SelectKBest, f_classif,mutual_info_classif

# 计算特征重要程度
# 多分类模型，使用f_classif
def getFeatureImportance(x,y):
    # 特征重要程度
    k = SelectKBest(f_classif, k=10)
    k.fit(x, y)
    print(k.scores_)
    # print(k.pvalues_)
    # print(k.get_support())
    return k

# 进行决策树分类，按照给出的x,y，计算出决策树分类的准确率
def getDecisionTreeAccuracy(x,y):
    # 决策树分类
    from sklearn.tree import DecisionTreeClassifier
    clf = DecisionTreeClassifier()
    clf.fit(x, y)
    # print(clf.predict(x))
    # print(clf.predict_proba(x))
    # print(clf.score(x, y))
    return clf.score(x, y)

# 进行决策树多分类
# 将x,y 按比例进行划分，分成训练集和测试集
# 预测结果
# 计算训练集与测试集每个分类查准率与查全率
def getDecisionTreeMultiClassify(x,y):
    # 决策树分类
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report

    # 划分训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # 决策树分类
    clf = DecisionTreeClassifier(max_depth=12, min_samples_leaf=8)
    # clf = DecisionTreeClassifier()
    clf.fit(x_train, y_train)

    depth = clf.get_depth()
    print('The depth of the decision tree classifier is:', depth)

    # 获取最小叶子节点样本数
    min_samples_leaf = clf.min_samples_leaf
    print('The minimum number of samples required to be at a leaf node is:', min_samples_leaf)
    # 预测结果
    y_pred = clf.predict(x_test)

    # 计算训练集与测试集每个分类查准率与查全率
    from sklearn.metrics import classification_report
    print(classification_report(y_train, clf.predict(x_train)))
    print(classification_report(y_test, y_pred))



# 计算np.array中每个不同值的数量和所占比例，直接打印到终端
def printNpArrayCountAndRatio(npArray):
    # 计算每个不同值的数量
    unique, counts = np.unique(npArray, return_counts=True)
    print(dict(zip(unique, counts)))
    # 计算每个不同值的所占比例
    unique, counts = np.unique(npArray, return_counts=True)
    print(dict(zip(unique, counts/len(npArray))))


def report1():
    df = pd.read_csv(getFilename('demoSsAllMakeLabel'))

    lines = []
    head = ['cv1']
    for i in range(8):
        head.append('cv7Count%d'%i)
    for i in range(8):
        head.append('cv7Ratio%d'%i)
    # for i in range(12):
    #     head.append('feature%d'%i)
    for xName in getXName(df):
        head.append(xName)

    lines.append(head)

    for cv1 in range(8):
        x,y = getXY(df,cv1)

        # 计算np.array中每个不同值的数量和所占比例，记录到字符串中，以便后续保存输出
        line = [str(cv1)]
        
        unique, counts = np.unique(y, return_counts=True)

        l1 = (list(dict(zip(unique, counts)).values()))
        # 如果l1中元素类型是int64，需要转成int
        for i in range(len(l1)):
            l1[i] = int(l1[i])

        line += l1
        line += (list(dict(zip(unique, counts/len(y))).values()))

        # 特征得分，目前共12个特征
        k = SelectKBest(f_classif, k=10)
        # k = SelectKBest(mutual_info_classif, k=10)
        

        # 预处理
        preprocessing_method = RobustScaler()
        preprocessing_method.fit(x)
        x = preprocessing_method.transform(x)

        k.fit(x, y)

        scoreList = list(k.scores_)
        scoreSum = sum(scoreList)
        # 求出scoreList中每个元素与scoreSum的比值，并放入新的列表中
        scoreListRatio = []
        for i in range(len(scoreList)):
            scoreListRatio.append(scoreList[i]/scoreSum)

        line += scoreListRatio

        lines.append(line)

        # 然后就是训练集和测试集的准确度，为了方便记录日志，可以只记录整体准确程度，详细的结论单独看详细日志        
    
    # print(lines)
    return lines

def report2():
    # 这里需要记录在不同参数的前提下，决策树的表现情况

    # 需要有详细记录，以便后续分析

    # 还需要有一个总体的评估计算，可以快速的在参数中获得最优的参数
    # 总体的评估指标应该采用按天的真实7日付费金额e的MAPE和R2

    from sklearn.tree import DecisionTreeClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report

    df = pd.read_csv(getFilename('demoSsAllMakeLabel'))

    # 划分训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3)
                                                        # , random_state=42)

    lines = []
    head = ['max_depth','min_samples_leaf','mape','r2']

    for max_depth in (5,10,15,20,25,30):
        for min_samples_leaf in (1,3,5,7):
            detailStr = ''
            for cv1 in range(8):
                x,y = getXY(df,cv1)
                # 决策树分类
                clf = DecisionTreeClassifier(max_depth=max_depth, min_samples_leaf=min_samples_leaf)
                # clf = DecisionTreeClassifier()
                clf.fit(x_train, y_train)

                # depth = clf.get_depth()
                # print('The depth of the decision tree classifier is:', depth)

                # # 获取最小叶子节点样本数
                # min_samples_leaf = clf.min_samples_leaf
                # print('The minimum number of samples required to be at a leaf node is:', min_samples_leaf)
                # 预测结果
                y_pred = clf.predict(x_test)

                # 计算训练集与测试集每个分类查准率与查全率
                detailStr += 'cv1:%d\n'%cv1
                detailStr += 'train:\n'
                detailStr += (classification_report(y_train, clf.predict(x_train)))
                detailStr += '\ntest:\n'
                detailStr += (classification_report(y_test, y_pred))

            detailLogFileName = '/src/data/doc/demoSS/report2DetailLog_%d_%d.txt'%(max_depth,min_samples_leaf)
            with open(detailLogFileName, 'w') as f:
                f.write(detailStr)

from sklearn.tree import DecisionTreeClassifier,plot_tree
from sklearn.metrics import r2_score,mean_absolute_percentage_error

def report3():
    sheet = 'Sheet8'
    GSheet().clearSheet('1111',sheet)
    GSheet().updateSheet('1111',sheet,'A1', [['特征数量','最大深度','最小叶子节点样本数', '训练mape', '训练r2', '测试mape', '测试r2']])
    gsLineCount = 2
    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    df = pd.read_csv(getFilename('demoSsAllMakeLabel'))

    df = df.merge(installDateDf, on='uid', how='left')
    # 将df按行，随机抽取30%作为测试集，剩下的70%作为训练集
    df = df.sample(frac=1).reset_index(drop=True)
    dfTest = df[:int(len(df)*0.3)]
    dfTrain = df[int(len(df)*0.3):]

    # 特征筛选，将原有的特征缩减至N个
    for n in (3,):
        for max_depth in (30,):
            for min_samples_leaf in (1,):
                dfTrainCopy = dfTrain.copy(deep=True)
                dfTestCopy = dfTest.copy(deep=True)

                trainRetDf = pd.DataFrame(columns=['uid','cv7p','r7usdp'])
                testRetDf = pd.DataFrame(columns=['uid','cv7p','r7usdp'])
                
                # 按照cv1将df分成8个子df，遍历li每个子df，分别进行决策树分类
                for cv1 in range(8):
                    dfCv1 = df[df['cv1']==cv1]
                    x,y = getXY2(dfCv1)
                    
                    # 用整体数据进行特征筛选
                    k = SelectKBest(f_classif, k=n)
                    k.fit(x, y)

                    # 将训练集和测试集进行特征筛选
                    trainCv1Df = dfTrain[dfTrain['cv1']==cv1].copy(deep=True)
                    trainX,trainY = getXY2(trainCv1Df)
                    trainXNew = k.transform(trainX)

                    # 决策树分类
                    clf = DecisionTreeClassifier(max_depth=max_depth, min_samples_leaf=min_samples_leaf)
                    clf.fit(trainXNew, trainY)

                    # fig, ax = plt.subplots(figsize=(12, 12))
                    # plot_tree(clf, ax=ax)
                    # # plt.show()
                    # plt.savefig('/src/data/tree_%d.png'%(cv1))

                    with open('/src/data/tree_%d.pkl'%(cv1), 'wb') as f:
                        pickle.dump(clf, f)

                    trainYPred = clf.predict(trainXNew)
                    trainCv1Df.loc[:, 'cv7p'] = trainYPred

                    # 预测结果
                    testCv1Df = dfTest[dfTest['cv1']==cv1].copy(deep=True)
                    testX,_ = getXY2(testCv1Df)
                    testXNew = k.transform(testX)
                    testYPred = clf.predict(testXNew)
                
                    # 将预测结果标记到dfCv1的'cv7p'列中
                    testCv1Df.loc[:, 'cv7p'] = testYPred

                    cvMapDf7 = pd.read_csv(getFilename('cvMapDf7_%d'%cv1))
                    for cv7 in range(9):
                        if len(cvMapDf7.loc[cvMapDf7.cv == cv7]) <= 0:
                            continue
                        min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
                        max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
                        avg = (min_event_revenue + max_event_revenue)/2
                        if avg < 0:
                            avg = 0
                        trainCv1Df.loc[trainCv1Df['cv7p']==cv7,'r7usdp'] = avg
                        testCv1Df.loc[testCv1Df['cv7p']==cv7,'r7usdp'] = avg

                    trainTmpDf = trainCv1Df[['uid','cv7p','r7usdp']]
                    trainRetDf = trainRetDf.append(trainTmpDf,ignore_index=True)

                    testTmpDf = testCv1Df[['uid','cv7p','r7usdp']]
                    testRetDf = testRetDf.append(testTmpDf,ignore_index=True)

                dfTrainCopy = pd.merge(dfTrainCopy, trainRetDf, on='uid', how='left')    
                dfTestCopy = pd.merge(dfTestCopy, testRetDf, on='uid', how='left')

                trainInstallDateDf = dfTrainCopy.groupby('installDate').agg({
                    'r7usd':'sum',
                    'r7usdp':'sum'
                })
                trainInstallDateDf.loc[:,'mape'] = abs(trainInstallDateDf['r7usd'] - trainInstallDateDf['r7usdp'])/trainInstallDateDf['r7usd']
                trainMape = mean_absolute_percentage_error(trainInstallDateDf['r7usd'],trainInstallDateDf['r7usdp'])
                trainR2Score = r2_score(trainInstallDateDf['r7usd'],trainInstallDateDf['r7usdp'])

                testInstallDateDf = dfTestCopy.groupby('installDate').agg({
                    'r7usd':'sum',
                    'r7usdp':'sum'
                })
                testInstallDateDf.loc[:,'mape'] = abs(testInstallDateDf['r7usd'] - testInstallDateDf['r7usdp'])/testInstallDateDf['r7usd']
                testMape = mean_absolute_percentage_error(testInstallDateDf['r7usd'],testInstallDateDf['r7usdp'])
                testR2Score = r2_score(testInstallDateDf['r7usd'],testInstallDateDf['r7usdp'])
                
                
                GSheet().updateSheet('1111',sheet,'A%d'%gsLineCount, [[n,max_depth,min_samples_leaf, trainMape, trainR2Score,testMape,testR2Score]])
                gsLineCount += 1

                print('n:%d max_depth:%d min_samples_leaf:%d'%(n,max_depth,min_samples_leaf))


# 要把max_depth和min_samples_leaf的值按照cv1分开优化，并记录结果
def report4():

    sheet = 'Sheet7'
    GSheet().clearSheet('1111',sheet)
    GSheet().updateSheet('1111',sheet,'A1', [['cv1','最大深度','最小叶子节点样本数', '训练mape', '训练r2', '测试mape', '测试r2']])
    gsLineCount = 2

    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    df = pd.read_csv(getFilename('demoSsAllMakeLabel'))

    df = df.merge(installDateDf, on='uid', how='left')
    # 将df按行，随机抽取30%作为测试集，剩下的70%作为训练集
    df = df.sample(frac=1).reset_index(drop=True)

    for cv1 in range(8):
        dfCv1 = df[df['cv1']==cv1].copy(deep=True)

        dfTrain = dfCv1[int(len(dfCv1)*0.3):]
        dfTest = dfCv1[:int(len(dfCv1)*0.3)]
        x,y = getXY2(dfCv1)

        k = SelectKBest(f_classif, k=5)
        k.fit(x, y)

        trainX,trainY = getXY2(dfTrain)
        trainXNew = k.transform(trainX)

        for max_depth in (5,10,20,30):
            for min_samples_leaf in (1,2,3):
                trainRetDf = pd.DataFrame(columns=['uid','cv7p','r7usdp'])
                testRetDf = pd.DataFrame(columns=['uid','cv7p','r7usdp'])

                dfTrainCopy = dfTrain.copy(deep=True)
                dfTestCopy = dfTest.copy(deep=True)

                clf = DecisionTreeClassifier(max_depth=max_depth, min_samples_leaf=min_samples_leaf)
                clf.fit(trainXNew, trainY)
                trainYPred = clf.predict(trainXNew)
                dfTrainCopy.loc[:, 'cv7p'] = trainYPred

                testX,_ = getXY2(dfTest)
                testXNew = k.transform(testX)
                testYPred = clf.predict(testXNew)
                dfTestCopy.loc[:, 'cv7p'] = testYPred

                cvMapDf7 = pd.read_csv(getFilename('cvMapDf7_%d'%cv1))
                for cv7 in range(9):
                    if len(cvMapDf7.loc[cvMapDf7.cv == cv7]) <= 0:
                        continue
                    min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
                    max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
                    avg = (min_event_revenue + max_event_revenue)/2
                    if avg < 0:
                        avg = 0
                    dfTrainCopy.loc[dfTrainCopy['cv7p']==cv7,'r7usdp'] = avg
                    dfTestCopy.loc[dfTestCopy['cv7p']==cv7,'r7usdp'] = avg
                
                trainTmpDf = dfTrainCopy[['uid','installDate','cv7','r7usd','cv7p','r7usdp']]
                trainRetDf = trainRetDf.append(trainTmpDf,ignore_index=True)
                trainInstallDateDf = trainRetDf.groupby('installDate').agg({
                    'r7usd':'sum',
                    'r7usdp':'sum'
                })
                trainInstallDateDf.loc[:,'mape'] = abs(trainInstallDateDf['r7usd'] - trainInstallDateDf['r7usdp'])/trainInstallDateDf['r7usd']
                trainMape = mean_absolute_percentage_error(trainInstallDateDf['r7usd'],trainInstallDateDf['r7usdp'])
                trainR2Score = r2_score(trainInstallDateDf['r7usd'],trainInstallDateDf['r7usdp'])

                testTmpDf = dfTestCopy[['uid','installDate','cv7','r7usd','cv7p','r7usdp']]
                testRetDf = testRetDf.append(testTmpDf,ignore_index=True)
                testInstallDateDf = testRetDf.groupby('installDate').agg({
                    'r7usd':'sum',
                    'r7usdp':'sum'
                })
                testInstallDateDf.loc[:,'mape'] = abs(testInstallDateDf['r7usd'] - testInstallDateDf['r7usdp'])/testInstallDateDf['r7usd']
                testMape = mean_absolute_percentage_error(testInstallDateDf['r7usd'],testInstallDateDf['r7usdp'])
                testR2Score = r2_score(testInstallDateDf['r7usd'],testInstallDateDf['r7usdp'])
                
                GSheet().updateSheet('1111',sheet,'A%d'%gsLineCount, [[cv1,max_depth,min_samples_leaf, trainMape, trainR2Score,testMape,testR2Score]])
                gsLineCount += 1

                print('cv1:%d max_depth:%d min_samples_leaf:%d'%(cv1,max_depth,min_samples_leaf))

# 将模型从kpl文件读取出来，并可视化保存为图片 
def getTreePic():
    for cv1 in range(8):
        with open('/src/data/tree_%d.pkl'%cv1, 'rb') as f:
            model = pickle.load(f)
            print(cv1)
            plt.figure(figsize=(1200, 60))
            plot_tree(model)
            plt.savefig('/src/data/tree_%d.png'%cv1)
            print(cv1,'ok')






from src.googleSheet import GSheet

if __name__ == '__main__':
    # r1usdR7usdDfDf = getR1usdR7usd()
    # r1usdR7usdDfDf.to_csv(getFilename('demoSsLabel'), index=False)
    
    # mergeBuildingDf = getMergeBuilding()
    # mergeBuildingDf.to_csv(getFilename('demoSsMergeBuilding'), index=False)

    # mergeArmyDf = getMergeArmy()
    # mergeArmyDf.to_csv(getFilename('demoSsMergeArmy'), index=False)
    
    # heroLevelUpDf = getHeroLevelUp()
    # heroLevelUpDf.to_csv(getFilename('demoSsHeroLevelUp'), index=False)

    # heroStarUpDf = getHeroStarUp()
    # heroStarUpDf.to_csv(getFilename('demoSsHeroStarUp'), index=False)

    # loginDf = getLogin()
    # loginDf.to_csv(getFilename('demoSsLogin'), index=False)

    # payCountDf = getPayCount()
    # payCountDf.to_csv(getFilename('demoSsPayCount'), index=False)

    # userLevelMax = getUserLevelMax()
    # userLevelMax.to_csv(getFilename('demoSsUserLevelMax'), index=False)

    # resourceDf = getResource()
    # resourceDf.to_csv(getFilename('demoSsResource'), index=False)

    # csvFileList = [
    #     # 将login放到最前面
    #     getFilename('demoSsLogin'),
    #     getFilename('demoSsLabel'),
    #     getFilename('demoSsMergeBuilding'),
    #     getFilename('demoSsMergeArmy'),
    #     getFilename('demoSsHeroLevelUp'),
    #     getFilename('demoSsHeroStarUp'),
    #     getFilename('demoSsPayCount'),
    #     getFilename('demoSsUserLevelMax'),
    #     getFilename('demoSsResource')
    # ]

    # df = mergeCsv(csvFileList)
    # # 合并之后所有空位填充0
    # df = df.fillna(0)
    # df.to_csv(getFilename('demoSsAll'), index=False)

    # df = pd.read_csv(getFilename('demoSsAll'))
    # df = makeLabel(df)
    # df.to_csv(getFilename('demoSsAllMakeLabel'), index=False)

    # df = pd.read_csv(getFilename('demoSsAllMakeLabel'))
    # for cv1 in range(8):
    #     x,y = getXY(df,cv1)

    #     # print(x.shape,y.shape)
    #     # print(x[:100],y[:100])
    #    getFeatureImportance(x,y)
    #     printNpArrayCountAndRatio(list(y))

    #     # print(getDecisionTreeAccuracy(x,y))
    #     getDecisionTreeMultiClassify(x,y)

    lines = report1()
            
    GSheet().clearSheet('1111','Sheet4')
    GSheet().updateSheet('1111','Sheet4','A1',lines)

    # loginByInstallDateDf = getLoginByInstallDate()
    # loginByInstallDateDf.to_csv(getFilename('demoSsLoginByInstallDate'), index=False)

    # report3()
    # report4()
    # getTreePic()
