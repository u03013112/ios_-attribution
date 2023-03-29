# MC的Demo失败了，原因是数据太多，处理不了。那个之后可以再想办法。
# 这里用数数的数据先试试看，至少算出一版基线。
import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
import datetime

import pandas as pd

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
    # df['cv7']转成int
    df['cv7'] = df['cv7'].astype(int)
    # 显示df前几行
    print(df.head())

    # labelDf = df[['customer_user_id','cv1','cv7']]

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

    # 深度拷贝一份df
    dfCopy = cv1Df.copy(deep = True)

    # 去掉customer_user_id,cv1,cv7
    dfCopy = dfCopy.drop(['customer_user_id','cv1','cv7'],axis = 1,inplace = True)

    # 转成numpy
    x = dfCopy.to_numpy()

    return x,y

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

    # resourceDf = getResource()
    # resourceDf.to_csv(getFilename('demoSsResource'), index=False)

    csvFileList = [
        # 将login放到最前面
        getFilename('demoSsLogin'),
        getFilename('demoSsLabel'),
        getFilename('demoSsMergeBuilding'),
        getFilename('demoSsMergeArmy'),
        getFilename('demoSsHeroLevelUp'),
        getFilename('demoSsHeroStarUp'),
        getFilename('demoSsPayCount'),
        getFilename('demoSsResource')
    ]

    df = mergeCsv(csvFileList)
    # 合并之后所有空位填充0
    df = df.fillna(0)
    df.to_csv(getFilename('demoSsAll'), index=False)

    df = pd.read_csv(getFilename('demoSsAll'))
    df = makeLabel(df)
    df.to_csv(getFilename('demoSsAllMakeLabel'), index=False)