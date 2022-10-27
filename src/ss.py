# 训练所需数据获得，与预测数据主要区别是，获取多天数据
import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
import datetime

import sys
sys.path.append('/src')

from src.config import ssToken

# TODO:目前获得的都是从ss用api得到的数值，但是条目限制目前只有10000，如果有必要需要切换为sql获得
class Data:
    def __init__(self,since=None,until=None,):
        self.token = ssToken
        if since is None:
            now = time.time()
            before1days = time.localtime(now - 3600 * 24)
            before30days = time.localtime(now - 3600 * 24 * 30)
            since = time.strftime('%Y-%m-%d',before30days)
            until = time.strftime('%Y-%m-%d',before1days)
        self.since = since
        self.until = until
        sinceTime = datetime.datetime.strptime(since,'%Y-%m-%d')
        unitlTime = datetime.datetime.strptime(until,'%Y-%m-%d')
        self.t_1 = (sinceTime+datetime.timedelta(days=-7)).strftime('%Y%m%d')
        self.t0 = (sinceTime+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        self.t1 = (unitlTime+datetime.timedelta(days=8)).strftime('%Y-%m-%d')
        self.t2 = (unitlTime+datetime.timedelta(days=9)).strftime('%Y-%m-%d')
        # print(self.since,self.until,self.t_1,self.t0,self.t1,self.t2)

    # 数数平台sql获取
    # 目前采用 美国安卓 用户
    # 要求所有数数的sql都采用这套配置，注册日期从2021-08-01到2021-08-31，获取周期从'2021-08-01到2021-10-03
    def ssSql(self,sql):
        url = 'http://bishushukeji.rivergame.net/querySql'
        url += '?token='+self.token
        # 防止bug
        sql = sql.replace('2021-08-01','__self.since__')
        sql = sql.replace('2021-08-31','__self.until__')
        sql = sql.replace('2021-07-31','__self.t0__')
        sql = sql.replace('2021-10-03','__self.t1__')
        sql = sql.replace('2021-10-04','__self.t2__')
        sql = sql.replace('20210725','__self.t_1__')

        sql = sql.replace('__self.since__',self.since)
        sql = sql.replace('__self.until__',self.until)
        sql = sql.replace('__self.t1__',self.t1)
        sql = sql.replace('__self.t0__',self.t0)
        sql = sql.replace('__self.t2__',self.t2)
        sql = sql.replace('__self.t_1__',self.t_1)

        # sql = sql.replace('googleadwords_int','Facebook Ads')

        print(sql)

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
        return lines

    # 从数数获取iOS付费用户（24小时内付费）用户的一些属性
    def get24HPayUserInfo(self):
        sql = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,group_4,group_5,group_6,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,group_4,group_5,group_6,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@usd_amount"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1186" on logic_table."platform" = "dim_2_0_1186"."platform@platform" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20210725) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0,format_datetime("#vp@ctime_utc0",'yyyy-MM-dd HH:mm:ss.SSS') group_1,"ad_mediasource" group_2,"ad_campaign" group_3,"app_idfa" group_4,"country" group_5,"sys_system" group_6 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","app_idfa","ctime","country","sys_system","#update_time","#event_date","#user_id","ad_campaign","ad_mediasource" from v_user_2) where "#event_date" > 20210725))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2021-07-31' and '2021-10-04') and ("@vpc_tz_#event_time" >= timestamp '2021-08-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2021-10-03'))) and ((ta_ev."platform@platform_merge" IN ('iOS(海外)','iOS(国内)')) and (ta_ev."#vp@lifetime_hour" <= 24))) group by group_0,group_1,group_2,group_3,group_4,group_5,group_6,"$__Date_Time")) group by group_0,group_1,group_2,group_3,group_4,group_5,group_6)) ORDER BY total_amount DESC'''
        lines = self.ssSql(sql)
        # print(lines)

        ret = {}
        for line in lines:
            j = json.loads(line)
            uid = j[0]
            installDate = j[1]
            media = j[2]
            campaign = j[3]
            idfa = j[4]
            country = j[5]
            iOSVersion = j[6]
            usd = j[8]
            ret[uid] = {
                'uid':uid,
                'installDate':installDate,
                'media':media,
                'country':country,
                'campaign':campaign,
                'idfa':idfa,
                'iOSVersion':iOSVersion,
                'usd':usd
            }
            
        return ret

    # 简易版，只获取付费金额，用于计算cv值
    def get24HPayUserInfoEasy(self):
        # sql = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@usd_amount"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1186" on logic_table."platform" = "dim_2_0_1186"."platform@platform" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20210725) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_2) where "#event_date" > 20210725)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2021-07-31' and '2021-10-04') and ("@vpc_tz_#event_time" >= timestamp '2021-08-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2021-10-03'))) and ((ta_ev."#vp@lifetime_hour" <= 24) and (ta_ev."platform@platform_merge" IN ('iOS(海外)')))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC'''
        sql = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@usd_amount"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try(date_diff('hour', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_hour",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1186" on logic_table."platform" = "dim_2_0_1186"."platform@platform" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20210725) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "#account_id" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20210725))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'order_complete' ) ) )) and ((("$part_date" between '2021-07-31' and '2021-10-04') and ("@vpc_tz_#event_time" >= timestamp '2021-08-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2021-10-03'))) and ((ta_ev."#vp@lifetime_hour" <= 24) and (ta_ev."platform@platform_merge" IN ('iOS(海外)')) and ((ta_u."#vp@ctime_utc0" >= cast('2021-08-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2021-08-31 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC'''
        lines = self.ssSql(sql)
        
        ret = []
        for line in lines:
            j = json.loads(line)
            uid = j[0]
            usd = j[2]
            ret.append(usd)
            
        return ret

if __name__ == "__main__":
    d = Data(since='2022-08-01',until='2022-08-31')
    # ret = d.get24HPayUserInfo()
    # print(ret['835935156465'])
    d.get24HPayUserInfoEasy()