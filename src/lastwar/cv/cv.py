# 计算cv map

import time
import json

import requests
from urllib import parse
from requests.adapters import HTTPAdapter

import pandas as pd

import sys
sys.path.append('/src')

# from src.config import ssToken2 as ssToken
from src.config import ssUrlPrefixLastwar,ssTokenLastwar
from src.tools import printProgressBar

# 异步执行数数的查询
def ssSql(sql):
    # url = 'http://123.56.188.109/open/submit-sql'
    # url += '?token='+ssToken
    url = ssUrlPrefixLastwar + 'open/submit-sql'
    url += '?token='+ssTokenLastwar
    
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
        # url2 = 'http://123.56.188.109/open/sql-task-info'
        # url2 += '?token='+ssToken+'&taskId='+taskId
        url2 = ssUrlPrefixLastwar + 'open/sql-task-info'
        url2 += '?token='+ssTokenLastwar+'&taskId='+taskId
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
                    # url3 = 'http://123.56.188.109/open/sql-result-page'
                    # url3 += '?token='+ssToken+'&taskId='+taskId+'&pageId=%d'%p
                    url3 = ssUrlPrefixLastwar + 'open/sql-result-page'
                    url3 += '?token='+ssTokenLastwar+'&taskId='+taskId+'&pageId=%d'%p

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


def getData1(sql,filename):
    # sql = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."usd"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((8-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('second', "#install_time", "#event_time")) as double) "#vp@life_time_second" from (select "#event_name","#event_time","usd","#zone_offset","#user_id","#install_time","$part_date","$part_event" from v_event_15)))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20230913)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 's_pay' ) ) )) and ((("$part_date" between '2023-09-19' and '2023-10-20') and ("@vpc_tz_#event_time" >= timestamp '2023-09-20' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-10-19'))) and (ta_ev."#vp@life_time_second" <= 8.64E+4)) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC limit 10'''

    lines = ssSql(sql)
    # print('lines:',len(lines))
    # print('lines[0]:',lines[0])


    df = pd.DataFrame(columns=['uid','install_date','payUsd'])
    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue

        uid = lineJ[0]
        installDate = lineJ[1]
        payUsd = lineJ[3]

        df_new = pd.DataFrame([[uid,installDate,payUsd]], columns=df.columns)
        df = df.append(df_new, ignore_index=True)
    
        df.to_csv(filename,index=False)

    return df

def makeLevels1(userDf, usd='r1usd', N=32):    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N - 1)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    # levels 排重
    levels = list(set(levels))
    # levels 去掉0
    levels.remove(0)
    # levels 排序
    levels.sort()
    max = levels[len(levels)-1]
    # levels[N-2] = 1000
    return levels

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    # 最后再加一档无上限
    # mapData['cv'].append(len(mapData['cv']))
    # mapData['min_event_revenue'].append(max)
    # mapData['max_event_revenue'].append(max)
    # mapData['avg'].append(max)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf[cv].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    return userDfCopy

def checkCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    # cvMapDf.loc[cvMapDf[cv]==1,'avg'] = 0.99
    addCvDf = addCv(userDf,cvMapDf,usd,cv)
    df = addCvDf.merge(cvMapDf,on=[cv],how='left')
    
    tmpDf = df.groupby([cv]).agg({usd:'sum','avg':'sum'}).reset_index()
    tmpDf['usd/usdSum'] = tmpDf[usd]/tmpDf[usd].sum()
    tmpDf['avg/avgSum'] = tmpDf['avg']/tmpDf['avg'].sum()
    print(tmpDf)

    df = df.groupby(['install_date']).agg({usd:'sum','avg':'sum'}).reset_index()
    df['mape'] = abs(df[usd] - df['avg']) / df[usd]
    print('mape:',df['mape'].mean())

def main1():
    # df = pd.read_csv('/src/data/zk2/lastwar20230920_20231019_allPay.csv')

    df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
    N = 32
    for i in range (N,100):
        levels = makeLevels1(df,usd='payUsd',N=i)
        if len(levels) >= N-1:
            print('N:',i,'levels:',len(levels))
            break
    
    cvMapDf = makeCvMap(levels)
    print(cvMapDf)
    cvMapDf.to_csv('/src/data/zk2/lastwar20230901_20231123_allPay_cvMap.csv',index=False)
    checkCv(df,cvMapDf,usd='payUsd',cv='cv')

def main2():
    # 强制第一个档位位1.98
    # 所以将小于等于1.98的用户排除，然后再分31个档位，然后再把1.98加回去

    df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
    df = df.loc[df['payUsd'] > 1.98]
    N = 31
    for i in range (N,100):
        levels = makeLevels1(df,usd='payUsd',N=i)
        if len(levels) >= N-1:
            print('N:',i,'levels:',len(levels))
            break
    
    levels.insert(0,1.98)
    cvMapDf = makeCvMap(levels)
    print(cvMapDf)

    cvMapDf.to_csv('/src/data/lastwarCvMap2.csv',index=False)

def check():
    # sql = '''
    #     select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@closing_currency__price_local__USD"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, try_cast(try(ROUND((("price_local" / IF(("closing_currency" = 'USD'), 1, ex_currency_agg["closing_currency"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@closing_currency__price_local__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((8-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('hour', lw_register_date, "#event_time")) as double) "#vp@lifetime_hour" from (select "#event_name","lw_register_date","price_local","#event_time","#zone_offset","#user_id","closing_currency","$part_date","$part_event" from v_event_15))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '2023-11-24')) ex_currency_agg FROM (( SELECT sequence(date '2023-11-24', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-30 and "#zone_offset"<=30, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd')))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20230825)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 's_pay_new' ) ) )) and ((("$part_date" between '2023-08-31' and '2023-11-24') and ("@vpc_tz_#event_time" >= timestamp '2023-09-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-11-23'))) and (ta_ev."#vp@lifetime_hour" <= 24)) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    # '''
    # getData1(sql,'/src/data/zk2/lastwar20230901_20231023_allPay.csv')

    # df = pd.read_csv('/src/data/lastwar_pay_20230901_20231123.csv')
    # # print(len(df))
    # # print(df['注册时间(UT0)'].min())
    # df.rename(columns={'s_pay_new.美元付费金额 - USD(每日汇率)总和':'payUsd','注册时间(UT0)':'install_date'},inplace=True)
    # df = df[['payUsd','install_date']]
    # df['install_date'] = pd.to_datetime(df['install_date'],format='%Y-%m-%d')
    # df['payUsd'] = df['payUsd'].astype(float)

    # df = df.loc[df['install_date']>='2023-11-01']

    # df.to_csv('/src/data/lastwar_pay2_20230901_20231123.csv',index=False)

    df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
    
    cvMapDf = pd.read_csv('/src/src/lastwar/cv/cvMap20231124.csv')
    cvMapDf = cvMapDf.loc[cvMapDf['conversion_value']<32][['conversion_value','min_event_revenue','max_event_revenue']].fillna(0)
    cvMapDf['avg'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue'])/2
    print(cvMapDf)
    checkCv(df,cvMapDf,usd='payUsd',cv='conversion_value')

    cvMapDf = pd.read_csv('/src/src/lastwar/cv/cvMap20231205.csv')
    cvMapDf = cvMapDf.loc[cvMapDf['conversion_value']<32][['conversion_value','min_event_revenue','max_event_revenue']].fillna(0)
    cvMapDf['avg'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue'])/2
    print(cvMapDf)
    checkCv(df,cvMapDf,usd='payUsd',cv='conversion_value')

    cvMapDf = pd.read_csv('/src/data/lastwarCvMap2.csv')
    print(cvMapDf)
    checkCv(df,cvMapDf,usd='payUsd',cv='cv')

# def debug():
#     df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
#     df = df.loc[df['payUsd'] > 0]
#     df = df.sort_values(['payUsd'],ascending=True)
#     print(df.head(10))


def check2():
    df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
    
    # 需要将payUsd列去重，然后要计算每一种payUsd的合计金额占所有金额的比例
    df['payUsdSum'] = df['payUsd']
    # 将payUsd四舍五入，保留1位小数
    df['payUsd'] = df['payUsd'].round(1)
    df = df.loc[df['payUsd'] <= 3]
    df = df.groupby(['payUsd']).agg({'payUsdSum':'sum'}).reset_index()
    df['payUsdSumRate'] = df['payUsdSum']/df['payUsdSum'].sum()
    # df['payUsdSumRate'] = df['payUsdSumRate'].round(5)
    # payUsdSumRate 改为.2f%格式
    df['payUsdSumRate'] = df['payUsdSumRate'].apply(lambda x: '%.2f%%' % (x * 100))
    df = df.sort_values(['payUsd'],ascending=True)
    df = df[['payUsd','payUsdSumRate']]
    df.to_csv('/src/data/report/iOS2_20230401_20230731/check2.csv')


def check3():
    df = pd.read_csv('p1.csv')
    df = df [['支付币种','S新支付.美元付费金额 - USD(每日汇率)总和','S新支付.price_local总和']]
    df.rename(columns={'支付币种':'currency','S新支付.美元付费金额 - USD(每日汇率)总和':'payUsd','S新支付.price_local总和':'payLocal'},inplace=True)
    # print(df.columns)
    df = df.groupby(['currency','payUsd','payLocal']).sum().reset_index()
    df = df.sort_values(['payUsd','currency','payLocal'],ascending=True).reset_index(drop=True)
    df = df[df['payUsd'] <= 3]
    df.to_csv('/src/data/report/iOS2_20230401_20230731/check3.csv')

if __name__ == '__main__':
    
    # sql = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(SUM(ta_ev."usd"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((8-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('second', "#install_time", "#event_time")) as double) "#vp@life_time_second" from (select "#event_name","#event_time","usd","#zone_offset","#user_id","#install_time","$part_date","$part_event" from v_event_15)))) ta_ev inner join (select *, "#account_id" group_0,format_datetime(ta_date_trunc('day',"lwu_register_date",1),'yyyy-MM-dd') group_1 from (select * from (select "lwu_register_date","#account_id","#update_time","#event_date","#user_id" from v_user_15) where "#event_date" > 20230814)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 's_pay' ) ) )) and ((("$part_date" between '2023-08-20' and '2023-10-20') and ("@vpc_tz_#event_time" >= timestamp '2023-08-21' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-10-19'))) and (ta_ev."#vp@life_time_second" <= 8.64E+4)) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC'''
    # getData1(sql,'/src/data/zk2/lastwar20230920_20231019_allPay.csv')

    # sql2 = '''select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(SUM(ta_ev."usd"), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((8-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('second', "#install_time", "#event_time")) as double) "#vp@life_time_second" from (select "#event_name","#event_time","usd","#zone_offset","#user_id","#install_time","$part_date","$part_event" from v_event_15)))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id","lwu_platform" from v_user_15) where "#event_date" > 20230913)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 's_pay' ) ) )) and ((("$part_date" between '2023-09-19' and '2023-10-20') and ("@vpc_tz_#event_time" >= timestamp '2023-09-20' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-10-19'))) and ((ta_ev."#vp@life_time_second" <= 8.64E+4) and (ta_u."lwu_platform" IN ('AppStore')))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC'''
    # getData1(sql2,'/src/data/zk2/lastwar20230920_20231019_appstorePay.csv')

    

    

    main1()

    # main2()

    # check()


    # check2()

    # check3()

    