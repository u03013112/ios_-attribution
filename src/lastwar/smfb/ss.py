# 训练所需数据获得，与预测数据主要区别是，获取多天数据
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



def getAddScoreData(startDayStr='2024-11-25',endDayStr='2024-11-30'):
    sql = f'''
SELECT 
"#account_id",
date_trunc('week', "#event_time") wk,
sum(add_score) as add_score_sum
from ta.v_event_15 
WHERE 
("$part_event" = 's_desertStorm_point') AND  "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
AND add_score>0
AND minute("#event_time") <= 15
group by 1,2
    '''
    lines = ssSql(sql=sql)

    print('lines:',len(lines))
    print(lines[:10])

    # 得到结论类似以下格式：
    # lines: 4
    # ['["1242741320000458","2024-11-25 00:00:00.000",610598.0]', '["1106005193000458","2024-11-25 00:00:00.000",1292451.0]', '["1365713733000458","2024-11-25 00:00:00.000",517384.0]', '']
    # 每行是个json数组，第一个元素是账号id，第二个元素是日期，第三个元素是分数
    # 放入一个DataFrame中，列名："#account_id","wk","add_score_sum"
    
    data = []
    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)
    df = pd.DataFrame(data,columns=["#account_id","wk","add_score_sum"])
    df.to_csv(f'/src/data/add_score_{startDayStr}_{endDayStr}.csv',index=False)
    print('save to /src/data/add_score_%s_%s.csv'%(startDayStr,endDayStr))
    return df

# 获取用户的个人积分历史数据，因为要检查过去历史3场的个人积分，所以要从2024-11-04开始
def getIndividualScoreTotalData(startDayStr='2024-11-04',endDayStr='2025-01-20'):
    sql = f'''
WITH ranked_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        sum(individual_score_total) AS individual_score_total,
        ROW_NUMBER() OVER (PARTITION BY "#account_id" ORDER BY date_trunc('week', "#event_time")) AS rn
    FROM ta.v_event_15 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
    GROUP BY "#account_id", date_trunc('week', "#event_time")
)
SELECT 
    "#account_id",
    wk,
    individual_score_total,
    COALESCE(
        AVG(individual_score_total) OVER (
            PARTITION BY "#account_id" 
            ORDER BY rn 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0
    ) AS individual_score_total_mean
FROM ranked_data
ORDER BY "#account_id", wk;
'''
    lines = ssSql(sql=sql)

    print('lines:',len(lines))
    print(lines[:10])

    data = []
    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)

    df = pd.DataFrame(data,columns=["#account_id","wk","individual_score_total","individual_score_total_mean"])
    df.to_csv(f'/src/data/individual_score_total_{startDayStr}_{endDayStr}.csv',index=False)
    print('save to /src/data/individual_score_total_%s_%s.csv'%(startDayStr,endDayStr))
    return df


def getLoginData(startDayStr='2024-11-04',endDayStr='2025-01-20'):
    sql = f'''
WITH base_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,  -- 保持周一
        date_trunc('week', "#event_time") + INTERVAL '3' DAY AS thursday,  -- 计算周四
        "#event_time"
    FROM v_event_15 
    WHERE "$part_event" = 's_login' AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
)
SELECT 
    "#account_id",
    wk,
    COUNT(CASE 
        WHEN "#event_time" >= thursday - INTERVAL '3' DAY 
        AND "#event_time" < thursday
        THEN 1 
        ELSE NULL 
    END) AS "3day_login_count",
    COUNT(CASE 
        WHEN "#event_time" >= thursday - INTERVAL '7' DAY 
        AND "#event_time" < thursday
        THEN 1 
        ELSE NULL 
    END) AS "7day_login_count"
FROM base_data
GROUP BY "#account_id", wk
ORDER BY "#account_id", wk;
'''

    lines = ssSql(sql=sql)

    print('lines:',len(lines))
    print(lines[:10])

    data = []
    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)

    df = pd.DataFrame(data,columns=["#account_id","wk","3day_login_count","7day_login_count"])
    df.to_csv(f'/src/data/login_{startDayStr}_{endDayStr}.csv',index=False)

# 沙漠风暴 目前线上计算 是否出站数据获得
# TODO：7日登录次数，目前统计有误，和3日登陆的一致，需要修正

def getData(startDayStr='2024-11-04',endDayStr='2025-01-20'):
    sql = f'''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength
    FROM ta.v_event_15,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND  "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_15 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND  "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND add_score > 0
        AND minute("#event_time") <= 15
    GROUP BY "#account_id", date_trunc('week', "#event_time")
),
ranked_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(individual_score_total) AS individual_score_total,
        ROW_NUMBER() OVER (PARTITION BY "#account_id" ORDER BY date_trunc('week', "#event_time")) AS rn
    FROM ta.v_event_15 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND  "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
    GROUP BY "#account_id", date_trunc('week', "#event_time")
),
individual_score_mean AS (
    SELECT 
        "#account_id",
        wk,
        individual_score_total,
        COALESCE(
            AVG(individual_score_total) OVER (
                PARTITION BY "#account_id" 
                ORDER BY rn 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ), 0
        ) AS individual_score_total_mean
    FROM ranked_data
),
base_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,  -- 保持周一
        date_trunc('week', "#event_time") + INTERVAL '3' DAY AS thursday,  -- 计算周四
        "#event_time"
    FROM v_event_15 
    WHERE "$part_event" = 's_login' AND  "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
login_counts AS (
    SELECT 
        "#account_id",
        wk,
        COUNT(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '3' DAY 
            AND "#event_time" < thursday
            THEN 1 
            ELSE NULL 
        END) AS "3day_login_count",
        COUNT(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '7' DAY 
            AND "#event_time" < thursday
            THEN 1 
            ELSE NULL 
        END) AS "7day_login_count"
    FROM base_data
    GROUP BY "#account_id", wk
)
SELECT
    w.wk,
    w."#account_id",
    w.strength,
    COALESCE(a.add_score_sum, 0) AS add_score_sum,
    CASE 
        WHEN COALESCE(a.add_score_sum, 0) > 0 THEN 1
        ELSE 0
    END AS activity,
    COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
    COALESCE(l."3day_login_count", 0) AS "3day_login_count",
    COALESCE(l."7day_login_count", 0) AS "7day_login_count"
FROM wk_account w
LEFT JOIN add_score_data a
ON w.wk = a.wk AND w."#account_id" = a."#account_id"
LEFT JOIN individual_score_mean i
ON w.wk = i.wk AND w."#account_id" = i."#account_id"
LEFT JOIN login_counts l
ON w.wk = l.wk AND w."#account_id" = l."#account_id"
ORDER BY w."#account_id", w.wk;
    '''

    lines = ssSql(sql=sql)

    print('lines:',len(lines))
    print(lines[:10])

    data = []

    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)

    df = pd.DataFrame(data,columns=["wk","#account_id","strength","add_score_sum","activity","individual_score_total_mean","3day_login_count","7day_login_count"])
    df.to_csv(f'/src/data/20250121smfb_data_20241125_20250120.csv',index=False)


if __name__ == '__main__':
    # getAddScoreData(startDayStr='2024-11-25',endDayStr='2025-01-20')

    # getIndividualScoreTotalData(startDayStr='2024-11-04',endDayStr='2025-01-20')

    # getLoginData(startDayStr='2024-11-04',endDayStr='2025-01-20')

    getData(startDayStr='2024-11-25',endDayStr='2025-01-20')