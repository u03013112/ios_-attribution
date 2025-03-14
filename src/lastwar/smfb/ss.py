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

# from src.config import ssToken
from src.config import ssTokenLastwar,ssUrlPrefixLastwar

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

def getAddScoreData(startDayStr='2024-11-25',endDayStr='2024-11-30'):
    sql = f'''
SELECT 
"#account_id",
date_trunc('week', "#event_time") wk,
sum(add_score) as add_score_sum
from ta.v_event_3 
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
    FROM ta.v_event_3 
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
    FROM v_event_3 
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
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
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
        "#event_time",
        -- 判断事件时间是周几
        CASE 
            -- 如果是周一至周三，归属于本周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 1 AND 3 THEN
                date_trunc('week', "#event_time") + INTERVAL '3' DAY
            -- 如果是周四至周六，归属于下周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 4 AND 6 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
            -- 周日单独处理
            WHEN EXTRACT(DOW FROM "#event_time") = 0 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
        END AS thursday
    FROM v_event_3 
    WHERE "$part_event" = 's_login' 
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
login_counts AS (
    SELECT 
        "#account_id",
        date_trunc('week', thursday - INTERVAL '3' DAY) AS wk,  -- 将周四转回周一
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
    GROUP BY "#account_id", date_trunc('week', thursday - INTERVAL '3' DAY)
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

def getData20250206():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        alliance_id,
        alliance_group,
        power1 + power2 + power3 AS total_power,
        CASE
            WHEN power1 >= 50 * 1000000 THEN 467
            WHEN power1 >= 48 * 1000000 THEN 144
            WHEN power1 >= 46 * 1000000 THEN 115
            WHEN power1 >= 44 * 1000000 THEN 86
            WHEN power1 >= 42 * 1000000 THEN 72
            WHEN power1 >= 40 * 1000000 THEN 60
            WHEN power1 >= 38 * 1000000 THEN 50
            WHEN power1 >= 36 * 1000000 THEN 40
            WHEN power1 >= 34 * 1000000 THEN 30
            WHEN power1 >= 32 * 1000000 THEN 20
            WHEN power1 >= 30 * 1000000 THEN 10
            WHEN power1 >= 28 * 1000000 THEN 5
            WHEN power1 >= 26 * 1000000 THEN 2
            WHEN power1 >= 24 * 1000000 THEN 1
            WHEN power1 >= 22 * 1000000 THEN 1
            WHEN power1 >= 20 * 1000000 THEN 1
            ELSE 0
        END +
        CASE
            WHEN power2 >= 50 * 1000000 THEN 467
            WHEN power2 >= 48 * 1000000 THEN 144
            WHEN power2 >= 46 * 1000000 THEN 115
            WHEN power2 >= 44 * 1000000 THEN 86
            WHEN power2 >= 42 * 1000000 THEN 72
            WHEN power2 >= 40 * 1000000 THEN 60
            WHEN power2 >= 38 * 1000000 THEN 50
            WHEN power2 >= 36 * 1000000 THEN 40
            WHEN power2 >= 34 * 1000000 THEN 30
            WHEN power2 >= 32 * 1000000 THEN 20
            WHEN power2 >= 30 * 1000000 THEN 10
            WHEN power2 >= 28 * 1000000 THEN 5
            WHEN power2 >= 26 * 1000000 THEN 2
            WHEN power2 >= 24 * 1000000 THEN 1
            WHEN power2 >= 22 * 1000000 THEN 1
            WHEN power2 >= 20 * 1000000 THEN 1
            ELSE 0
        END +
        CASE
            WHEN power3 >= 50 * 1000000 THEN 467
            WHEN power3 >= 48 * 1000000 THEN 144
            WHEN power3 >= 46 * 1000000 THEN 115
            WHEN power3 >= 44 * 1000000 THEN 86
            WHEN power3 >= 42 * 1000000 THEN 72
            WHEN power3 >= 40 * 1000000 THEN 60
            WHEN power3 >= 38 * 1000000 THEN 50
            WHEN power3 >= 36 * 1000000 THEN 40
            WHEN power3 >= 34 * 1000000 THEN 30
            WHEN power3 >= 32 * 1000000 THEN 20
            WHEN power3 >= 30 * 1000000 THEN 10
            WHEN power3 >= 28 * 1000000 THEN 5
            WHEN power3 >= 26 * 1000000 THEN 2
            WHEN power3 >= 24 * 1000000 THEN 1
            WHEN power3 >= 22 * 1000000 THEN 1
            WHEN power3 >= 20 * 1000000 THEN 1
            ELSE 0
        END AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE predicted_activity = 1
    GROUP BY alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;

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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","group_a","strength_old_a","strength_new_a","alliance_b_id","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_20241125_20250205.csv',index=False)


# 添加了服务器id
def getData20250206_serverId():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        CASE
            WHEN alliance_id = teamaallianceid THEN servera
            WHEN alliance_id = teamballianceid THEN serverb
        END AS server_id
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        server_id,
        alliance_id,
        alliance_group,
        power1 + power2 + power3 AS total_power,
        CASE
            WHEN power1 >= 50 * 1000000 THEN 467
            WHEN power1 >= 48 * 1000000 THEN 144
            WHEN power1 >= 46 * 1000000 THEN 115
            WHEN power1 >= 44 * 1000000 THEN 86
            WHEN power1 >= 42 * 1000000 THEN 72
            WHEN power1 >= 40 * 1000000 THEN 60
            WHEN power1 >= 38 * 1000000 THEN 50
            WHEN power1 >= 36 * 1000000 THEN 40
            WHEN power1 >= 34 * 1000000 THEN 30
            WHEN power1 >= 32 * 1000000 THEN 20
            WHEN power1 >= 30 * 1000000 THEN 10
            WHEN power1 >= 28 * 1000000 THEN 5
            WHEN power1 >= 26 * 1000000 THEN 2
            WHEN power1 >= 24 * 1000000 THEN 1
            WHEN power1 >= 22 * 1000000 THEN 1
            WHEN power1 >= 20 * 1000000 THEN 1
            ELSE 0.9
        END +
        CASE
            WHEN power2 >= 50 * 1000000 THEN 467
            WHEN power2 >= 48 * 1000000 THEN 144
            WHEN power2 >= 46 * 1000000 THEN 115
            WHEN power2 >= 44 * 1000000 THEN 86
            WHEN power2 >= 42 * 1000000 THEN 72
            WHEN power2 >= 40 * 1000000 THEN 60
            WHEN power2 >= 38 * 1000000 THEN 50
            WHEN power2 >= 36 * 1000000 THEN 40
            WHEN power2 >= 34 * 1000000 THEN 30
            WHEN power2 >= 32 * 1000000 THEN 20
            WHEN power2 >= 30 * 1000000 THEN 10
            WHEN power2 >= 28 * 1000000 THEN 5
            WHEN power2 >= 26 * 1000000 THEN 2
            WHEN power2 >= 24 * 1000000 THEN 1
            WHEN power2 >= 22 * 1000000 THEN 1
            WHEN power2 >= 20 * 1000000 THEN 1
            ELSE 0.9
        END +
        CASE
            WHEN power3 >= 50 * 1000000 THEN 467
            WHEN power3 >= 48 * 1000000 THEN 144
            WHEN power3 >= 46 * 1000000 THEN 115
            WHEN power3 >= 44 * 1000000 THEN 86
            WHEN power3 >= 42 * 1000000 THEN 72
            WHEN power3 >= 40 * 1000000 THEN 60
            WHEN power3 >= 38 * 1000000 THEN 50
            WHEN power3 >= 36 * 1000000 THEN 40
            WHEN power3 >= 34 * 1000000 THEN 30
            WHEN power3 >= 32 * 1000000 THEN 20
            WHEN power3 >= 30 * 1000000 THEN 10
            WHEN power3 >= 28 * 1000000 THEN 5
            WHEN power3 >= 26 * 1000000 THEN 2
            WHEN power3 >= 24 * 1000000 THEN 1
            WHEN power3 >= 22 * 1000000 THEN 1
            WHEN power3 >= 20 * 1000000 THEN 1
            ELSE 0.9
        END AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.server_id,
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        server_id,
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE predicted_activity = 1
    GROUP BY server_id,alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    as_a.server_id AS server_id_a,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    as_b.server_id AS server_id_b,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;
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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","server_id_a","group_a","strength_old_a","strength_new_a","alliance_b_id","server_id_b","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_server_id_20241125_20250205.csv',index=False)



# 简单版本，直接用简单办法计算匹配分数
def getData20250206_s():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        alliance_id,
        alliance_group,
        power1 + power2 * 0.6 + power3 * 0.3 + power4 * 0.1 AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE predicted_activity = 1
    GROUP BY alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;
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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","group_a","strength_old_a","strength_new_a","alliance_b_id","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_s_20241125_20250205.csv',index=False)

# 简单版本，直接用简单办法计算匹配分数
# 添加了服务器id
def getData20250206_s_serverId():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        CASE
            WHEN alliance_id = teamaallianceid THEN servera
            WHEN alliance_id = teamballianceid THEN serverb
        END AS server_id
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        server_id,
        alliance_id,
        alliance_group,
        power1 + power2 * 0.6 + power3 * 0.3 + power4 * 0.1 AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.server_id,
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        server_id,
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE predicted_activity = 1
    GROUP BY server_id,alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    as_a.server_id AS server_id_a,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    as_b.server_id AS server_id_b,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;
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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","server_id_a","group_a","strength_old_a","strength_new_a","alliance_b_id","server_id_b","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_s_server_id_20241125_20250205.csv',index=False)


def getData20250206_s2():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        alliance_id,
        alliance_group,
        (power1 + power2 * 0.6 + power3 * 0.3 + power4 * 0.1)/1000 AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new,
        ROW_NUMBER() OVER (PARTITION BY w.alliance_id, w.wk ORDER BY w.strength_new DESC) AS rank
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE predicted_activity = 1 AND rank <= 10
    GROUP BY alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;
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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","group_a","strength_old_a","strength_new_a","alliance_b_id","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_s2_20241125_20250205.csv',index=False)

def getData20250206_s2t():
    sql = '''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        strength,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER), 0) AS power1,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER), 0) AS power2,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER), 0) AS power3,
        COALESCE(CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER), 0) AS power4,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(strengthinfo) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
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
score_calculation AS (
    SELECT
        "#account_id",
        wk,
        power1,
        power2,
        power3,
        power4,
        alliance_id,
        alliance_group,
        (power1 + power2 * 0.6 + power3 * 0.3 + power4 * 0.1)/1000 AS strength_new
    FROM wk_account
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.alliance_id,
        w.alliance_group,
        COALESCE(s.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(s.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity,
        w.power1,
        w.power2,
        w.power3,
        w.power4,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE w.strength_new
        END AS strength_new,
        ROW_NUMBER() OVER (PARTITION BY w.alliance_id, w.wk ORDER BY w.strength_new DESC) AS rank
    FROM score_calculation w
    LEFT JOIN add_score_data s
    ON w.wk = s.wk AND w."#account_id" = s."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        strength AS strength_old
    FROM
        ta.v_event_3
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '2024-11-25' AND '2025-02-05'
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
    LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
    AND a.wk = b.wk
    AND a.min_id = b.min_id
    AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
),
alliance_scores AS (
    SELECT
        alliance_id,
        alliance_group,
        wk,
        SUM(strength_new) AS total_strength_new
    FROM prediction
    WHERE actual_activity = 1 AND rank <= 10
    GROUP BY alliance_id, alliance_group, wk
)
SELECT
    br.wk,
    br.alliance_a_id,
    ad_a.alliance_group AS group_a,
    ad_a.strength_old AS strength_old_a,
    COALESCE(as_a.total_strength_new, 0) AS strength_new_a,
    br.alliance_b_id,
    ad_b.alliance_group AS group_b,
    ad_b.strength_old AS strength_old_b,
    COALESCE(as_b.total_strength_new, 0) AS strength_new_b,
    br.score_a,
    br.score_b,
    br.is_quality
FROM
    battle_results br
LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
AND br.wk = ad_a.wk
LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
AND br.wk = ad_b.wk
LEFT JOIN alliance_scores as_a ON br.alliance_a_id = as_a.alliance_id
AND br.group_a = CAST(as_a.alliance_group AS VARCHAR)
AND br.wk = as_a.wk
LEFT JOIN alliance_scores as_b ON br.alliance_b_id = as_b.alliance_id
AND br.group_b = CAST(as_b.alliance_group AS VARCHAR)
AND br.wk = as_b.wk;
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

    df = pd.DataFrame(data,columns=["wk","alliance_a_id","group_a","strength_old_a","strength_new_a","alliance_b_id","group_b","strength_old_b","strength_new_b","score_a","score_b","is_quality"])
    df.to_csv(f'/src/data/20250206smfb_data_s2t_20241125_20250205.csv',index=False)




if __name__ == '__main__':
    # getAddScoreData(startDayStr='2024-11-25',endDayStr='2025-01-20')

    # getIndividualScoreTotalData(startDayStr='2024-11-04',endDayStr='2025-01-20')

    # getLoginData(startDayStr='2024-11-04',endDayStr='2025-01-20')

    # getData(startDayStr='2024-11-25',endDayStr='2025-01-20')

    # getData20250206()
    # getData20250206_s()
    # getData20250206_s2()
    # getData20250206_s2t()

    getData20250206_serverId()
    # getData20250206_s_serverId()