# 计算最近一个月的数据，查看最小收入占比的前N服务器
# 如果出现3~36服以外的服务器，要进行告警
# 其中N可以用其他方式计算，比如获取最小收入占比5倍的阈值，将小于这个阈值的服务器计入告警范围

import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
from datetime import date

import os
import pandas as pd
import numpy as np
from prophet import Prophet

import matplotlib.pyplot as plt

import sys
sys.path.append('/src')

# from src.config import ssToken
from src.config import ssUrlPrefixLastwar,ssTokenLastwar
from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addText,addFile,sendMessage,addImage,addCode,sendMessageToWebhook,sendMessageToWebhook2

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

def getData(endday='2025-02-25'):
    filename = f'/src/data/lastwarPredictRevenue3_36_sum_data_{endday}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        print(f'load file {filename}')
    else:
        #希望获得2025-01-01 到 endday 的数据，但是发现第一天的数据不完整，应该是由于时区导致的，所以从2024-12-31开始获取数据 
        sql = f'''
WITH event_data AS (
    SELECT
        lw_cross_zone,
        DATE(
            IF(
                "#zone_offset" IS NOT NULL
                AND "#zone_offset" BETWEEN -30
                AND 30,
                DATE_ADD(
                    'second',
                    CAST((0 - "#zone_offset") * 3600 AS INTEGER),
                    "#event_time"
                ),
                "#event_time"
            )
        ) AS event_date,
        usd,
        "#user_id"
    FROM
        v_event_3
    WHERE
        "$part_event" = 's_pay_new'
        AND "$part_date" BETWEEN '2023-12-31'
        AND '{endday}'
),
user_data AS (
    SELECT
        "#user_id"
    FROM
        v_user_3
    WHERE
        "lwu_is_gm" IS NOT NULL
)
SELECT
    e.event_date as day,
    e.lw_cross_zone as server_id,
    ROUND(
        SUM(
            e.usd
        ),
        4
    ) AS revenue
FROM
    event_data e
    LEFT JOIN user_data u ON e."#user_id" = u."#user_id"
WHERE
    e.event_date BETWEEN DATE '2023-12-31'
    AND DATE '{endday}'
    AND u."#user_id" IS NULL
GROUP BY
    e.lw_cross_zone,
    e.event_date
ORDER BY
    revenue DESC;
        '''

        lines = ssSql(sql=sql)

        # print('lines:',len(lines))
        # print(lines[:10])

        data = []
        for line in lines:
            if line == '':
                continue
            j = json.loads(line)
            data.append(j)
        df = pd.DataFrame(data,columns=["day","server_id","revenue"])

        # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
        df['day'] = pd.to_datetime(df['day'], errors='coerce')
        df = df.dropna(subset=['day'])

        # 将server_id转换为整数
        def convert_server_id(server_id):
            try:
                return int(server_id[3:])
            except:
                return np.nan

        df = df[df['server_id'] != '(null)']        
        df['server_id'] = df['server_id'].apply(convert_server_id)
        df = df.dropna(subset=['server_id'])

        # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)

        # 重新过滤一下数据，只获取2024-01-01 到 endday 的数据
        df = df[df['day'] >= '2024-01-01']

        df.to_csv(filename, index=False)

    return df

# 将day_diff作为季节性因子，舍弃自然季节性
# 一个简单的尝试
# 舍弃day列，建立一个新的ds列，ds是从1970-01-01开始，加上day_diff天
# 然后用一部分服务器作为训练集，一部分服务器作为测试集
# 分别计算训练集和测试集的日MAPE，周MAPE，月MAPE
def prophetTest1(df):
    df['ds'] = pd.to_datetime('1970-01-01') + pd.to_timedelta(df['day_diff'],unit='D')
    serverIdList = list(df['server_id'].unique())
    serverIdList = sorted(serverIdList)
    # 取前80%的服务器作为训练集
    trainServerIdList = serverIdList[:int(len(serverIdList)*0.8)]
    # 取后20%的服务器作为测试集
    testServerIdList = serverIdList[int(len(serverIdList)*0.8):]

    trainDf = df[df['server_id'].isin(trainServerIdList)][['ds','revenue']]
    testDf = df[df['server_id'].isin(testServerIdList)][['ds','revenue']]

    model = Prophet()
    model.fit(trainDf)


    
def draw(df):
    # 目前数据结果是：从119服开始~1165服结束
    # 进行取整操作，从201~1100，每100个分一组，然后收入按分组求和

    # df['server_id'] = df['server_id'].apply(lambda x:int(x))
    # df = df[(df['server_id'] >= 200) & (df['server_id'] < 1100)]

    # df['server_id_group'] = df['server_id'] // 100 * 100

    # group100Df = df.groupby(['server_id_group','day_diff']).agg({'revenue':'sum'}).reset_index()

    group100Df = df.groupby(['start_day','day_diff']).agg({'revenue':'sum'}).reset_index()

    # 画图
    plt.figure(figsize=(18, 6))
    startDayList = list(group100Df['start_day'].unique())
    startDayList = sorted(startDayList)

    for startDay in startDayList:
        serverDf = group100Df[group100Df['start_day'] == startDay]
        plt.plot(serverDf['day_diff'],serverDf['revenue'],label=str(startDay))

    plt.xlabel('server start days')
    plt.ylabel('Revenue')
    plt.title(f'Server ltv')
    
    plt.legend()
    plt.grid()
    plt.savefig('/src/data/lastwarServerRevenueDraw.png')
    plt.close()

    


def main(today = None):
    if today is None:
        today = date.today()

    todayStr = today.strftime('%Y-%m-%d')

    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    df = getData(yesterdayStr)
    df['day'] = pd.to_datetime(df['day'])

    # print(df.head())

    serverIdList = list(df['server_id'].unique())
    serverIdList = sorted(serverIdList)

    # 开服时间表
    serverStartDf = pd.DataFrame()

    for serverId in serverIdList:
        serverDf = df[df['server_id'] == serverId]
        serverDf = serverDf.sort_values(['day'],ascending=True)
        serverStartDay = serverDf['day'].iloc[0]

        # print('serverId:',serverId,'start:',serverStartDay)
        serverStartDf = serverStartDf.append({'server_id':serverId,'start_day':serverStartDay},ignore_index=True)

    # print(serverStartDf.head())
    # server_id  start_day
    # 0        3.0 2024-01-01
    # 1        4.0 2024-01-01
    # 2        5.0 2024-01-01
    # 3        6.0 2024-01-01
    # 4        7.0 2024-01-01

    # 过滤掉开服过早的服务器
    serverStartDf = serverStartDf[serverStartDf['start_day'] > pd.to_datetime('2024-01-01')]
    # 过滤掉开服过晚的服务器，至少开服90天
    serverStartDf = serverStartDf[serverStartDf['start_day'] < pd.to_datetime(yesterday - pd.Timedelta(days=90))]
    

    # 整理数据，将现有的数据添加开服时间与开服天数
    df = df.merge(serverStartDf,on='server_id',how='right')
    df['day_diff'] = (df['day'] - df['start_day']).dt.days
    # print(df.head())

    
    serverIdList = list(df['server_id'].unique())
    serverIdList = sorted(serverIdList)
    # print('serverIdList:',serverIdList)
    # 目前数据结果是：从119服开始~1165服结束

    # prophetTest1(df)
    draw(df)

if __name__ == '__main__':
    main()