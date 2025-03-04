# 误差监测
# 针对3~36服 整体预测 的误差监测
# 和针对3~36服中最低收入服的预测的误差监测
# 每周一进行一次
import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
from datetime import date

import os
import re
import pandas as pd
import numpy as np

import sys
sys.path.append('/src')

from src.config import ssToken
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
        v_event_15
    WHERE
        "$part_event" = 's_pay_new'
        AND "$part_date" BETWEEN '2023-12-31'
        AND '{endday}'
),
user_data AS (
    SELECT
        "#user_id"
    FROM
        v_user_15
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
    AND e.lw_cross_zone IN ('APS3','APS4','APS5','APS6','APS7','APS8','APS9','APS10','APS11','APS12','APS13','APS14','APS15','APS16','APS17','APS18','APS19','APS20','APS21','APS22','APS23','APS24','APS25','APS26','APS27','APS28','APS29','APS30','APS31','APS32','APS33','APS34','APS35','APS36')
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


# 获得误差，哪找days的间隔，比如days=7，就是每周的误差
# 注意，需要保证尽量周一进行，并且预测也是周一进行
def getTotalMape(days = 7):
    # for debug，设置今天是2025-03-03
    today = pd.to_datetime('2025-03-03')

    # 改为获取昨日数据，因为今日数据可能不完整
    # today = date.today()
    todayStr = today.strftime('%Y-%m-%d')
    
    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    realDataDf = getData(yesterdayStr)
    realDataTotalDf = realDataDf.groupby('day').agg({'revenue':'sum'}).reset_index()
    realDataTotalDf.rename(columns={'day':'ds'},inplace=True)
    realDataTotalDf['ds'] = pd.to_datetime(realDataTotalDf['ds'], errors='coerce')
    print('realDataTotalDf:')
    print(realDataTotalDf)

    # 找到所有预测数据
    # 在 '/src/data/' 中 找到所有类似lastwarPredictRevenue3_36_sum_{yyyy-mm-dd}.csv的文件
    files = os.listdir('/src/data/')
    predictFiles = []
    # 定义正则表达式模式来匹配日期格式
    pattern = re.compile(r'^lastwarPredictRevenue3_36_sum_\d{4}-\d{2}-\d{2}\.csv$')

    # 遍历文件列表，筛选符合条件的文件
    for file in files:
        if pattern.match(file):
            predictFiles.append(file)

    # 输出符合条件的文件列表
    print(predictFiles)

    predictDf = pd.DataFrame()
    for file in predictFiles:
        df = pd.read_csv(f'/src/data/{file}')
        # 只保留预测数据，即revenue为空的数据
        df = df[df['revenue'].isna()]
        predictDf = predictDf.append(df)
    
    predictDf = predictDf.sort_values(['day','ds']).reset_index(drop=True)
    predictDf['ds'] = pd.to_datetime(predictDf['ds'], errors='coerce')
    predictDf['day'] = pd.to_datetime(predictDf['day'], errors='coerce')
    # 间隔days = day - ds 的天数
    predictDf['deltaDays'] = (predictDf['ds'] - predictDf['day']).dt.days
    predictDf['deltaDays//days'] = predictDf['deltaDays'] // days
    print('predictDf:')
    print(predictDf)

    predictDf = predictDf[['ds','predict2','day','deltaDays//days']]
    if predictDf.empty or realDataTotalDf.empty:
        print('predictDf or realDataTotalDf is empty')
        return

    predictDf = predictDf.merge(realDataTotalDf,on='ds',how='left')
    
    # 将没有真实数据的预测数据删除
    predictDf = predictDf.dropna(subset=['revenue'])
    print('predictDf:')
    print(predictDf)
    
    # 按照预测日和间隔天数分组，计算预测值和真实值的和
    predictDf = predictDf.groupby(['day','deltaDays//days']).agg({'revenue':'sum','predict2':'sum'}).reset_index()
    # 计算mape
    predictDf['mape'] = abs(predictDf['revenue'] - predictDf['predict2']) / predictDf['revenue']
    print('predictDf:')
    print(predictDf)

    mapeDf = predictDf.groupby('deltaDays//days').agg({'mape':'mean'}).reset_index()
    print('mapeDf:')
    print(mapeDf)



if __name__ == "__main__":
    getTotalMape()