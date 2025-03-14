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
def getTotalMape(reportData,today = None,days = 7):
    
    # 改为获取昨日数据，因为今日数据可能不完整
    if today is None:
        today = date.today()

    todayStr = today.strftime('%Y-%m-%d')
    reportData['todayStr'] = todayStr

    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    realDataDf = getData(yesterdayStr)
    realDataTotalDf = realDataDf.groupby('day').agg({'revenue':'sum'}).reset_index()
    realDataTotalDf.rename(columns={'day':'ds'},inplace=True)
    realDataTotalDf['ds'] = pd.to_datetime(realDataTotalDf['ds'], errors='coerce')
    print('realDataTotalDf:')
    print(realDataTotalDf)
    realDataTotalDf.to_csv('/src/data/lastwar_getTotalMape_realDataTotalDf.csv',index=False)
    reportData['lastwar_getTotalMape_realDataTotalDf.csv'] = '/src/data/lastwar_getTotalMape_realDataTotalDf.csv'

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

    reportData['getTotalMape_predictFiles'] = predictFiles
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
    predictDf.to_csv('/src/data/lastwar_getTotalMape_predictDf1.csv',index=False)

    reportData['lastwar_getTotalMape_predictDf1.csv'] = '/src/data/lastwar_getTotalMape_predictDf1.csv'

    predictDf = predictDf[['ds','predict2','day','deltaDays//days']]
    if predictDf.empty or realDataTotalDf.empty:
        print('predictDf or realDataTotalDf is empty')
        return

    predictDf = predictDf.merge(realDataTotalDf,on='ds',how='left')
    
    # 将没有真实数据的预测数据删除
    predictDf = predictDf.dropna(subset=['revenue'])
    print('predictDf:')
    print(predictDf)
    predictDf.to_csv('/src/data/lastwar_getTotalMape_predictDf2.csv',index=False)

    reportData['lastwar_getTotalMape_predictDf2.csv'] = '/src/data/lastwar_getTotalMape_predictDf2.csv'

    # 按照预测日和间隔天数分组，计算预测值和真实值的和
    predictDf = predictDf.groupby(['day','deltaDays//days']).agg({'revenue':'sum','predict2':'sum'}).reset_index()
    # 计算mape
    predictDf['mape'] = abs(predictDf['revenue'] - predictDf['predict2']) / predictDf['revenue']
    print('predictDf:')
    print(predictDf)
    predictDf.to_csv('/src/data/lastwar_getTotalMape_predictDf3.csv',index=False)

    reportData['lastwar_getTotalMape_predictDf3.csv'] = '/src/data/lastwar_getTotalMape_predictDf3.csv'

    mapeDf = predictDf.groupby('deltaDays//days').agg({'mape':'mean'}).reset_index()
    print('mapeDf:')
    print(mapeDf)
    mapeDf.to_csv('/src/data/lastwar_getTotalMape_mapeDf.csv',index=False)
    reportData['lastwar_getTotalMape_mapeDf.csv'] = '/src/data/lastwar_getTotalMape_mapeDf.csv'

# 废弃，思路不清醒的时候写的
def getMinMape(today = None,days = 7):
    # 改为获取昨日数据，因为今日数据可能不完整
    if today is None:
        # today = date.today()

        # for debug，设置今天是2025-03-03
        today = pd.to_datetime('2025-03-03')
    todayStr = today.strftime('%Y-%m-%d')
    
    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    realDataDf = getData(yesterdayStr)
    
    realDataTotalDf = realDataDf.copy()
    realDataTotalDf.rename(columns={'day':'ds'},inplace=True)
    realDataTotalDf['ds'] = pd.to_datetime(realDataTotalDf['ds'], errors='coerce')
    realDataTotalDf2 = realDataTotalDf.copy()
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
    predictDf.to_csv('/src/data/lastwar_getMinMape_predictDf1.csv',index=False)

    # last3monthBegin & last3monthEnd 是列 day 的向前3个自然月，不包括day所在月，比如day是2025-03-03，那么last3monthBegin是2024-12-01，last3monthEnd是2025-02-28
    predictDf['last3monthEnd'] = predictDf['day'].apply(lambda x: x.replace(day=1) - pd.Timedelta(days=1))
    predictDf['last3monthBegin'] = predictDf['last3monthEnd'].apply(lambda x: x.replace(day=1) - pd.DateOffset(months=2))
    
    predictDf.to_csv('/src/data/lastwar_getMinMape_predictDf2.csv',index=False)

    realDataTotalDf['month'] = realDataTotalDf['ds'].apply(lambda x: x.replace(day=1))
    realDataTotalDf = realDataTotalDf.groupby(['month','server_id']).agg({'revenue':'sum'}).reset_index()

    # 计算每月的收入总和和最小收入
    summaryDf = realDataTotalDf.groupby('month').agg(
        revenue_sum=('revenue', 'sum'),
        revenue_min=('revenue', 'min')
    ).reset_index()

    # 获取每月最小收入对应的 server_id
    min_server_ids = realDataTotalDf.loc[
        realDataTotalDf.groupby('month')['revenue'].idxmin(), ['month', 'server_id']
    ].rename(columns={'server_id': 'revenue_min_server_id'})

    # 合并结果
    summaryDf = summaryDf.merge(min_server_ids, on='month')
    summaryDf['min/sum'] = summaryDf['revenue_min'] / summaryDf['revenue_sum']
    summaryDf.to_csv('/src/data/lastwar_getMinMape_predictDf3.csv',index=False)

    def find_min_ratio(row):
        mask = (summaryDf['month'] >= row['last3monthBegin']) & (summaryDf['month'] <= row['last3monthEnd'])
        filtered_summary = summaryDf[mask]
        if not filtered_summary.empty:
            min_row = filtered_summary.loc[filtered_summary['min/sum'].idxmin()]
            return pd.Series([min_row['revenue_min_server_id'], min_row['min/sum']])
        else:
            return pd.Series([None, None])

    predictDf[['revenue_min_server_id', 'min/sum']] = predictDf.apply(find_min_ratio, axis=1)
    predictDf['predict2*(min/sum)'] = predictDf['predict2'] * predictDf['min/sum']

    predictDf = predictDf[[
        'ds','day','deltaDays//days','last3monthBegin','last3monthEnd','revenue_min_server_id','min/sum','predict2*(min/sum)'
    ]].merge(realDataTotalDf2[['ds','server_id','revenue']], left_on=['ds', 'revenue_min_server_id'], right_on=['ds', 'server_id'], how='left')
    
    predictDf = predictDf.dropna(subset=['revenue'])
    predictDf.to_csv('/src/data/lastwar_getMinMape_predictDf4.csv',index=False)

    predictDf = predictDf.groupby('deltaDays//days').agg({'revenue':'sum','predict2*(min/sum)':'sum'}).reset_index()
    predictDf['mape'] = abs(predictDf['revenue'] - predictDf['predict2*(min/sum)']) / predictDf['revenue']
    print('predictDf:')
    print(predictDf)
    predictDf.to_csv('/src/data/lastwar_getMinMape_predictDf5.csv',index=False)

    mapeDf = predictDf.groupby('deltaDays//days').agg({'mape':'mean'}).reset_index()
    print('mapeDf:')
    print(mapeDf)

def getSumMinMape(reportData,today = None):
        
    # 改为获取昨日数据，因为今日数据可能不完整
    if today is None:
        today = date.today()
        
    todayStr = today.strftime('%Y-%m-%d')
    
    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    realDataDf = getData(yesterdayStr).copy()
    realDataDf['day'] = pd.to_datetime(realDataDf['day'], errors='coerce')
    realDataDf['month'] = realDataDf['day'].apply(lambda x: x.replace(day=1))
    realDataDf2 = realDataDf.groupby(['month','server_id']).agg({'revenue':'sum'}).reset_index()
    realDataDf3 = realDataDf2.groupby('month').agg({'revenue':'min'}).reset_index()
    print('realDataDf3')
    realDataDf3.to_csv('/src/data/lastwar_getSumMinMape_realDataDf3.csv',index=False)

    reportData['lastwar_getSumMinMape_realDataDf3.csv'] = '/src/data/lastwar_getSumMinMape_realDataDf3.csv'

    # 找到所有预测数据
    files = os.listdir('/src/data/')
    predictFiles = []
    # 定义正则表达式模式来匹配日期格式
    pattern = re.compile(r'^lastwarPredictRevenue3_36_sum_min_\d{4}-\d{2}-\d{2}\.csv$')

    # 遍历文件列表，筛选符合条件的文件
    for file in files:
        if pattern.match(file):
            predictFiles.append(file)

    # 输出符合条件的文件列表
    print(predictFiles)

    reportData['getSumMinMape_predictFiles'] = predictFiles

    predictDf = pd.DataFrame()
    for file in predictFiles:
        df = pd.read_csv(f'/src/data/{file}')
        # 只保留预测数据，即revenue为空的数据
        df = df[df['revenue'].isna()]
        predictDf = predictDf.append(df)

    predictDf['day'] = pd.to_datetime(predictDf['day'], errors='coerce')
    predictDf['ds'] = pd.to_datetime(predictDf['ds'], errors='coerce')
    # monthDiff 即月份差距，ds - day 的月份差距，自然月差距，不看日，只看月。比如 ds 是2025-03-03，day 是2025-02-25，那么 monthDiff 是1
    predictDf['monthDiff'] = (predictDf['ds'].dt.year - predictDf['day'].dt.year) * 12 + predictDf['ds'].dt.month - predictDf['day'].dt.month
    predictDf = predictDf[predictDf['revenue'].isna()]
    predictDf = predictDf[['ds','day','monthDiff','predict2_lower','predict2','predict2_upper']]

    if predictDf.empty or realDataDf3.empty:
        print('predictDf or realDataDf3 is empty')
        return
    
    predictDf.rename(columns={'ds':'month'},inplace=True)
    # print('predictDf:')
    # print(predictDf)
    # print('realDataDf3:')
    # print(realDataDf3)
    predictDf = predictDf.merge(realDataDf3,on='month',how='left')
    predictDf = predictDf.dropna(subset=['revenue'])
    # print('merge predictDf:')
    # print(predictDf)

    predictDf['mape_lower'] = abs(predictDf['revenue'] - predictDf['predict2_lower']) / predictDf['revenue']
    predictDf['mape'] = abs(predictDf['revenue'] - predictDf['predict2']) / predictDf['revenue']
    predictDf['mape_upper'] = abs(predictDf['revenue'] - predictDf['predict2_upper']) / predictDf['revenue']

    # 真实收入小于mape_lower，认为是危险
    predictDf['danger'] = predictDf['revenue'] < predictDf['predict2_lower']

    # 本月数据不完整，将month属于本月的数据删除
    currentMonth = today.replace(day=1)
    predictDf = predictDf[predictDf['month'] < currentMonth]

    print('mape predictDf:')
    print(predictDf)
    predictDf.to_csv('/src/data/lastwar_getSumMinMape_predictDf.csv',index=False)

    reportData['lastwar_getSumMinMape_predictDf.csv'] = '/src/data/lastwar_getSumMinMape_predictDf.csv'

    predictDf = predictDf.groupby('monthDiff').agg({'mape_lower':'mean','mape':'mean','mape_upper':'mean','danger':'sum'}).reset_index()

    print('groupby predictDf:')
    print(predictDf)

    predictDf.to_csv('/src/data/lastwar_getSumMinMape_predictDf2.csv',index=False)

    reportData['lastwar_getSumMinMape_predictDf2.csv'] = '/src/data/lastwar_getSumMinMape_predictDf2.csv'


def report(reportData):
    # 获取飞书的token
    tenantAccessToken = getTenantAccessToken()

    docId = createDoc(tenantAccessToken, f"lastwar预测服务器收入3~36服 {reportData['todayStr']} 误差监测",'OKcPfXlcalm39DdLT54cuuM5nqh')
    print('docId:', docId)

    addText(tenantAccessToken, docId, '','本报告每周一自动生成',text_color=1,bold = True)
    addHead1(tenantAccessToken, docId,'', '整体预测结果')
    addText(tenantAccessToken, docId, '', '由于目前采用每周预测之后90天数据，而且预测的精度随预测时间的增加而降低，所以将不同的周期预测结果按照预测日和间隔天数（周）分组，计算预测值和真实值的和，然后计算mape。')
    addText(tenantAccessToken, docId, '', '这个结果会一定程度的反映预测的准确性。')
    addHead2(tenantAccessToken, docId,'', '获得真实数据')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getTotalMape_realDataTotalDf.csv'],view_type= 1)
    addHead2(tenantAccessToken, docId,'', '获得预测数据文件')
    for filename in reportData['getTotalMape_predictFiles']:
        fullpath = f'/src/data/{filename}'
        addFile(tenantAccessToken, docId, '', fullpath,view_type= 1)

    addHead2(tenantAccessToken, docId,'', '数据处理')
    addText(tenantAccessToken, docId, '', '1. 将预测数据按照预测日和间隔天数（周）分组，计算预测值和真实值的和')
    addText(tenantAccessToken, docId, '', '列解释：ds-被预测日，predict2-预测值，day-预测执行日期，deltaDays-被预测日期与预测执行日期差（即预测多少天的数据），deltaDays//days-被预测日期与预测执行日期差整除7（即预测多少周的数据）')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getTotalMape_predictDf1.csv'],view_type= 1)
    addText(tenantAccessToken, docId, '', '2. 分组计算mape')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getTotalMape_predictDf3.csv'],view_type= 1)
    addHead2(tenantAccessToken, docId,'', '结果')
    addText(tenantAccessToken, docId, '', '列解释：deltaDays//days-预测多少周的数据，mape-平均绝对百分比误差')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getTotalMape_mapeDf.csv'],view_type= 2)

    addHead1(tenantAccessToken, docId,'', '最差服务器预测结果')
    addText(tenantAccessToken, docId, '', '与整体预测结果类似，但是最差服务器预测是按自然月进行汇总的，所以误差监测也按照自然月进行。比如2月预测3月数据则认为是预测未来1个月的数据。按照这种方法，按照预测几个月进行分组，然后计算MAPE。')
    addHead2(tenantAccessToken, docId,'', '获得真实数据')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getSumMinMape_realDataDf3.csv'],view_type= 1)
    addHead2(tenantAccessToken, docId,'', '获得预测数据文件')
    for filename in reportData['getSumMinMape_predictFiles']:
        fullpath = f'/src/data/{filename}'
        addFile(tenantAccessToken, docId, '', fullpath,view_type= 1)

    addHead2(tenantAccessToken, docId,'', '数据处理')
    addText(tenantAccessToken, docId, '', '1. 将预测数据按照预测日和间隔月份分组，计算预测值和真实值的和')
    addText(tenantAccessToken, docId, '', '列解释：month-被预测月份，predict2_lower-预测值下限，predict2-预测值，predict2_upper-预测值上限，day-预测执行日期，monthDiff-被预测月份与预测执行月份差（自然月），danger-真实收入小于预测值下限的标志')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getSumMinMape_predictDf.csv'],view_type= 1)
    addHead2(tenantAccessToken, docId,'', '结果')
    addText(tenantAccessToken, docId, '', '列解释：monthDiff-预测多少月的数据，mape_lower-平均绝对百分比误差下限，mape-平均绝对百分比误差，mape_upper-平均绝对百分比误差上限，danger-真实收入小于预测值下限的标志')
    addFile(tenantAccessToken, docId, '', reportData['lastwar_getSumMinMape_predictDf2.csv'],view_type= 2)

    docUrl = 'https://rivergame.feishu.cn/docx/'+docId
    print('文档创建完成：',docUrl)

    message = '目前由于数据不足，没有措辞与结论总结，将主要过程数据记录，供参考。'

    testWebhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c'

    webhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/0a71b38a-68cc-4600-b50f-60432dfec0ce'

    sendMessageToWebhook2(f'lastwar预测服务器收入3~36服 {reportData["todayStr"]} 误差监测 报告已生成',message,'详细报告',docUrl,testWebhookUrl)
    


if __name__ == "__main__":
    reportData = {}
    # today = pd.to_datetime('2025-03-03')
    today = None
    getTotalMape(reportData,today = today,days = 7)
    getSumMinMape(reportData,today = today)

    report(reportData)

