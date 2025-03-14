# 计算3~36服的收入和的预测，使用 Prophet 模型，每个服务器单独预测，预测未来90天的数据
# 每周一进行一次预测
# 结果保存到csv文件，文件名为 lastwar_predict_3_36_revenue_sum_{mondayStr}.csv
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
from src.config import ssTokenLastwar,ssUrlPrefixLastwar
from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addText,addFile,sendMessage,addImage,addCode,sendMessageToWebhook,sendMessageToWebhook2


from src.report.aws.aws_s3 import S3Manager

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

# # 记录一下模型的性能
# 都写在一个函数里，太长了，特意拆出来
def computeMape(train_df, train_forecast, test_df, test_forecast, server_df,full_forecast,reportData):
    # 计算train、test的MAPE
    train_df_new = train_df[['ds', 'y']].merge(train_forecast[['ds', 'yhat']], on='ds')
    train_df_new = train_df_new.merge(server_df[['ds', 'revenue']], on='ds')
    train_df_new['mape'] = np.abs(train_df_new['revenue'] - train_df_new['yhat']) / train_df_new['revenue']
    train_df_new['mape14'] = np.abs(train_df_new['y'] - train_df_new['yhat']) / train_df_new['y']
    train_mape = np.mean(train_df_new['mape'])
    train_mape14 = np.mean(train_df_new['mape14'])

    test_df_new = test_df[['ds', 'y']].merge(test_forecast[['ds', 'yhat']], on='ds')
    test_df_new = test_df_new.merge(server_df[['ds', 'revenue']], on='ds')
    test_df_new['mape'] = np.abs(test_df_new['revenue'] - test_df_new['yhat']) / test_df_new['revenue']
    test_df_new['mape14'] = np.abs(test_df_new['y'] - test_df_new['yhat']) / test_df_new['y']
    test_mape = np.mean(test_df_new['mape'])
    test_mape14 = np.mean(test_df_new['mape14'])

    # 按周统计train、test的MAPE
    train_df_new['week'] = train_df_new['ds'].dt.strftime('%W')
    train_df_new_week = train_df_new.groupby('week').agg({'revenue': 'sum', 'y':'sum' , 'yhat': 'sum'}).reset_index()
    train_df_new_week['mape'] = np.abs(train_df_new_week['revenue'] - train_df_new_week['yhat']) / train_df_new_week['revenue']
    train_df_new_week['mape14'] = np.abs(train_df_new_week['y'] - train_df_new_week['yhat']) / train_df_new_week['y']
    train_mape_week = np.mean(train_df_new_week['mape'])
    train_mape14_week = np.mean(train_df_new_week['mape14'])

    test_df_new['week'] = test_df_new['ds'].dt.strftime('%W')
    test_df_new_week = test_df_new.groupby('week').agg({'revenue': 'sum', 'y':'sum' , 'yhat': 'sum'}).reset_index()
    test_df_new_week['mape'] = np.abs(test_df_new_week['revenue'] - test_df_new_week['yhat']) / test_df_new_week['revenue']
    test_df_new_week['mape14'] = np.abs(test_df_new_week['y'] - test_df_new_week['yhat']) / test_df_new_week['y']
    test_mape_week = np.mean(test_df_new_week['mape'])
    test_mape14_week = np.mean(test_df_new_week['mape14'])

    # 按月统计train、test的MAPE
    train_df_new['month'] = train_df_new['ds'].dt.strftime('%Y-%m')
    train_df_new_month = train_df_new.groupby('month').agg({'revenue': 'sum', 'y':'sum' , 'yhat': 'sum'}).reset_index()
    train_df_new_month['mape'] = np.abs(train_df_new_month['revenue'] - train_df_new_month['yhat']) / train_df_new_month['revenue']
    train_df_new_month['mape14'] = np.abs(train_df_new_month['y'] - train_df_new_month['yhat']) / train_df_new_month['y']
    train_mape_month = np.mean(train_df_new_month['mape'])
    train_mape14_month = np.mean(train_df_new_month['mape14'])

    test_df_new['month'] = test_df_new['ds'].dt.strftime('%Y-%m')
    test_df_new_month = test_df_new.groupby('month').agg({'revenue': 'sum', 'y':'sum' , 'yhat': 'sum'}).reset_index()
    test_df_new_month['mape'] = np.abs(test_df_new_month['revenue'] - test_df_new_month['yhat']) / test_df_new_month['revenue']
    test_df_new_month['mape14'] = np.abs(test_df_new_month['y'] - test_df_new_month['yhat']) / test_df_new_month['y']
    test_mape_month = np.mean(test_df_new_month['mape'])
    test_mape14_month = np.mean(test_df_new_month['mape14'])

    mapeText = f'''
按天统计
训练集平均绝对误差:{train_mape} 训练集指数加权移动平均14平均绝对误差:{train_mape14}
测试集平均绝对误差:{test_mape} 测试集指数加权移动平均14平均绝对误差:{test_mape14}
按周统计
训练集平均绝对误差:{train_mape_week} 训练集指数加权移动平均14平均绝对误差:{train_mape14_week}
测试集平均绝对误差:{test_mape_week} 测试集指数加权移动平均14平均绝对误差:{test_mape14_week}
按月统计
训练集平均绝对误差:{train_mape_month} 训练集指数加权移动平均14平均绝对误差:{train_mape14_month}
测试集平均绝对误差:{test_mape_month} 测试集指数加权移动平均14平均绝对误差:{test_mape14_month}
    '''
    print(mapeText)

    reportData['mapeText'] = mapeText

    # print('按天统计')
    # print('训练集平均绝对误差:',train_mape,'训练集指数加权移动平均14平均绝对误差:',train_mape14)
    # print('测试集平均绝对误差:',test_mape,'测试集指数加权移动平均14平均绝对误差:',test_mape14)
    # print('按周统计')
    # print('训练集平均绝对误差:',train_mape_week,'训练集指数加权移动平均14平均绝对误差:',train_mape14_week)
    # print('测试集平均绝对误差:',test_mape_week,'测试集指数加权移动平均14平均绝对误差:',test_mape14_week)
    # print('按月统计')
    # print('训练集平均绝对误差:',train_mape_month,'训练集指数加权移动平均14平均绝对误差:',train_mape14_month)
    # print('测试集平均绝对误差:',test_mape_month,'test_mape14_测试集指数加权移动平均14平均绝对误差month:',test_mape14_month)



    # 画图
    plt.figure(figsize=(18, 6))
    plt.plot(train_df_new['ds'], train_df_new['y'], label='Actual Revenue')
    plt.plot(train_df_new['ds'], train_df_new['yhat'], label='Predicted Revenue')

    plt.plot(test_df_new['ds'], test_df_new['y'], label='Actual Revenue', alpha=0.6)
    plt.plot(test_df_new['ds'], test_df_new['yhat'], label='Predicted Revenue', alpha=0.6)

    plt.plot(full_forecast['ds'], full_forecast['yhat'], label='Future Predicted Revenue', linestyle='--')

    # 添加竖线分割训练集和测试集，以及测试集和预测集
    plt.axvline(x=pd.to_datetime(test_df['ds'].min()), color='r', linestyle='--', label='Train/Test Split')
    plt.axvline(x=pd.to_datetime(test_df['ds'].max()), color='g', linestyle='--', label='Test/Future Split')

    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.title(f'Server 3~36 Revenue Sum Forecast')
    plt.legend()
    plt.savefig(f'/src/data/lastwarPredict3To36RevenueSum.png')
    print(f'save file /src/data/lastwarPredict3To36RevenueSum.png')
    plt.close()

    reportData['lastwarPredict3To36RevenueSum.png'] = '/src/data/lastwarPredict3To36RevenueSum.png'

    return train_mape,train_mape14,test_mape,test_mape14,train_mape_week,train_mape14_week,test_mape_week,test_mape14_week,train_mape_month,train_mape14_month,test_mape_month,test_mape14_month

def computeRevenueRateMin(df,reportData):
    # 获取最近3个自然月的数据，其中目前今天所在的月份不完整，不计入在内
    today = date.today()
    # startDay 是本月不算，3个自然月的第一天；endDay 是上个月的最后一天
    startDay = today.replace(day=1) - pd.Timedelta(days=1)
    endDay = startDay
    startDay = startDay - pd.offsets.MonthBegin(3)
    startDayStr = startDay.strftime('%Y-%m-%d')
    endDayStr = endDay.strftime('%Y-%m-%d')
    print('today:', today, 'startDay:', startDayStr, 'endDay:', endDayStr)

    last3MonthDf = df[(df['day'] >= startDayStr) & (df['day'] <= endDayStr)].copy()
    last3MonthDf['day'] = pd.to_datetime(last3MonthDf['day'])
    last3MonthDf['month'] = last3MonthDf['day'].dt.strftime('%Y-%m')
    last3MonthDf = last3MonthDf.groupby(['server_id', 'month']).sum().reset_index()

    # 过滤掉收入小于1的数据，有些服务器已经没有数据了
    last3MonthDf = last3MonthDf[last3MonthDf['revenue'] > 1]
    last3MonthDf = last3MonthDf.sort_values(['month', 'server_id']).reset_index(drop=True)
    last3MonthDf.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_debug1.csv', index=False)
    print('last3MonthDf:', last3MonthDf)
    
    # 计算每个月每个服务器的收入占比
    last3MonthDf['revenueRate'] = last3MonthDf.groupby('month')['revenue'].transform(lambda x: x / x.sum())
    last3MonthDf.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_debug2.csv', index=False)
    
    # 找到最低的收入占比
    minRevenueRateIdx = last3MonthDf['revenueRate'].idxmin()
    minRevenueRate = last3MonthDf.loc[minRevenueRateIdx, 'revenueRate']
    minRevenueRateMonth = last3MonthDf.loc[minRevenueRateIdx, 'month']
    minRevenueRateServerId = last3MonthDf.loc[minRevenueRateIdx, 'server_id']
    
    minRevenueText = f'最低收入占比: {minRevenueRate*100:.2f}%, 月份: {minRevenueRateMonth}, 服务器: {minRevenueRateServerId}'
    reportData['minRevenueText'] = minRevenueText
    print(minRevenueText)
    
    return minRevenueRate, minRevenueRateMonth, minRevenueRateServerId
    
def computeRevenueRateMean(df):
    # 获取最近3个自然月的数据，其中目前今天所在的月份不完整，不计入在内
    today = date.today()
    # startDay 是本月不算，3个自然月的第一天；endDay 是上个月的最后一天
    startDay = today.replace(day=1) - pd.Timedelta(days=1)
    endDay = startDay
    startDay = startDay - pd.offsets.MonthBegin(3)
    startDayStr = startDay.strftime('%Y-%m-%d')
    endDayStr = endDay.strftime('%Y-%m-%d')
    print('today:', today, 'startDay:', startDayStr, 'endDay:', endDayStr)

    last3MonthDf = df[(df['day'] >= startDayStr) & (df['day'] <= endDayStr)]
    last3MonthDf['day'] = pd.to_datetime(last3MonthDf['day'])
    last3MonthDf['month'] = last3MonthDf['day'].dt.strftime('%Y-%m')
    last3MonthDf = last3MonthDf.groupby(['server_id', 'month']).sum().reset_index()

    # 过滤掉收入小于1的数据，有些服务器已经没有数据了
    last3MonthDf = last3MonthDf[last3MonthDf['revenue'] > 1]
    last3MonthDf = last3MonthDf.sort_values(['month', 'server_id']).reset_index(drop=True)
    last3MonthDf.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_debug1.csv', index=False)
    print('last3MonthDf:', last3MonthDf)
    
    # 计算每个月每个服务器的收入占比
    last3MonthDf['revenueRate'] = last3MonthDf.groupby('month')['revenue'].transform(lambda x: x / x.sum())
    last3MonthDf.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_debug2.csv', index=False)
    
    # 找到最低的收入占比
    minRevenueRateIdx = last3MonthDf['revenueRate'].idxmin()
    minRevenueRate = last3MonthDf.loc[minRevenueRateIdx, 'revenueRate']
    minRevenueRateMonth = last3MonthDf.loc[minRevenueRateIdx, 'month']
    minRevenueRateServerId = last3MonthDf.loc[minRevenueRateIdx, 'server_id']

    # 找到minRevenueRateServerId对应的3个月的收入占比均值
    minRevenueRateMean = last3MonthDf[last3MonthDf['server_id'] == minRevenueRateServerId]['revenueRate'].mean()
    
    print('minRevenueRate:', minRevenueRate)
    print('minRevenueRateMonth:', minRevenueRateMonth)
    print('minRevenueRateServerId:', minRevenueRateServerId)
    print('minRevenueRateMean:', minRevenueRateMean)
    
    return minRevenueRateMean, minRevenueRateMonth, minRevenueRateServerId


# 获得置信区间宽度
def getWidth(df,result_df,reportData,today = None):
    # 获取最近3个自然月的数据，其中目前今天所在的月份不完整，不计入在内
    if today is None:
        today = date.today()
    todayStr = today.strftime('%Y-%m-%d')
    # startDay 是本月不算，3个自然月的第一天；endDay 是上个月的最后一天
    startDay = today.replace(day=1) - pd.Timedelta(days=1)
    endDay = startDay
    startDay = startDay - pd.offsets.MonthBegin(3)
    startDayStr = startDay.strftime('%Y-%m-%d')
    endDayStr = endDay.strftime('%Y-%m-%d')
    print('today:', today, 'startDay:', startDayStr, 'endDay:', endDayStr)

    last3MonthDf = df[(df['day'] >= startDayStr) & (df['day'] <= endDayStr)].copy()
    last3MonthDf['day'] = pd.to_datetime(last3MonthDf['day'])
    last3MonthDf['month'] = last3MonthDf['day'].dt.strftime('%Y-%m')
    last3MonthDf = last3MonthDf.groupby(['server_id', 'month']).sum().reset_index()

    minRevenueRate, minRevenueRateMonth, minRevenueRateServerId = computeRevenueRateMin(df,reportData)
    # minRevenueRate, minRevenueRateMonth, minRevenueRateServerId = computeRevenueRateMean(df)

    reportData['minRevenueRate'] = minRevenueRate
    reportData['minRevenueRateMonth'] = minRevenueRateMonth
    reportData['minRevenueRateServerId'] = minRevenueRateServerId

    # 用minRevenueRate * 预测值作为对 minRevenueRateServerId 的预测
    # 然后按月汇总，计算每个月的预测值和实际值的误差比例

    minRevenueRateServerDf = last3MonthDf[last3MonthDf['server_id'] == minRevenueRateServerId].copy(deep=True)

    result_df_copy = result_df.copy(deep=True)
    result_df_copy['month'] = result_df_copy['ds'].dt.strftime('%Y-%m')
    # 获取日期，只有dd部分
    result_df_copy['dd'] = result_df_copy['ds'].dt.strftime('%d') 
    
    result_df_copy_for_dd = result_df_copy.copy(deep=True)
    result_df_copy_for_dd = result_df_copy_for_dd[['month','dd']]
    result_df_copy_for_dd = result_df_copy_for_dd.groupby(['month']).agg({'dd' : 'max'}).reset_index()
    print('result_df_copy_for_dd:')
    print(result_df_copy_for_dd)

    result_df_copy = result_df_copy[(result_df_copy['ds']>startDayStr) & (result_df_copy['ds']<=endDayStr)]
    # 只用predict1来做，因为predict2的数据中包含了后3个月的数据
    # 其实predict1中野包含了部分后3个月的数据，暂时忽略
    result_df_copy = result_df_copy.groupby(['month']).agg(
        {
            'predict1' : 'sum',
            'dd' : 'max'
        }
    ).reset_index()
    
    result_df_copy['predict'] = result_df_copy['predict1'] * minRevenueRate
    print('result_df_copy:')
    print(result_df_copy)

    minRevenueRateServerDf = minRevenueRateServerDf.merge(result_df_copy[['month','predict']], on='month')
    minRevenueRateServerDf['diffRate'] = np.abs(minRevenueRateServerDf['revenue'] - minRevenueRateServerDf['predict']) / minRevenueRateServerDf['revenue']

    width = minRevenueRateServerDf['diffRate'].max()
    print('width:', width)

    # 直接在这里 将最小占比服务器的真实值，预测值，和预测值的置信区间 单独放入一个df
    # 然后保存文件

    minRevenueRateServerDf['predict_lower'] = minRevenueRateServerDf['predict'] * (1 - width)
    minRevenueRateServerDf['predict_upper'] = minRevenueRateServerDf['predict'] * (1 + width)

    result_df_copy2 = result_df.copy(deep=True)
    result_df_copy2 = result_df_copy2[(result_df_copy2['ds'] >= startDayStr)]
    result_df_copy2['month'] = result_df_copy2['ds'].dt.strftime('%Y-%m')
    result_df_copy2 = result_df_copy2.groupby(['month']).agg(
        {'predict2' : 'sum'}
    ).reset_index()

    minRevenueRateServerDf = minRevenueRateServerDf.merge(result_df_copy2[['month','predict2']], on='month', how='right')
    minRevenueRateServerDf['server_id'] = minRevenueRateServerId
    minRevenueRateServerDf['predict2'] *= minRevenueRate
    minRevenueRateServerDf['predict2_lower'] = minRevenueRateServerDf['predict2'] * (1 - width)
    minRevenueRateServerDf['predict2_upper'] = minRevenueRateServerDf['predict2'] * (1 + width)

    # 计算阈值警戒线
    # 目前阈值是两个：每日收入低于10美元 和 每日收入低于20美元，根据month对应的天数，算出月警戒线
    minRevenueRateServerDf['day'] = pd.to_datetime(minRevenueRateServerDf['month'] + '-01')
    
    # minRevenueRateServerDf['days'] = minRevenueRateServerDf['day'].dt.daysinmonth
    minRevenueRateServerDf = minRevenueRateServerDf.merge(result_df_copy_for_dd[['month','dd']], on='month')
    minRevenueRateServerDf.rename(columns={'dd':'days'}, inplace=True)
    minRevenueRateServerDf['days'] = pd.to_numeric(minRevenueRateServerDf['days'])

    minRevenueRateServerDf['threshold10'] = 10 * minRevenueRateServerDf['days']
    minRevenueRateServerDf['threshold20'] = 20 * minRevenueRateServerDf['days']

    minRevenueRateServerDf['ds'] = minRevenueRateServerDf['day']
    minRevenueRateServerDf['day'] = todayStr

    minRevenueRateServerDf['predict2_lower_per_day'] = minRevenueRateServerDf['predict2_lower'] / minRevenueRateServerDf['days']
    

    print('minRevenueRateServerDf:')
    print(minRevenueRateServerDf)
    minRevenueRateServerDf.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_min_{todayStr}.csv', index=False)
    reportData['lastwarPredictRevenue3_36_sum_min.csv'] = f'/src/data/lastwarPredictRevenue3_36_sum_min_{todayStr}.csv'

    minRevenueRateServerDf['month'] = pd.to_datetime(minRevenueRateServerDf['month'])



    # 画图
    plt.figure(figsize=(18, 6))
    plt.plot(minRevenueRateServerDf['month'], minRevenueRateServerDf['revenue'], label='Actual Revenue')
    plt.plot(minRevenueRateServerDf['month'], minRevenueRateServerDf['predict'], label='Predicted Revenue')
    plt.plot(minRevenueRateServerDf['month'], minRevenueRateServerDf['predict2'], label='Predicted2 Revenue')

    # 将警戒线用红色虚线画出来
    plt.plot(minRevenueRateServerDf['month'], minRevenueRateServerDf['threshold10'], label='Threshold 10', linestyle='--', color='r')
    plt.plot(minRevenueRateServerDf['month'], minRevenueRateServerDf['threshold20'], label='Threshold 20', linestyle='--', color='yellow')

    plt.fill_between(minRevenueRateServerDf['month'], minRevenueRateServerDf['predict2_lower'], minRevenueRateServerDf['predict2_upper'], color='gray', alpha=0.3)

    plt.xlabel('Month')
    plt.ylabel('Revenue')
    plt.title(f'Server {minRevenueRateServerId} Revenue Forecast')
    plt.legend()
    plt.savefig(f'/src/data/lastwarPredictRevenue3_36_sum_min_{todayStr}.png')
    print(f'save file /src/data/lastwarPredictRevenue3_36_sum_min_{todayStr}.png')

    reportData['lastwarPredictRevenue3_36_sum_min.png'] = f'/src/data/lastwarPredictRevenue3_36_sum_min_{todayStr}.png'

    reportData['width'] = width

    return width, minRevenueRateServerDf

def prophet1FloorL(today = None,future_periods=90):
    # 将所有报告需要用到的数据都保存在这个字典中，最后返回出去，交给report函数生成飞书报告
    reportData = {}

    # 改为获取昨日数据，因为今日数据可能不完整
    if today is None:
        today = date.today()
    # # for debug，设置今天是2025-03-03
    # today = pd.to_datetime('2025-02-24')

    todayStr = today.strftime('%Y-%m-%d')


    yesterday = today - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y-%m-%d')

    # 从today 往前 8周，7*8 天 作为测试集
    testStartDay = yesterday - pd.Timedelta(days=7*8)
    testStartDayStr = testStartDay.strftime('%Y-%m-%d') 
    # 预测结束日期是今天往后90天
    futureDay = yesterday + pd.Timedelta(days=future_periods)
    futureDayStr = futureDay.strftime('%Y-%m-%d')

    print('预测开始日期:', yesterdayStr, '测试集开始日期:', testStartDayStr, '预测结束日期:', futureDayStr)

    reportData['todayStr'] = todayStr
    reportData['yesterdayStr'] = yesterdayStr
    reportData['testStartDayStr'] = testStartDayStr
    reportData['futureDayStr'] = futureDayStr
    
    # 最近3个月
    # last3Month = [yesterday - pd.offsets.MonthBegin(0), yesterday - pd.offsets.MonthBegin(1), yesterday - pd.offsets.MonthBegin(2)]
    last3Month = [(yesterday - pd.DateOffset(months=i)).strftime('%Y-%m') for i in range(4, 1, -1)]
    # yyyy-mm格式,逗号分隔
    reportData['last3MonthStr'] = f'{last3Month[0]}，{last3Month[1]}和{last3Month[2]}'
    print('最近3个月:', reportData['last3MonthStr'])

    df = getData(yesterdayStr)
    reportData['lastwarPredictRevenue3_36_sum_data'] = f'/src/data/lastwarPredictRevenue3_36_sum_data_{yesterdayStr}.csv'

    df0 = df.copy(deep=True)

    df['day'] = pd.to_datetime(df['day'])
    
    df = df.sort_values(by=['server_id', 'day'])
    df = df.groupby(['day']).agg(
        {'revenue': 'sum'}
    ).reset_index()


    server_df = df.copy(deep=True)
    server_df['revenue14'] = server_df['revenue'].ewm(span=14, adjust=False).mean()

    # 作为训练集与测试集的分割
    start_date = testStartDayStr
    
    server_df.rename(columns={'day': 'ds', 'revenue14': 'y'}, inplace=True)
    
    train_df = server_df[['ds', 'y']][server_df['ds'] < start_date]
    test_df = server_df[['ds', 'y']][server_df['ds'] >= start_date]

    # 训练 Prophet 模型并进行预测
    model = Prophet()
    model.fit(train_df)
    
    train_forecast = model.predict(train_df[['ds']])
    test_forecast = model.predict(test_df[['ds']])

    # 合并训练集和测试集数据
    full_df = pd.concat([train_df, test_df])
    model = Prophet()
    model.fit(full_df)

    # 预测未来 future_periods 天的数据
    future = model.make_future_dataframe(periods=future_periods)
    full_forecast = model.predict(future)

    # 保存结果到 CSV 文件
    result_df = full_forecast[['ds', 'yhat']].merge(server_df[['ds', 'revenue', 'y']], on='ds', how='left')
    result_df.rename(columns={'y': 'revenue_ewm14', 'yhat': 'predicted_revenue'}, inplace=True)
    result_df['initial_predicted_revenue'] = result_df['predicted_revenue']
    result_df.loc[result_df['ds'] >= start_date, 'initial_predicted_revenue'] = np.nan

    # 将训练集和测试集的预测结果合并到 initial_predicted_revenue 列中
    initial_predicted = pd.concat([train_forecast[['ds', 'yhat']], test_forecast[['ds', 'yhat']]])
    initial_predicted.rename(columns={'yhat': 'initial_predicted_revenue'}, inplace=True)
    result_df = result_df.merge(initial_predicted, on='ds', how='left')

    result_df.rename(columns={
        'initial_predicted_revenue_y': 'predict1',
        'predicted_revenue': 'predict2',
    }, inplace=True)
    result_df = result_df[['ds', 'revenue', 'revenue_ewm14', 'predict1','predict2']]

    result_df['day'] = todayStr
    result_df.to_csv(f'/src/data/lastwarPredictRevenue3_36_sum_{todayStr}.csv', index=False)
    print(f'save file /src/data/lastwarPredictRevenue3_36_sum_{todayStr}.csv')
    reportData['lastwarPredictRevenue3_36_sum.csv'] = f'/src/data/lastwarPredictRevenue3_36_sum_{todayStr}.csv'

    computeMape(train_df, train_forecast, test_df, test_forecast, server_df,full_forecast,reportData)

    _, minRevenueRateServerDf = getWidth(df0,result_df,reportData,today)

    # 找到minRevenueRateServerDf中，低于threshold10和threshold20的最早月份
    minRevenueRateServerDf['danger10'] = minRevenueRateServerDf['revenue'] < minRevenueRateServerDf['threshold10']
    minRevenueRateServerDf['danger20'] = minRevenueRateServerDf['revenue'] < minRevenueRateServerDf['threshold20']


    danger10 = minRevenueRateServerDf[minRevenueRateServerDf['danger10'] == True]
    danger20 = minRevenueRateServerDf[minRevenueRateServerDf['danger20'] == True]

    waringText = ''
    if len(danger10) > 0:
        print('低于10美元的月份:', danger10['month'].min())
        waringText += f'低于10美元的月份:{danger10["month"].min()}。'
    if len(danger20) > 0:
        print('低于20美元的月份:', danger20['month'].min())
        waringText += f'低于20美元的月份:{danger20["month"].min()}。'

    if len(danger10) == 0 and len(danger20) == 0:
        print('暂时没有预测到低于10美元或20美元的情况')
        waringText += '暂时没有预测到低于10美元或20美元的情况。'
        
    minRevenueRateServerDf['predict2_lower_per_day'] = minRevenueRateServerDf['predict2_lower'] / minRevenueRateServerDf['days']
    minP2 = minRevenueRateServerDf['predict2_lower_per_day'].min()
    waringText += f'预测期间月平均最低日收入可能达到{minP2:.2f}美元'
    print(f'\n预测期间月平均最低日收入可能达到{minP2:.2f}美元')

    reportData['waringText'] = waringText
    reportData['minP2'] = f'{minP2:.2f}'

    return reportData


def report(reportData):
    # 获取飞书的token
    tenantAccessToken = getTenantAccessToken()

    docId = createDoc(tenantAccessToken, f"lastwar预测服务器收入3~36服 {reportData['todayStr']}",'OKcPfXlcalm39DdLT54cuuM5nqh')
    print('docId:', docId)

    addText(tenantAccessToken, docId, '','本报告每周一自动生成',text_color=1,bold = True)
    addHead1(tenantAccessToken, docId,'', '结论前置')
    
    addText(tenantAccessToken, docId, '',f"预测时间为{reportData['yesterdayStr']}~{reportData['futureDayStr']}。最近3个月（{reportData['last3MonthStr']}）收入占比最低（收入金额最低）的是第{reportData['minRevenueRateServerId']}服务器。",bold = True)

    if len(reportData['waringText']) > 0:
        addText(tenantAccessToken, docId, '',reportData['waringText'],bold = True)

    addHead1(tenantAccessToken, docId,'', '方案阐述与中间步骤结果')
    addHead2(tenantAccessToken, docId,'', '步骤')
    addText(tenantAccessToken, docId, '', f"1、数数获得2024-01-01至{reportData['yesterdayStr']}，3~36服收入数据")
    addText(tenantAccessToken, docId, '', f'数数数据筛选条件：')
    addImage(tenantAccessToken, docId, '','/src/src/lastwar/20250224/pic1.png')
    addFile(tenantAccessToken, docId, '',reportData['lastwarPredictRevenue3_36_sum_data'],view_type = 1)
    
    addText(tenantAccessToken, docId, '', f'2、预测1，将8周作为测试集，预测3~36服整体收入，此预测用最近的历史数据验证此方案稳定性，并不真的预测未来。得到结果误差：')
    addText(tenantAccessToken, docId, '', f"误差细节，其中训练集为2024-01-01~{reportData['testStartDayStr']}，测试集为{reportData['testStartDayStr']}~{reportData['yesterdayStr']}。平均绝对误差指真实收入与预测收入误差，指数加权移动平均14平均绝对误差指14日移动平均后的收入与预测收入误差：")
    
    addCode(tenantAccessToken, docId, '',reportData['mapeText'])

    addText(tenantAccessToken, docId, '', f'3、预测2，将所有历史数据均放入模型，预测之后90天总体收入金额。得到结果：')
    addText(tenantAccessToken, docId, '', '''
预测结果csv文件，列解释：
ds是数据日期，
revenue是真实收入金额，
revenue_ewm14是真实收入金额14日加权移动平均结果的结果，
predict1：步骤2（预测1）中的预测结果
predict2：步骤3（预测2）中的预测结果
    ''')

    addFile(tenantAccessToken, docId, '',reportData['lastwarPredictRevenue3_36_sum.csv'],view_type = 1)
    addText(tenantAccessToken, docId, '', f'曲线图：')

    addImage(tenantAccessToken, docId, '',reportData['lastwarPredict3To36RevenueSum.png'])

    addText(tenantAccessToken, docId, '', f'4、获得最近3个月，3~36服中，收入占比（收入金额）的服务器。')
    addCode(tenantAccessToken, docId, '', reportData['minRevenueText'])
    addText(tenantAccessToken, docId, '', f'5、使用步骤3中的结果，与步骤4中的占比，估测3~36服中 收入占比（收入金额）最低服务器未来收入。')
    addText(tenantAccessToken, docId, '', f'曲线图：')
    addImage(tenantAccessToken, docId, '',reportData['lastwarPredictRevenue3_36_sum_min.png'])

    addText(tenantAccessToken, docId, '', '''
上图中灰色部分为预测可能误差范围，称作置信区间。
置信区间宽度计算方式：
步骤2中的预测结果（预测1，因为这个预测中不包含最近真实数据，更能真实反应预测误差）的预测月收入 乘以 步骤4中的结果中的 “最低收入占比” 作为预测值。
与 步骤4中的结果中的 最低收入服务器 最近3个自然月（不包括本月）的真实月收入 作为真实值。
按月计算 预测值与真实值的 误差，共3个月，产生3个误差结果（绝对值（预测收入 - 真实收入）/真实收入）。
取最大误差作为置信区宽度。
    ''')
    addText(tenantAccessToken, docId, '', f'宽度：{reportData["width"]:.4f}',bold=True)

    addHead2(tenantAccessToken, docId,'', '结果判定')
    addText(tenantAccessToken, docId, '', '''
按照步骤3（预测2）中的结果，乘以步骤4的结果中的 “最低收入占比” 作为最低收入服务器收入预测值。
最低收入服务器收入预测值 乘以 （1-宽度）作为预测结果的最坏可能。
当预测结果最坏可能的日均收入 低于 每日 10美元 或者 20美元 时，判定为出现危险情况，予以警告。
    ''')

    addText(tenantAccessToken, docId, '', '具体结果：')
    addFile(tenantAccessToken, docId, '',reportData['lastwarPredictRevenue3_36_sum_min.csv'],view_type = 1)

    addText(tenantAccessToken, docId, '', reportData['waringText'],bold=True)

    addHead1(tenantAccessToken, docId,'', '备注')
    addHead2(tenantAccessToken, docId,'', '平均绝对百分比误差（MAPE）解释')
    addText(tenantAccessToken, docId, '', '''
平均绝对百分比误差（MAPE）是一种常用的衡量预测模型准确性的指标，它表示预测值与实际值之间的相对误差的平均值。MAPE 的公式如下：
MAPE = (1/n) * Σ (|A_t - F_t| / A_t) * 100%
其中，n 是数据点的数量，At 是时间点 t 的实际值，Ft 是时间点 t 的预测值。MAPE 通过计算每个时间点的绝对百分比误差，并取其平均值，来评估预测模型的整体表现。MAPE 的值越小，表示预测模型的准确性越高。
    ''')

    addHead2(tenantAccessToken, docId,'', '指数加权移动平均（EWMA）解释')
    addText(tenantAccessToken, docId, '', '''
指数加权移动平均是一种更复杂的移动平均方法，它对较新的数据点赋予更大的权重，从而更敏感于最新的数据变化。EWMA 的公式如下：
EWMA_t = α * P_t + (1 - α) * EWMA_(t-1)
其中，EWMA_t 是时间点 t 的指数加权移动平均值，α 是平滑系数，取值范围在 0 到 1 之间，P_t 是时间点 t 的数据值，EWMA_(t-1) 是时间点 t-1 的指数加权移动平均值。
    ''')


    message = reportData['waringText']

    docUrl = 'https://rivergame.feishu.cn/docx/'+docId

    # message += f"\n[详细报告]({docUrl})"

    # sendMessageToWebhook(message,'https://open.feishu.cn/open-apis/bot/v2/hook/571e5617-d93c-4b96-81db-f288fbefba32')

    testWebhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c'

    webhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/0a71b38a-68cc-4600-b50f-60432dfec0ce'

    # sendMessageToWebhook2(f"lastwar预测服务器收入3~36服 {reportData['todayStr']} 报告已生成",message,'详细报告',docUrl,testWebhookUrl)
    sendMessageToWebhook2(f"lastwar预测服务器收入3~36服 {reportData['todayStr']} 报告已生成",message,'详细报告',docUrl,webhookUrl)

def uploadFileToAwsS3(reportData):
    s3_manager = S3Manager()
    bucket_name = "lastwardata"

    directory_name = "datascience/szj/lastwarPredictServer3To36SumRevenue20250227/"
    
    lastwarPredictRevenue3_36_sumDirectory = directory_name + 'lastwarPredictRevenue3_36_sum/'
    lastwarPredictRevenue3_36_sum_minDirectory = directory_name + 'lastwarPredictRevenue3_36_sum_min/'

    # 检查目录是否存在，如果不存在则创建
    if not s3_manager.check_directory(bucket_name, lastwarPredictRevenue3_36_sumDirectory):
        print(f"目录 {directory_name} 不存在，正在创建...")
        s3_manager.create_directory(bucket_name, directory_name)

    if not s3_manager.check_directory(bucket_name, lastwarPredictRevenue3_36_sum_minDirectory):
        print(f"目录 {directory_name} 不存在，正在创建...")
        s3_manager.create_directory(bucket_name, directory_name)
    

    # 上传文件到指定目录
    s3_manager.upload_file_to_s3(reportData['lastwarPredictRevenue3_36_sum.csv'], bucket_name, lastwarPredictRevenue3_36_sumDirectory)
    s3_manager.upload_file_to_s3(reportData['lastwarPredictRevenue3_36_sum_min.csv'], bucket_name, lastwarPredictRevenue3_36_sum_minDirectory)


if __name__ == '__main__':
    reportData = prophet1FloorL()
    report(reportData)
    uploadFileToAwsS3(reportData)

    # # for debug
    # mondayList = [
    #     '2024-12-30',
    #     '2025-01-06',
    #     '2025-01-13',
    #     '2025-01-20',
    #     '2025-01-27',
    #     '2025-02-03',
    #     '2025-02-10',
    #     '2025-02-17',
    #     '2025-02-24',
    #     '2025-03-03',
    # ]
    # for monday in mondayList:
    #     reportData = prophet1FloorL(pd.to_datetime(monday))
    #     # report(reportData)
    #     uploadFileToAwsS3(reportData)
        



    

