# 计算3~36服的收入和的预测，使用 Prophet 模型，每个服务器单独预测，预测未来90天的数据
# 每周一进行一次预测
# 结果保存到csv文件，文件名为 lastwar_predict_3_36_revenue_sum_{mondayStr}.csv
import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse
import json
from datetime import date

import pandas as pd
import numpy as np
from prophet import Prophet

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

def getData(endday='2025-02-25'):
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

    return df

def prophet1FloorL(future_periods=90):
    # 改为获取昨日数据，因为今日数据可能不完整
    today = date.today() - pd.Timedelta(days=1)
    todayStr = today.strftime('%Y-%m-%d')
    # 从today 往前 8周，7*8 天 作为测试集
    testStartDay = today - pd.Timedelta(days=7*8)
    testStartDayStr = testStartDay.strftime('%Y-%m-%d') 
    print('today:',todayStr,'testStartDay:',testStartDayStr)

    df = getData(todayStr)

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
    
    # 记录一下模型的性能
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

    print('按天统计')
    print('train_mape:',train_mape,'train_mape14:',train_mape14)
    print('test_mape:',test_mape,'test_mape14:',test_mape14)
    print('按周统计')
    print('train_mape_week:',train_mape_week,'train_mape14_week:',train_mape14_week)
    print('test_mape_week:',test_mape_week,'test_mape14_week:',test_mape14_week)
    print('按月统计')
    print('train_mape_month:',train_mape_month,'train_mape14_month:',train_mape14_month)
    print('test_mape_month:',test_mape_month,'test_mape14_month:',test_mape14_month)

if __name__ == '__main__':
    prophet1FloorL()
            

