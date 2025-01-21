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

if __name__ == '__main__':
    getAddScoreData(startDayStr='2024-11-25',endDayStr='2025-01-20')