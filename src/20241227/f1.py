# 直接使用付费金额预测7日ROI
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData(install_day_start, install_day_end):
    filename = f'/src/data/20241227_data_{install_day_start}_{install_day_end}.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''
SELECT
    install_day,
    mediasource as media,
    country,
    sum(usd) as cost,
    sum(d1) as revenue_d1,
    sum(d7) as revenue_d7
FROM
    tmp_lw_cost_and_roi_by_day
WHERE
    install_day BETWEEN '{install_day_start}' AND '{install_day_end}'
GROUP BY
    install_day,
    mediasource,
    country
;
        '''
        print("执行的SQL语句如下：\n")
        print(sql)
        
        # 执行SQL查询并返回结果
        data = execSql(sql)
        data.to_csv(filename, index=False)
    
    return data

def dataPreparation(df):
    # 将输入的df，进行数据预处理
    mediaList = [
        {
            'media':'Facebook Ads',
            'newName':'FACEBOOK',
        },{
            'media':'googleadwords_int',
            'newName':'GOOGLE',
        },{
            'media':'applovin_int',
            'newName':'APPLOVIN',
        },{
            'media':'Organic',
            'newName':'ORGANIC',
        }
    ]
    # 将mediasource字段的值进行替换，其他媒体统一替换为OTHER
    df.loc[:, 'media'] = df['media'].apply(lambda x: x if x in [item['media'] for item in mediaList] else 'OTHER')

    for item in mediaList:
        # df['media'] = df['media'].replace(item['media'], item['newName'])
        df.loc[df['media'] == item['media'], 'media'] = item['newName']

    df = df.groupby(['install_day', 'media', 'country']).agg({
        'cost': 'sum',
        'revenue_d1': 'sum',
        'revenue_d7': 'sum',
    }).reset_index()

    # 按照日期进行分组，计算总收入
    # 然后再加上 每个媒体两列： cost 和 revenue_d7 ，记作 {mediaName}_cost 和 {mediaName}_revenue_d7
    # 最终得到的结果是： install_day, total_revenue, FACEBOOK_cost, FACEBOOK_revenue_d7, GOOGLE_cost, GOOGLE_revenue_d7, APPLOVIN_cost, APPLOVIN_revenue_d7, ORGANIC_cost, ORGANIC_revenue_d7, OTHER_cost, OTHER_revenue_d7

    totalDf = df.groupby(['install_day']).agg({
        'cost': 'sum',
        'revenue_d1': 'sum',
        'revenue_d7': 'sum',
    }).reset_index()

    # 计算每个媒体的cost和revenue_d7
    for item in mediaList:
        media = item['newName']
        mediaCost = df[df['media'] == media].groupby(['install_day']).agg({
            'cost': 'sum',
            'revenue_d7': 'sum',
        }).reset_index()
        mediaCost.columns = ['install_day', f'{media}_cost', f'{media}_revenue_d7']
        totalDf = pd.merge(totalDf, mediaCost, on='install_day', how='left')

    return totalDf

def main():
    df = getHistoricalData('20240101', '20241212')
    usDf = df[df['country'] == 'US'].copy()

    usDf = dataPreparation(usDf)
    print(usDf)

if __name__ == '__main__':
    main()
