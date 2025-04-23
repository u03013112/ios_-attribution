import sys
sys.path.append('/src')

from src.maxCompute import execSql

import os
import datetime
import pandas as pd

def getDataFromMC(startDayStr, endDayStr):
    filename = f'/src/data/th_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)

    else:
        sql = f'''
select
    install_day AS day,
    app_package as appid,
    sum(cost_value_usd) as cost,
    sum(revenue_d1) as r1usd,
    sum(revenue_d7) as r7usd,
    sum(revenue_d30) as r30usd,
    sum(revenue_d60) as r60usd,
    sum(revenue_d90) as r90usd,
    sum(revenue_d120) as r120usd,
    sum(revenue_d150) as r150usd,
    sum(revenue_d180) as r180usd,
    sum(revenue_d210) as r210usd,
    sum(revenue_d240) as r240usd,
    sum(revenue_d270) as r270usd
from
    dws_overseas_public_roi
where
    app = '116'
    and zone = '0'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package
;
        '''
        print('从MC获得数据')
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def main():
    startDayStr = '20240701'
    endDayStr = '20250422'

    df = getDataFromMC(startDayStr, endDayStr)
    df['day'] = pd.to_datetime(df['day'], format='%Y%m%d')

    # 先计算出可以完整获得1日、7日、30日……的最后日期
    endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')
    endDay1 = endDay - datetime.timedelta(days=1)
    endDay7 = endDay - datetime.timedelta(days=7)
    endDay30 = endDay - datetime.timedelta(days=30)
    endDay60 = endDay - datetime.timedelta(days=60)
    endDay90 = endDay - datetime.timedelta(days=90)
    endDay120 = endDay - datetime.timedelta(days=120)
    endDay150 = endDay - datetime.timedelta(days=150)
    endDay180 = endDay - datetime.timedelta(days=180)
    endDay210 = endDay - datetime.timedelta(days=210)
    endDay240 = endDay - datetime.timedelta(days=240)
    endDay270 = endDay - datetime.timedelta(days=270)

    print('endDay1:', endDay1)
    print('endDay7:', endDay7)
    print('endDay30:', endDay30)
    print('endDay60:', endDay60)
    print('endDay90:', endDay90)
    print('endDay120:', endDay120)
    print('endDay150:', endDay150)
    print('endDay180:', endDay180)
    print('endDay210:', endDay210)
    print('endDay240:', endDay240)
    print('endDay270:', endDay270)

    df = df[df['day'] <= endDay1].copy()
    df.loc[df['day']> endDay7,'r7usd'] = None
    df.loc[df['day']> endDay30,'r30usd'] = None
    df.loc[df['day']> endDay60,'r60usd'] = None
    df.loc[df['day']> endDay90,'r90usd'] = None
    df.loc[df['day']> endDay120,'r120usd'] = None
    df.loc[df['day']> endDay150,'r150usd'] = None
    df.loc[df['day']> endDay180,'r180usd'] = None
    df.loc[df['day']> endDay210,'r210usd'] = None
    df.loc[df['day']> endDay240,'r240usd'] = None
    df.loc[df['day']> endDay270,'r270usd'] = None

    df['month'] = df['day'].dt.strftime('%Y%m')
    
    def custom_sum(series):
        return series.sum() if not series.isnull().any() else None

    df = df.groupby(['month', 'appid']).agg({
        'cost': custom_sum,
        'r1usd': custom_sum,
        'r7usd': custom_sum,
        'r30usd': custom_sum,
        'r60usd': custom_sum,
        'r90usd': custom_sum,
        'r120usd': custom_sum,
        'r150usd': custom_sum,
        'r180usd': custom_sum,
        'r210usd': custom_sum,
        'r240usd': custom_sum,
        'r270usd': custom_sum
    }).reset_index()

    df = df.sort_values(by=['month', 'appid'])
    # print(df[df['appid'] == 'com.greenmushroom.boomblitz.gp'])    

    appidList = df['appid'].unique()

    for appid in appidList:
        print(f'appid: {appid}')
        appDf = df[df['appid'] == appid]
        appDf = appDf.sort_values(by=['month'])
        appDf.to_csv(f'/src/data/th2_{appid}_20240701_20250422.csv', index=False)
        print(appDf.corr())
        print('')

    # 不分app
    df2 = df.groupby(['month']).agg({
        'cost': custom_sum,
        'r1usd': custom_sum,
        'r7usd': custom_sum,
        'r30usd': custom_sum,
        'r60usd': custom_sum,
        'r90usd': custom_sum,
        'r120usd': custom_sum,
        'r150usd': custom_sum,
        'r180usd': custom_sum,
        'r210usd': custom_sum,
        'r240usd': custom_sum,
        'r270usd': custom_sum
    }).reset_index()
    df2 = df2.sort_values(by=['month'])
    df2.to_csv('/src/data/th2_20240701_20250422.csv', index=False)
    print('不分appid:')
    print(df2.corr())
    print('')

def debug():
    df = pd.read_csv('/src/data/th2_20240701_20250422.csv')

    cols = df.columns.tolist()
    # 去掉第一列
    cols = cols[1:]
    print(cols)

    for i in range(len(cols)-1):
        col0 = cols[i]
        col1 = cols[i+1]

        # 计算比例
        df0 = df[[col0, col1]].copy()
        # 排除空值所在的行
        df0 = df0.dropna()

        if df0[col1].sum() == 0 or df0[col0].sum() == 0:
            continue

        # print(df0[col1].sum(), df0[col0].sum())

        ratio = df0[col1].sum() / df0[col0].sum()
        print(f'{col1} / {col0} = {ratio}')
        
        



if __name__ == '__main__':
    # main()
    debug()
