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
    SUBSTRING(install_day, 1, 6) AS month,
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
    SUBSTRING(install_day, 1, 6),
    app_package
;
        '''
        print('从MC获得数据')
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def main():
    df = getDataFromMC('20240701', '20250422')

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
        'cost': 'sum',
        'r1usd': 'sum',
        'r7usd': 'sum',
        'r30usd': 'sum',
        'r60usd': 'sum',
        'r90usd': 'sum',
        'r120usd': 'sum',
        'r150usd': 'sum',
        'r180usd': 'sum',
        'r210usd': 'sum',
        'r240usd': 'sum',
        'r270usd': 'sum'
    }).reset_index()
    df2 = df2.sort_values(by=['month'])
    df2.to_csv('/src/data/th2_20240701_20250422.csv', index=False)
    print('不分appid:')
    print(df2.corr())
    print('')

if __name__ == '__main__':
    main()


