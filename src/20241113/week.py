import os
import pandas as pd
import numpy as np

import sys
sys.path.append('/src')
from src.maxCompute import execSql as execSql_local

execSql = execSql_local


def getData():
    sql = '''
SELECT
    install_day,
    SUM(usd) AS usd
FROM
    tmp_lw_cost_and_roi_by_day
WHERE
    install_day between '20240101' and '20241212'
GROUP BY
    install_day
;
'''
    data = execSql(sql)
    return data

def week():
    df = getData()
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')
    df['week'] = df['install_day'].dt.strftime('%Y-%W')
    df['monday'] = df['install_day'].dt.strftime('%Y%m%d')
    df = df.sort_values('install_day', ascending=True)

    df = df.groupby('week').agg({
        'usd': 'sum',
        'monday': 'first'
    }).reset_index()

    
    df['usd_pct'] = df['usd'].pct_change()

    print(df)

    print('usd pct max:', df['usd_pct'].max())
    print('usd pct min:', df['usd_pct'].min())

    print('usd pct > 0.1:', len(df[df['usd_pct'] > 0.1]))
    print('usd pct < -0.1:', len(df[df['usd_pct'] < -0.1]))

    print('usd pct > 0.2:', len(df[df['usd_pct'] > 0.2]))
    print('usd pct < -0.2:', len(df[df['usd_pct'] < -0.2]))

    print('usd pct > 0.3:', len(df[df['usd_pct'] > 0.3]))
    print('usd pct < -0.3:', len(df[df['usd_pct'] < -0.3]))

if __name__ == '__main__':
    week()

