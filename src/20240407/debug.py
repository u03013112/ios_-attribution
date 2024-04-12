import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql

def getSkanDataFromMC():
    filename = '/src/data/20240412_lw_skanData.csv'
    if not os.path.exists(filename):

        sql = '''
    select
        skad_conversion_value as cv,
        count(*) as count,
        day
    from
        ods_platform_appsflyer_skad_details
    where
        day between '20240327'
        and '20240411'
        AND app_id = 'id6448786147'
        AND event_name in ('af_skad_install', 'af_skad_redownload')
    group by
        skad_conversion_value,
        day;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'day':str})

    return df

def main():
    df = getSkanDataFromMC()
    df['cv'] = pd.to_numeric(df['cv'], errors='coerce')
    df['cv'] = df['cv'].fillna(-1)
    df.loc[df['cv']>=32,'cv'] -= 32

    # cv分组
    cvGroup = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]}
    ]

    df['cvGroup'] = 'unknown'
    for group in cvGroup:
        df.loc[df['cv'].isin(group['cvList']),'cvGroup'] = group['name']
    
    # 日期分组，20240327~20240402为第一周，20240403~20240409为第二周
    df['week'] = 'unknown'
    df.loc[df['day']<'20240403','week'] = 'week1'
    df.loc[
        ((df['day']>='20240403') & (df['day']<='20240409'))
        ,'week'] = 'week2'

    df = df.groupby(['cvGroup','week']).sum().reset_index()
    df.to_csv('/src/data/20240412_lw_skanDataGroup.csv', index=False)

    week1Df = df[df['week']=='week1']
    week1Df.to_csv('/src/data/20240412_lw_skanDataGroupWeek1.csv', index=False)
    week2Df = df[df['week']=='week2']
    week2Df.to_csv('/src/data/20240412_lw_skanDataGroupWeek2.csv', index=False)


if __name__ == '__main__':
    main()