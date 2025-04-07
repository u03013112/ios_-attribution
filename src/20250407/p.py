import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def main():
    df03 = pd.read_csv('lastwar_inapps_20250403.csv')
    df04 = pd.read_csv('lastwar_inapps_20250404.csv')
    df05 = pd.read_csv('lastwar_inapps_20250405.csv')

    print('列名:')
    print(df03.columns)
    print(df04.columns)

    df03 = df03.sort_values(by=['app_id','skad_conversion_value'])
    print(df03[(df03['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())
    df04 = df04.sort_values(by=['app_id','skad_conversion_value'])
    print(df04[(df04['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())
    df05 = df05.sort_values(by=['app_id','skad_conversion_value'])
    print(df05[(df05['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())


    df03 = df03[(df03['app_id'] == 'id6448786147') & (df03['skad_conversion_value'] == 32)]
    df04 = df04[(df04['app_id'] == 'id6448786147') & (df04['skad_conversion_value'] == 32)]


    print('len df03:',len(df03))
    print('len df04:',len(df04))
    return

    # print('event_name count:')
    # df03G = df03.groupby(['event_name']).size().reset_index(name='count')
    # df04G = df04.groupby(['event_name']).size().reset_index(name='count')
    # print(df03G)
    # print(df04G)

    for col in df03.columns:
        if col not in df04.columns:
            print(f"列 {col} 在 df03 中存在，但在 df04 中不存在。")
            continue
        
        if col == 'app_id':
            continue

        print(f"==================\n列 {col} 的 count:")
        df03G = df03.groupby([col]).size().reset_index(name='count')
        df04G = df04.groupby([col]).size().reset_index(name='count')
        print('20250403:')
        print(df03G)
        print('')
        print('20250404:')
        print(df04G)

    

def getSKANDataFromMC(dayStr, days):
    dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')

    sql = f'''
        SELECT
            skad_conversion_value as cv,
            count(*) as cnt,
            day
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day between '{dayBeforeStr}' and '{dayStr}'
            AND app_id = 'id6448786147'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
        GROUP BY
            skad_conversion_value,
            day
        ;
    '''

    df = execSql(sql)
    return df


def forHaitao():
    sql = '''
select
*
from 
ods_platform_appsflyer_skad_postbacks_copy
where
    day = 20250401
    and app_id in ('6448786147')
;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/haitao.csv', index=False)


def forHaitao2():
    sql = '''
select *
from
ods_platform_appsflyer_skad_details
where
    day = 20250401
    and app_id in ('id6448786147')
;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/haitao2.csv', index=False)


def p1():
    df = getSKANDataFromMC('20250407', 7)
    # 按照day，cv排序
    # 画图，day为x轴，cnt为y轴，每个cv一张图
    # 保存到文件 /src/data/20250407_cv{cv}.png
    cvList = df['cv'].unique()
    for cv in cvList:
        df2 = df[df['cv'] == cv]
        df2 = df2.sort_values(by=['day'])
        df2['day'] = pd.to_datetime(df2['day'], format='%Y%m%d')
        df2.plot(x='day', y='cnt', title=f'cv={cv}', kind='line')
        plt.savefig(f'/src/data/20250407_cv{cv}.png')
        plt.close()


if __name__ == "__main__":
    # main()
    # forHaitao()
    # forHaitao2()
    p1()