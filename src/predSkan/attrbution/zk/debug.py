import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql, execSqlBj


def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

# 获得ssot的安装数
def debug01():
    sql = '''
        select
            SUM(CAST(REPLACE(total_attributions, ',', '') AS INT)) as installs,
            campaign,
            day
        from
            rg_ai_bj.ads_appsflyer_ssot
        where
            day > 20230501
            and day < 20230701
        group by
            campaign,
            day
        ;
    '''
    df = execSqlBj(sql)
    return df

def debug01_1():
    # 获得af event中有归因的用户安装数
    sql = '''
        select
            count(distinct appsflyer_id) as installs,
            media_source,
            day
        from
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN 20230501
            AND 20230701
            and event_name = 'install'
            AND media_source IS not NULL
        group by
            media_source,
            day;
    '''
    df = execSql(sql)
    return df

def debug01_2():
    # 获得skan中cv<32的用户数
    sql = '''
        SELECT
            media_source,
            count(*) as installs,
            day
        FROM
            ods_platform_appsflyer_skad_details
        WHERE
            day > 20230501
            and day < 20230701
            AND app_id = 'id1479198816'
            AND event_name in ('af_skad_install', 'af_skad_redownload')
            AND skad_conversion_value < 32
        group by
            media_source,
            day
        ;
    '''
    df = execSql(sql)
    return df

# 获得skan的安装数
def debug02():
    sql = '''
        select
            media_source,
            count(*) as installs,
            day
        from
            ods_platform_appsflyer_skad_details
        where
            day > 20230501
            and day < 20230701
            AND app_id = 'id1479198816'
            AND event_name in ('af_skad_install', 'af_skad_redownload')
        group by
            media_source,
            day;
    '''
    df = execSql(sql)
    return df

# 获得skan的安装数，不要重下载
def debug02_1():
    sql = '''
        select
            media_source,
            count(*) as installs,
            day
        from
            ods_platform_appsflyer_skad_details
        where
            day > 20230501
            and day < 20230701
            AND app_id = 'id1479198816'
            AND event_name = 'af_skad_install'
        group by
            media_source,
            day;
    '''
    df = execSql(sql)
    return df

def debug03():
    df1 = pd.read_csv(getFilename('debug01'))
    # 将campaign映射成media
    df1['media_source'] = 'unknown'
    df1.loc[df1['campaign'].str.contains('VO|AEO'), 'media_source'] = 'Facebook Ads'
    df1.loc[df1['campaign'].str.contains('Tiktok'), 'media_source'] = 'bytedanceglobal_int'
    df1.loc[df1['campaign'].str.contains('UAC'), 'media_source'] = 'googleadwords_int'
    df1.drop(columns=['campaign'], inplace=True)
    df1 = df1.groupby(['media_source', 'day']).sum().reset_index()

    df2 = pd.read_csv(getFilename('debug02'))

    df = df1.merge(df2, on=['media_source','day'], how='left', suffixes=('_ssot','_skan'))

    df2_1 = pd.read_csv(getFilename('debug02_1'))
    df2_1.rename(columns={
        'installs':'installs_skan_no_redownload'
    }, inplace=True)
    df = df.merge(df2_1, on=['media_source','day'], how='left')

    df.to_csv(getFilename('debug03'), index=False)

import matplotlib.pyplot as plt
def draw03():
    fig, ax1 = plt.subplots(figsize=(24, 6))
    # 针对debug03进行绘图
    df = pd.read_csv(getFilename('debug03'))
    df = df.loc[df['media_source'] != 'unknown']
    df = df[['media_source','day','installs_ssot','installs_skan']]
    df.to_csv(getFilename('draw03'), index=False)
    # 先画一张整体的图，不分媒体，按照日期，画出ssot和skan的安装数
    plt.title('installs_skan vs installs_ssot')
    df['day'] = pd.to_datetime(df['day'], format='%Y%m%d')
    df['day'] = df['day'].dt.strftime('%Y-%m-%d')

    dfTotal = df.groupby(['day']).sum().reset_index()
    # day作为x，installs_skan，installs_ssot用不同颜色
    ax1.plot(dfTotal['day'], dfTotal['installs_skan'], label='installs_skan', color='red')
    ax1.plot(dfTotal['day'], dfTotal['installs_ssot'], label='installs_ssot', color='blue')

    plt.xticks(dfTotal['day'][::14], rotation=45)
    plt.legend()
    plt.savefig(f'/src/data/zk2/debug03_total.jpg', bbox_inches='tight')
    plt.close()

    # 分媒体，每个媒体画一张图
    for media in df['media_source'].unique():
        fig, ax1 = plt.subplots(figsize=(24, 6))
        plt.title('installs_skan vs installs_ssot %s'%media)
        dfMedia = df.loc[df['media_source'] == media]
        ax1.plot(dfMedia['day'], dfMedia['installs_skan'], label='installs_skan', color='red')
        ax1.plot(dfMedia['day'], dfMedia['installs_ssot'], label='installs_ssot', color='blue')
        plt.xticks(dfMedia['day'][::14], rotation=45)
        plt.legend()

        plt.savefig(f'/src/data/zk2/debug03_{media}.jpg', bbox_inches='tight')
        plt.close()


def debug04():
    df = pd.read_csv(getFilename('debug03'))

    df = df.loc[df['media_source'] != 'unknown']

    # df = df.loc[(df['day']>=20230501) & (df['day']<=20230630)]

    dfCopy = df.copy()

    df = df.groupby(['media_source']).sum().reset_index()
    df['installs_skan/installs_ssot'] = df['installs_skan'] / df['installs_ssot']
    df['installs_skan_no_redownload/installs_ssot'] = df['installs_skan_no_redownload'] / df['installs_ssot']

    # df = df[['media_source','day','installs_skan/installs_ssot','installs_skan_no_redownload/installs_ssot']]
    df = df[['media_source','installs_skan/installs_ssot']]
    print(df)

    dfgroup = df.mean().reset_index()
    # print(dfgroup)
    df_str = dfgroup.to_string(index=False)
    print(df_str)

    df = dfCopy.copy()

    df['day'] = df['day'].astype(str).str.slice(0,6)
    df = df.groupby(['media_source','day']).sum().reset_index()
    df['installs_skan/installs_ssot'] = df['installs_skan'] / df['installs_ssot']

    dfTotal = df.groupby(['day']).sum().reset_index()
    dfTotal['installs_skan/installs_ssot'] = dfTotal['installs_skan'] / dfTotal['installs_ssot']
    print(dfTotal[['day','installs_skan/installs_ssot']])

    print(df[['media_source','day','installs_skan/installs_ssot']])


def debug05():
    df1_1 = pd.read_csv(getFilename('debug01_1'))
    # df1_1 中media_source 中replace 'restricted' to 'Facebook Ads'
    df1_1['media_source'] = df1_1['media_source'].str.replace('restricted','Facebook Ads')

    df1_2 = pd.read_csv(getFilename('debug01_2'))

    df1 = df1_1.merge(df1_2, on=['media_source','day'], how='left', suffixes=('_af','_skan'))
    df1['installs'] = df1['installs_af'] + df1['installs_skan']
    df1.drop(columns=['installs_af','installs_skan'], inplace=True)

    df1 = df1.loc[df1['media_source'].isin(['Facebook Ads','bytedanceglobal_int','googleadwords_int'])]
    df1 = df1.loc[df1['installs'] > 0]

    df2 = pd.read_csv(getFilename('debug02'))

    df = df1.merge(df2, on=['media_source','day'], how='left', suffixes=('_ssot','_skan'))

    df2_1 = pd.read_csv(getFilename('debug02_1'))
    df2_1.rename(columns={
        'installs':'installs_skan_no_redownload'
    }, inplace=True)
    df = df.merge(df2_1, on=['media_source','day'], how='left')

    df.to_csv(getFilename('debug05'), index=False)

    df = df.loc[df['media_source'] != 'unknown']

    # 列 day 是类似 20230501 的格式
    # 转化成 202305，即只保留月份
    # 然后按照月份进行汇总
    # 计算installs_skan/installs_ssot
    # 计算installs_skan_no_redownload/installs_ssot
    # 并打印到终端

    df['day'] = df['day'].astype(str).str.slice(0,6)
    df = df.groupby(['media_source','day']).sum().reset_index()
    df['installs_skan/installs_ssot'] = df['installs_skan'] / df['installs_ssot']
    df['installs_skan_no_redownload/installs_ssot'] = df['installs_skan_no_redownload'] / df['installs_ssot']

    # df = df[['media_source','day','installs_skan/installs_ssot','installs_skan_no_redownload/installs_ssot']]
    df = df[['media_source','day','installs_skan/installs_ssot']]
    
    # print(df)
    # 将DataFrame转换为一个长字符串
    df_str = df.to_string(index=False)

    # 使用print函数打印这个字符串，并使用replace函数将所有的换行符替换为一个空格
    print(df_str)


if __name__ == '__main__':
    # df1 = debug01()
    # df1.to_csv(getFilename('debug01'), index=False)

    # df1_1 = debug01_1()
    # df1_1.to_csv(getFilename('debug01_1'), index=False)

    # df1_2 = debug01_2()
    # df1_2.to_csv(getFilename('debug01_2'), index=False)

    # df2 = debug02()
    # df2.to_csv(getFilename('debug02'), index=False)

    # df2_1 = debug02_1()
    # df2_1.to_csv(getFilename('debug02_1'), index=False)

    # debug03()

    debug04()

    # debug05()

    # draw03()