# 应用情报 与 自然量 安装与7日收入 相关性

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.sensortower.intel import getAndroidCategoryRanking,getAndroidDownloadAndRevenue,getAndroidFeaturedDownloads

# 类别排名与自然量安装相关性
def corr1():
    df = getAndroidCategoryRanking('com.topwar.gp',category='all',countries='US',startDate='2023-01-01',endDate='2023-12-31')
    # date 是类似于 2023-01-01 的datetime，转禅城 2023-01 格式的字符串
    df['date'] = df['date'].apply(lambda x:x.strftime('%Y-%m'))
    df = df.groupby(['date']).agg({'rank':'mean'}).reset_index()

    df2 = getAndroidDownloadAndRevenue('com.topwar.gp',startDate='2023-01-01',endDate='2023-12-31')
    df = df.merge(df2,on=['date'],how='left')
    df.rename(columns={
        'date':'install_date',
        'downloads':'sensortower_downloads'
    },inplace=True)

    # sql = '''
    #     select
    #         count(distinct customer_user_id) as installs,
    #         to_char(
    #             to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
    #             "yyyy-mm"
    #         ) as install_date
    #     from
    #         ods_platform_appsflyer_events
    #     where
    #         app_id = 'com.topwar.gp'
    #         and event_name = 'install'
    #         and zone = 0
    #         and day >= 20230101
    #         and day <= 20231231
    #         and media_source is NULL
    #     group by
    #         install_date;
    # '''
    # df3 = execSql(sql)
    # df3.to_csv('/src/data/corr1_0.csv',index=False)
    df3 = pd.read_csv('/src/data/corr1_0.csv')

    df = df.merge(df3,on=['install_date'],how='left')

    df4 = getAndroidFeaturedDownloads('com.topwar.gp',startDate='2023-01-01',endDate='2023-12-31')
    # date 是类似于 2023-01-01 的str，转禅城 2023-01 格式的字符串
    df4['date'] = df4['date'].apply(lambda x:x[:7])
    df4 = df4.groupby(['date']).agg({'downloads':'sum'}).reset_index()
    df4.rename(columns={
        'date':'install_date',
        'downloads':'featured_downloads'
    },inplace=True)

    df = df.merge(df4,on=['install_date'],how='left')

    df['installs - featured_downloads'] = df['installs'] - df['featured_downloads']

    df['rank'] = df['rank'].apply(lambda x:round(x,1))

    df.rename(columns={
        'install_date':'安装日期',
        'rank':'排行榜平均名次',
        'sensortower_downloads':'下载量',
        'revenues':'收入(流水)',
        'installs':'自然量下载',
        'featured_downloads':'推荐排行榜下载',
        'installs - featured_downloads':'自然量下载 - 推荐排行榜下载'
    },inplace=True)
    
    df.to_csv('/src/data/corr1.csv',index=False)
    print(df)
    print(df.corr())
    df.corr().to_csv('/src/data/corr1_corr.csv')



def debug():
    sql = '''
        select
            count(distinct customer_user_id) as installs,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm"
            ) as install_date,
            media_source as media
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and event_name = 'install'
            and zone = 0
            and day >= 20230101
            and day <= 20231231
        group by
            install_date,
            media_source
        ;
    '''
    df3 = execSql(sql)
    df3.to_csv('/src/data/corr_debug.csv',index=False)

    # media为空的，media2是0，其他非空的，media2是1
    df3['media2'] = df3['media'].apply(lambda x:0 if pd.isnull(x) else 1)
    df = df3.groupby(['install_date','media2']).agg({'installs':'sum'}).reset_index()
    # df 打散成 media2=0 和 media2=1 变为两个列
    df = df.pivot(index='install_date',columns='media2',values='installs').reset_index()
    df.rename(columns={
        0:'自然量下载',
        1:'非自然量下载'
    },inplace=True)

    print(df.corr())

if __name__ == '__main__':
    # corr1()
    debug()