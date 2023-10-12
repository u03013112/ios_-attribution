# 获取数据
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)


# 从tmp_unique_id表里获得install_timestamp,game_uid,country_code
# where 条件：app=102，app_id='id1479198816',mediasource='bytedanceglobal_int'
# 再加上install_timestamp >= 20230101对应的unix时间戳（秒）
# 再从ods_platform_appsflyer_events表中获得customer_user_id,event_timestamp,event_revenue_usd
# where条件：app_id='id1479198816'，AND day >= '20230101' AND event_name in ('af_purchase_oldusers','af_purchase') AND zone = 0
# 将上面两个表left join，game_uid = customer_user_id
# 注意要过滤掉事件时间戳小于安装时间戳的数据
# 然后进行汇总，按照install_timestamp按小时汇总，并将付费信息按小时汇总，只关注第1小时~第24小时
# 最终我希望得到的是比如2023-01-01 0时~1时 用户第一小时付费金额~第24小时付费金额，以及第7天付费金额
# 其中7天付费金额是自然日7天，即将安装时间戳转换成utc0的日期，事件时间戳也转成utc0的日期，将事件日期-安装日期小于7的所有付费金额汇总，就是7日付费金额

def getData1New():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
            WHERE
                o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            country_code,
            HOUR(FROM_UNIXTIME(event_timestamp)) AS hour,
            SUM(event_revenue_usd) AS revenue
        FROM
            joined_data
        WHERE
            DATE(FROM_UNIXTIME(event_timestamp)) = DATE(FROM_UNIXTIME(install_timestamp))
        GROUP BY
            install_date,
            hour,
            country_code;
    '''
    print(sql)
    df = execSql(sql)
    return df


# 获取用户数量
def getUserCount():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            country_code,
            HOUR(install_timestamp) AS hour,
            count(distinct game_uid) AS user_count
        FROM
            tmp_unique_id
        GROUP BY
            install_date,
            hour,
            country_code;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 获取付费用户数量
def getPayUserCount():
    str1 = ''
    for i in range(24):
        str1 += '''
            COUNT(
                DISTINCT CASE
                    WHEN first_pay_hour <= %d THEN game_uid
                    ELSE NULL
                END
            ) AS cumulative_pay_user_count_%d,
        '''%(i,i)

    # print(str1)

    sql = '''
        WITH install_data AS (
            SELECT
                game_uid,
                country_code,
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                to_char(
                    FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
                    'YYYYMMDD'
                ) AS install_date
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        event_data AS (
            SELECT
                customer_user_id,
                event_timestamp,
                FLOOR((event_timestamp %% 86400) / 3600) AS event_hour
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
                AND event_revenue_usd > 0
        ),
        joined_data AS (
            SELECT
                i.install_date,
                i.country_code,
                i.game_uid,
                MIN(e.event_timestamp) AS first_pay_timestamp,
                FLOOR((MIN(e.event_timestamp) %% 86400) / 3600) AS first_pay_hour
            FROM
                install_data i
                JOIN event_data e ON i.game_uid = e.customer_user_id
                AND e.event_timestamp >= i.install_timestamp
            GROUP BY
                i.install_date,
                i.country_code,
                i.game_uid
        )
        SELECT
            install_date,
            %s
            country_code
        FROM
            joined_data
        GROUP BY
            install_date,
            country_code;
    '''%(str1)
    print(sql)
    df = execSql(sql)
    return df

def getData2New():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
            WHERE
                o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            country_code,
            SUM(event_revenue_usd) AS revenue
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 7
        GROUP BY
            install_date,
            country_code;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 获得3日内付费数据
def getData3New():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
            WHERE
                o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            country_code,
            SUM(event_revenue_usd) AS revenue
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 3
        GROUP BY
            install_date,
            country_code;
    '''
    print(sql)
    df = execSql(sql)
    return df



def sqlTest():
    sql = '''
        select
            sum(revenue_value_usd) as revenue
        from
            dwd_overseas_revenue_afattribution_realtime
        where
            app = 102
            and zone = 0
            and window_cycle = 9999
            and day >= 20230910
            and app_package = 'id1479198816'
            and install_day = 20230910
            and mediasource = 'bytedanceglobal_int'
            and DATEDIFF(
                FROM_UNIXTIME(cast (event_time as bigint)),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 7;
    '''
    print(sql)
    df = execSql(sql)
    print(df)

# 获得用户级别的付费数据，暂时忽略7日内完全不付费用户
def getUserPayData():
    sql = ''''
    '''
    print(sql)
    df = execSql(sql)
    return df

def check1():
    df = pd.read_csv(getFilename('20230919_1_new'))

def check2():
    df = pd.read_csv(getFilename('20230919_2'))
    # 列install_timestamp_fix_by_hour 是unix时间戳，秒，需要转换成日期格式类似 2023-01-01
    df['install_date'] = pd.to_datetime(df['install_timestamp_fix_by_hour'],unit='s').dt.strftime('%Y-%m-%d')
    df = df.groupby(['install_date']).agg({'revenue':'sum'})
    df = df.sort_values(by='install_date',ascending=False)
    print(df.head(10))

def geoGroup():
    # 国家太多了，需要进行分组
    # 先对国家进行汇总，查看哪些国家的付费金额比较大，只对比7日付费金额
    # 将付费金额较小的国家进行合并
    df = pd.read_csv(getFilename('20230919_2'))
    df = df.groupby(['country_code']).agg({'revenue':'sum'})
    df = df.sort_values(by='revenue',ascending=False)
    sumRevenue = df['revenue'].sum()
    df['rate'] = df['revenue'] / sumRevenue
    print(df.head(30))

def dataMerge():
    df1 = pd.read_csv(getFilename('20230919_1_new'))
    
    df_full = pd.MultiIndex.from_product([df1['install_date'].unique(), 
                                      df1['country_code'].unique(), 
                                      np.arange(24)], 
                                     names=['install_date', 'country_code', 'hour']).to_frame(index=False)

    # 将完整的数据框与原始数据进行合并，填充缺失的'revenue'值为0
    df_full = pd.merge(df_full, df1, on=['install_date', 'country_code', 'hour'], how='left')
    df_full['revenue'].fillna(0, inplace=True)

    # 对'revenue'进行累计求和
    df_full['revenue'] = df_full.groupby(['install_date', 'country_code'])['revenue'].cumsum()

    # 进行数据透视
    df_pivot = df_full.pivot_table(index=['install_date', 'country_code'], 
                                columns='hour', 
                                values='revenue').reset_index()

    # 重命名列名
    df_pivot.columns = ['install_date', 'country_code'] + ['r{}H_usd'.format(i) for i in range(24)]

    # 打印结果
    # print(df_pivot)

    aggDict = {}
    for i in range(24):
        aggDict['r%dH_usd'%i] = 'sum'
    dfGroup = df_pivot.groupby(by = ['install_date','country_code']).agg(aggDict)
    dfGroup = dfGroup.sort_values(by=['install_date','country_code'],ascending=False)
    # print(dfGroup.head(10))

    df2 = pd.read_csv(getFilename('20230919_2_new'))
    df2.rename(columns={'revenue':'r7D_usd'},inplace=True)
    
    df = pd.merge(dfGroup,df2,on=['install_date','country_code'],how='left')
    df = df.sort_values(by=['install_date','country_code'],ascending=True)

    df.to_csv(getFilename('20230919_3'),index=False)
    aggDict = {}
    for i in range(24):
        aggDict['r%dH_usd'%i] = 'sum'
    aggDict['r7D_usd'] = 'sum'
    df = df.groupby(by = ['install_date']).agg(aggDict).reset_index()
    df.to_csv(getFilename('20230919_3_sum'),index=False)

# merge user count
def dataMerge2():
    df1 = pd.read_csv(getFilename('20230919_user_count'))
    
    df_full = pd.MultiIndex.from_product([df1['install_date'].unique(), 
                                      df1['country_code'].unique(), 
                                      np.arange(24)], 
                                     names=['install_date', 'country_code', 'hour']).to_frame(index=False)

    # 将完整的数据框与原始数据进行合并，填充缺失的'revenue'值为0
    df_full = pd.merge(df_full, df1, on=['install_date', 'country_code', 'hour'], how='left')
    df_full['user_count'].fillna(0, inplace=True)

    df_full['user_count'] = df_full.groupby(['install_date', 'country_code'])['user_count'].cumsum()

    # 进行数据透视
    df_pivot = df_full.pivot_table(index=['install_date', 'country_code'], 
                                columns='hour', 
                                values='user_count').reset_index()

    # 重命名列名
    df_pivot.columns = ['install_date', 'country_code'] + ['{}H_user_count'.format(i) for i in range(24)]

    # 打印结果
    # print(df_pivot)

    df_pivot = df_pivot.sort_values(by=['install_date','country_code'],ascending=False)
    # print(dfGroup.head(10))

    df2 = pd.read_csv(getFilename('20230919_2_new'))
    df2.rename(columns={'revenue':'r7D_usd'},inplace=True)
    
    df = pd.merge(df_pivot,df2,on=['install_date','country_code'],how='left')
    df = df.sort_values(by=['install_date','country_code'],ascending=True)

    payUserCountDf = pd.read_csv(getFilename('20230919_pay_user_count'))
    df = pd.merge(df,payUserCountDf,on=['install_date','country_code'],how='left')
    df.to_csv(getFilename('20230919_3_user_count'),index=False)

    # df.to_csv(getFilename('20230919_3_user_count'),index=False)
    # aggDict = {}
    # for i in range(24):
    #     aggDict['%dH_user_count'%i] = 'sum'
    # aggDict['r7D_usd'] = 'sum'
    # df = df.groupby(by = ['install_date']).agg(aggDict).reset_index()
    # # df.to_csv(getFilename('20230919_3_sum_user_count'),index=False)

    # payUserCountDf = pd.read_csv(getFilename('20230919_pay_user_count'))
    # payUserCountDf = payUserCountDf.drop(columns=['country_code'])
    # payUserCountGroupDf = payUserCountDf.groupby(by = ['install_date']).sum().reset_index()

    # df = pd.merge(df,payUserCountGroupDf,on=['install_date'],how='left')
    # df.to_csv(getFilename('20230919_3_sum_user_count'),index=False)

# 暂时不合并国家
def mergeAll():
    df = pd.read_csv(getFilename('20230919_3_user_count'))
    df2 = pd.read_csv(getFilename('20230919_3'))
    df2 = df2.drop(columns=['r7D_usd'])

    df = df.merge(df2,on=['install_date','country_code'],how='left')
    df.fillna(0,inplace=True)
    df.to_csv(getFilename('20230919_3_all'),index=False)

def dataMergeAll():
    df = pd.read_csv(getFilename('20230919_3_sum'))
    df2 = pd.read_csv(getFilename('20230919_3_sum_user_count'))
    df2 = df2.drop(columns=['r7D_usd'])
    df = pd.merge(df,df2,on=['install_date'],how='left')
    for i in range(24):
        # 列改名 cumulative_pay_user_count_0 -> 0H_pay_user_count
        df.rename(columns={'cumulative_pay_user_count_%d'%i:'%dH_pay_user_count'%i},inplace=True)
        # 列改名 r0H_usd -> 0H_revenue_usd
        df.rename(columns={'r%dH_usd'%i:'%dH_revenue_usd'%i},inplace=True)
        # 列 0H_revenue_usd 保留2位小数
        df['%dH_revenue_usd'%i] = df['%dH_revenue_usd'%i].round(2)
        # 列类型 0H_user_count，改为 int
        df['%dH_user_count'%i].fillna(0,inplace=True)
        df['%dH_user_count'%i] = df['%dH_user_count'%i].astype(int)

    # 再调整列的顺序，最先是install_date，然后是0H_revenue_usd，1H_revenue_usd，...，23H_revenue_usd，0H_pay_user_count，1H_pay_user_count，...，23H_pay_user_count，r7D_usd
    df = df[['install_date'] + ['%dH_revenue_usd'%i for i in range(24)] + ['%dH_user_count'%i for i in range(24)] + ['%dH_pay_user_count'%i for i in range(24)] + ['r7D_usd']]
    df.to_csv(getFilename('20230919_3_sum_all'),index=False)

def merge3():
    df = pd.read_csv(getFilename('20230919_3_sum_all'))
    df3 = pd.read_csv(getFilename('20230919_3_new'))
    df3 = df3.rename(columns={'revenue':'r3D_usd'})
    df3 = df3.groupby(by=['install_date']).agg({'r3D_usd':'sum'})
    df = df.merge(df3,on=['install_date'],how='left')
    df = df.sort_values(by=['install_date'],ascending=True)

    df['install_date'] = df['install_date'].astype(str)

    dateList = []
    for i in range(1,10):
        date = {
            'name':'2023年0%d月'%i,
            'start':'20230%d01'%i,
            'end':'20230%d01'%(i+1)
        }
        dateList.append(date)

    for date in dateList:
        df0 = df[(df['install_date'] >= date['start']) & (df['install_date'] < date['end'])]
        corr = df0.corr()

        print(date['name'])
        print('r3D 10H corr:',corr['r3D_usd']['9H_revenue_usd'])
        print('r3D 12H corr:',corr['r3D_usd']['11H_revenue_usd'])
        print('r3D 14H corr:',corr['r3D_usd']['13H_revenue_usd'])
        print('')
        print('r7D 10H corr:',corr['r7D_usd']['9H_revenue_usd'])
        print('r7D 12H corr:',corr['r7D_usd']['11H_revenue_usd'])
        print('r7D 14H corr:',corr['r7D_usd']['13H_revenue_usd'])


def checkCorr():
    df = pd.read_csv(getFilename('20230919_3_sum_all'))
    # for i in range(24):
    #     df['%dH_pay_rate'%i] = df['%dH_pay_user_count'%i] / df['%dH_user_count'%i]
    #     df['%dH_ARPPU'%i] = df['%dH_revenue_usd'%i] / df['%dH_pay_user_count'%i]
    #     df['%dH_ARPPU'%i] = df['%dH_revenue_usd'%i] / df['%dH_pay_user_count'%i]
    corrDf = df.corr()
    corrDf['r7D_usd_sqre'] = corrDf['r7D_usd'] ** 2
    # 设置显示选项以避免省略行
    pd.set_option('display.max_rows', None)

    print(corrDf[['r7D_usd','r7D_usd_sqre']])
    corrDf[['r7D_usd','r7D_usd_sqre']].to_csv(getFilename('20230919_3_sum_all_corr'),index=False)

def autoCorr():
    df = pd.read_csv(getFilename('20230919_3_sum_all'))
    # 查看每天的列 0H_revenue_usd~23H_revenue_usd 的自相关性
    for i in range(24):
        print('第%d小时'%i)
        for j in [1,2,3]:
            print('%d:'%j,df['%dH_revenue_usd'%i].autocorr(j))


def geoCorr():
    geoList = [
        {'name':'US','codeList':['US']},
        {'name':'KR','codeList':['KR']},
        {'name':'UK','codeList':['UK']},
        {'name':'AU','codeList':['AU']},
        {'name':'CA','codeList':['CA']},
        {'name':'SG','codeList':['SG']},
        {'name':'DE','codeList':['DE']},
        {'name':'JP','codeList':['JP']},
        {'name':'TW','codeList':['TW']},
        {'name':'Europe','codeList':['DE','FR','UK','IT','RU']},
    ]

    df = pd.read_csv(getFilename('20230919_3_all'))
    df.fillna(0,inplace=True)

    for geo in geoList:
        geo_name = geo['name']
        code_list = geo['codeList']
        geo_df = df[df['country_code'].isin(code_list)]

        aggDict = {
            'r7D_usd': 'sum',
        }
        for i in range(24):
            aggDict['r%dH_usd' % i] = 'sum'
            aggDict['%dH_user_count' % i] = 'sum'
            aggDict['cumulative_pay_user_count_%d' % i] = 'sum'
        
        geo_agg_df = geo_df.groupby(by=['install_date']).agg(aggDict)

        print('{} 相关系数'.format(geo_name))
        print('首日付费金额相关系数：',geo_agg_df.corr()['r7D_usd']['r23H_usd'])
        print('用户数量相关系数：',geo_agg_df.corr()['r7D_usd']['23H_user_count'])
        print('付费用户相关系数：',geo_agg_df.corr()['r7D_usd']['cumulative_pay_user_count_23'])

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def debug():
    df = pd.read_csv(getFilename('20230919_3_sum_all'))
    df = df.groupby(['install_date']).agg({
        '23H_revenue_usd':'sum',
        'r7D_usd':'sum'
    })
    df = df.sort_values(by='install_date',ascending=True).reset_index()
    
    # install_date 是类似 20230101 的字符串，需要转换成日期格式
    df['install_date'] = pd.to_datetime(df['install_date'],format='%Y%m%d')
    print(df.head(20))

    fig, ax1 = plt.subplots(figsize=(24, 6))

    plt.title('tiktok iOS')

    # Plot r7usd and r7usdp on the left y-axis
    ax1.plot(df['install_date'], df['23H_revenue_usd'], label='r1usd')
    ax1.plot(df['install_date'], df['r7D_usd'], label='r7usd')
    ax1.set_ylabel('r1usd and r7usd')
    ax1.set_xlabel('Install Date')

    ax1.legend()

    # Save the plot as a jpg image
    plt.savefig(f'/src/data/zk2/20230919.jpg', bbox_inches='tight')
    plt.close()

# 
def r1r7corr(fromDayStr,toDayStr):
    df = pd.read_csv(getFilename('20230919_3_sum_all'),dtype={'install_date':str})
    df = df[(df['install_date'] >= fromDayStr) & (df['install_date'] <= toDayStr)]
    df = df.groupby(['install_date']).agg({
        '23H_revenue_usd':'sum',
        'r7D_usd':'sum'
    })
    df = df.sort_values(by='install_date',ascending=True).reset_index()
    print('from %s to %s'%(fromDayStr,toDayStr))
    print('相关系数：',df.corr()['23H_revenue_usd']['r7D_usd'])

    print('平均r7/r1：',df['r7D_usd'].sum() / df['23H_revenue_usd'].sum())

    # install_date 是类似 20230101 的字符串，需要转换成日期格式
    df['install_date'] = pd.to_datetime(df['install_date'],format='%Y%m%d')
    df['r7/r1'] = df['r7D_usd'] / df['23H_revenue_usd']

    df['r1 rolling7'] = df['23H_revenue_usd'].rolling(7).mean()
    df['r7 rolling7'] = df['r7D_usd'].rolling(7).mean()
    df['r7/r1 rolling7'] = df['r7/r1'].rolling(7).mean()


    fig, ax1 = plt.subplots(figsize=(24, 6))

    plt.title('tiktok iOS')

    # Plot r7usd and r7usdp on the left y-axis
    ax1.plot(df['install_date'], df['23H_revenue_usd'], label='r1usd',alpha=0.5)
    ax1.plot(df['install_date'], df['r7D_usd'], label='r7usd',alpha=0.5)
    ax1.plot(df['install_date'], df['r1 rolling7'], label='r1 rolling7')
    ax1.plot(df['install_date'], df['r7 rolling7'], label='r7 rolling7')

    ax1.set_ylabel('r1usd and r7usd')
    ax1.set_xlabel('Install Date')

    ax2 = ax1.twinx()
    ax2.plot(df['install_date'], df['r7/r1'], label='r7/r1', linestyle='--',alpha=0.5)
    ax2.plot(df['install_date'], df['r7/r1 rolling7'], label='r7/r1 rolling7', linestyle='--')
    ax2.set_ylabel('r7/r1')

    # Set x-axis to display dates with a 7-day interval
    ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    plt.xticks(df['install_date'][::14], rotation=45)

    # Add legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')


    # Save the plot as a jpg image
    plt.savefig(f'/src/data/zk2/r1r7_{fromDayStr}_{toDayStr}.jpg', bbox_inches='tight')
    print('save to /src/data/zk2/r1r7_%s_%s.jpg'%(fromDayStr,toDayStr))
    plt.close()

if __name__ == '__main__':
    # df = getData1()
    # df.to_csv(getFilename('20230919_1'),index=False)

    # df = getData1New()
    # df.to_csv(getFilename('20230919_1_new'),index=False)

    # df = getData2()
    # df.to_csv(getFilename('20230919_2'),index=False)
    
    # df = getData2New()
    # df.to_csv(getFilename('20230919_2_new'),index=False)

    # df = getData3New()
    # df.to_csv(getFilename('20230919_3_new'),index=False)

    # getUserCount().to_csv(getFilename('20230919_user_count'),index=False)
    # getPayUserCount().to_csv(getFilename('20230919_pay_user_count'),index=False)

    # check2()

    # geoGroup()
    # dataMerge()
    # dataMergeAll()
    # checkCorr()
    # autoCorr()
    # mergeAll()
    # geoCorr()
    # debug()

    # r1r7corr('20230810','20230915')

    # r1r7corr('20230501','20230601')
    # r1r7corr('20230601','20230701')
    # r1r7corr('20230701','20230801')
    # r1r7corr('20230801','20230901')

    # r1r7corr('20230101','20230501')
    # r1r7corr('20230501','20230901')

    merge3()