import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)


# 观察时间范围：20231006~20231010，简称A组
# 对比组时间范围：20231001~20231005，简称B组

# 获得A、B两组的国家分布
# 包括国家用户数、国家付费金额
def getData1():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                country_code,
                game_uid
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp BETWEEN UNIX_TIMESTAMP(datetime '2023-10-01 00:00:00') AND UNIX_TIMESTAMP(datetime '2023-10-10 23:59:59')
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
                AND day >= '20231001'
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
            COUNT(DISTINCT game_uid) AS install_count,
            country_code,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_7d
        FROM
            joined_data
        GROUP BY
            install_date,
            country_code
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20231006Data1'),index=False)

# 获得所有用户国家分布，上面getData1只有付费用户
def getData1_2():
    sql = '''
        SELECT
            to_char(
                FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
                'YYYYMMDD'
            ) AS install_date,
            country_code,
            COUNT(DISTINCT game_uid) AS install_count
        FROM
            rg_bi.tmp_unique_id
        WHERE
            app = 102
            AND app_id = 'id1479198816'
            AND mediasource = 'bytedanceglobal_int'
            AND install_timestamp BETWEEN UNIX_TIMESTAMP(datetime '2023-10-01 00:00:00')
            AND UNIX_TIMESTAMP(datetime '2023-10-10 23:59:59')
        group by
            country_code,
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20231006Data1_2'),index=False)

def mind1():
    # 所有用户的国家分布
    df2 = pd.read_csv(getFilename('20231006Data1_2'))
    dfA2 = df2.loc[df2['install_date'] >= 20231006]
    dfB2 = df2.loc[df2['install_date'] < 20231006]
    dfA2 = dfA2.groupby(['country_code']).agg({'install_count':'sum'})
    dfB2 = dfB2.groupby(['country_code']).agg({'install_count':'sum'})
    dfA2 = dfA2.sort_values(by=['install_count'],ascending=False)
    dfB2 = dfB2.sort_values(by=['install_count'],ascending=False)
    # 计算A、B两组的国家安装数占比
    dfA2['install_count_rate'] = dfA2['install_count'] / dfA2['install_count'].sum()
    dfB2['install_count_rate'] = dfB2['install_count'] / dfB2['install_count'].sum()
    print('A组 国家用户数前10名：')
    print(dfA2.head(10))
    print('B组 国家用户数前10名：')
    print(dfB2.head(10))
    print('-----------------------------------')

    df = pd.read_csv(getFilename('20231006Data1'))
    dfA = df.loc[df['install_date'] >= 20231006].copy()
    dfB = df.loc[df['install_date'] < 20231006].copy()
    dfA = dfA.groupby(['country_code']).agg({'install_count':'sum','revenue_1d':'sum','revenue_3d':'sum','revenue_7d':'sum'}).reset_index()
    dfB = dfB.groupby(['country_code']).agg({'install_count':'sum','revenue_1d':'sum','revenue_3d':'sum','revenue_7d':'sum'}).reset_index()
    dfA = dfA.sort_values(by=['install_count'],ascending=False).reset_index(drop=True)
    dfB = dfB.sort_values(by=['install_count'],ascending=False).reset_index(drop=True)
    # 计算A、B两组的国家安装数占比（付费用户）
    dfA['install_count_rate'] = dfA['install_count'] / dfA['install_count'].sum()
    dfB['install_count_rate'] = dfB['install_count'] / dfB['install_count'].sum()
    print('A组 国家用户数（付费）前10名：')
    print(dfA[['country_code','install_count','install_count_rate']].head(10))
    print('B组 国家用户数（付费）前10名：')
    print(dfB[['country_code','install_count','install_count_rate']].head(10))
    
    # 计算A、B两组的国家付费率
    dfAMerge = pd.merge(dfA,dfA2,on=['country_code'],suffixes=('_pay','_all'))
    dfBMerge = pd.merge(dfB,dfB2,on=['country_code'],suffixes=('_pay','_all'))
    dfAMerge['pay_rate'] = dfAMerge['install_count_pay'] / dfAMerge['install_count_all']
    dfBMerge['pay_rate'] = dfBMerge['install_count_pay'] / dfBMerge['install_count_all']
    dfAMerge = dfAMerge.sort_values(by=['install_count_all'],ascending=False)
    dfBMerge = dfBMerge.sort_values(by=['install_count_all'],ascending=False)
    print('A组 安装数前10名的国家付费率 ：')
    print(dfAMerge[['country_code','pay_rate']].head(10))
    print('B组 安装数前10名的国家付费率 ：')
    print(dfBMerge[['country_code','pay_rate']].head(10))

    # 计算A、B两组的国家付费金额占比
    # 计算1日
    dfA['revenue_1d_rate'] = dfA['revenue_1d'] / dfA['revenue_1d'].sum()
    dfB['revenue_1d_rate'] = dfB['revenue_1d'] / dfB['revenue_1d'].sum()
    dfA = dfA.sort_values(by=['revenue_1d'],ascending=False).reset_index(drop=True)
    dfB = dfB.sort_values(by=['revenue_1d'],ascending=False).reset_index(drop=True)
    print('A组 国家付费金额（1日）前10名：')
    print(dfA[['country_code','revenue_1d','revenue_1d_rate']].head(10))
    print('B组 国家付费金额（1日）前10名：')
    print(dfB[['country_code','revenue_1d','revenue_1d_rate']].head(10))

    # 计算3日
    dfA['revenue_3d_rate'] = dfA['revenue_3d'] / dfA['revenue_3d'].sum()
    dfB['revenue_3d_rate'] = dfB['revenue_3d'] / dfB['revenue_3d'].sum()
    dfA = dfA.sort_values(by=['revenue_3d'],ascending=False).reset_index(drop=True)
    dfB = dfB.sort_values(by=['revenue_3d'],ascending=False).reset_index(drop=True)
    print('A组 国家付费金额（3日）前10名：')
    print(dfA[['country_code','revenue_3d','revenue_3d_rate']].head(10))
    print('B组 国家付费金额（3日）前10名：')
    print(dfB[['country_code','revenue_3d','revenue_3d_rate']].head(10))

    # 计算3日/1日
    dfA['revenue_3d_1d_rate'] = dfA['revenue_3d'] / dfA['revenue_1d']
    dfB['revenue_3d_1d_rate'] = dfB['revenue_3d'] / dfB['revenue_1d']
    dfA = dfA.sort_values(by=['revenue_3d'],ascending=False).reset_index(drop=True)
    dfB = dfB.sort_values(by=['revenue_3d'],ascending=False).reset_index(drop=True)
    print('A组 3日国家付费金额前10名 对应的3日/1日：')
    print(dfA[['country_code','revenue_3d_1d_rate']].head(10))
    print('B组 3日国家付费金额前10名 对应的3日/1日：')
    print(dfB[['country_code','revenue_3d_1d_rate']].head(10))

if __name__ == '__main__':
    # getData1()
    # print('Done!')
    # getData1_2()

    print(mind1())
