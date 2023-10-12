# 尝试对数据进行进一步分析，主要分析各种数据与7日回收的关系
# 目前想到的有：
# 1、1日、2日、3日收入与7日回收的关系
# 2、用户数、付费用户数与7日回收的关系
# 3、付费分布，即将用户付费金额分档位，比如8个档位，然后分别看看这些档位的首日付费金额与7日回收的关系
# 4、用户24小时内付费与7日回收的关系，怀疑首日（自然日付费）有一些偶然性，这个可以参考2日的相关系数
# 5、国家分布，这个暂时没想好，因为单独看到除了美国，其他国家的相关性都不高

# 另外就是可以尝试分开对不同档位的首日与7日回收金额进行相关性测试

# 这样就需要将所有用户的付费数据都下载下来，慢慢处理
# 需要所有tiktok用户数据，这里可以暂时不要7日内完全没有付费的用户
# 列：uid,install_date,r1usd,r2usd,r3usd,r7usd
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

pd.set_option('display.max_rows', None)

def getData1():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid
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
                    ) < 2 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_2d,
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
            install_date
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

# getData1().to_csv('/src/data/zk2/20230919_analyze1.csv',index=False)

def mind1():
    df = pd.read_csv('/src/data/zk2/20230919_analyze1.csv',dtype={'install_date':str})
    df1 = df.loc[df['install_date'] < '20230501']
    df2 = df.loc[df['install_date'] >= '20230501']
    print('2023年5月之前 相关系数：')
    print(df1.corr(method='spearman')['revenue_7d'])
    print('2023年5月之后 相关系数：')
    print(df2.corr(method='spearman')['revenue_7d'])

# mind1()

# 结果：2日还行，可以考虑看看24小时付费金额的相关性
# 2023年5月之前 相关系数：
# revenue_1d    0.931331
# revenue_2d    0.974410
# revenue_3d    0.985559
# revenue_7d    1.000000
# 2023年5月之后 相关系数：
# revenue_1d    0.784430
# revenue_2d    0.897765
# revenue_3d    0.915626
# revenue_7d    1.000000

def mind2():
    df = pd.read_csv('/src/data/zk2/20230919_3_sum_all.csv',dtype={'install_date':str})
    df = df[['install_date','23H_revenue_usd','23H_user_count','23H_pay_user_count','r7D_usd']]
    df1 = df.loc[df['install_date'] < '20230501']
    df2 = df.loc[df['install_date'] >= '20230501']
    print('2023年5月之前 相关系数：')
    print(df1.corr(method='spearman')['r7D_usd'])
    print('2023年5月之后 相关系数：')
    print(df2.corr(method='spearman')['r7D_usd'])

# mind2()

# 结果：和预想的差不多
# 2023年5月之前 相关系数：
# 23H_revenue_usd       0.931345
# 23H_user_count        0.875635
# 23H_pay_user_count    0.868792
# r7D_usd               1.000000
# 2023年5月之后 相关系数：
# 23H_revenue_usd       0.781766
# 23H_user_count        0.616968
# 23H_pay_user_count    0.659572
# r7D_usd               1.000000

def getData3():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid
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
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
        )
        SELECT
            game_uid,
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
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
                    ) < 2 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_2d,
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
            game_uid
        HAVING
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) > 0
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

# getData3().to_csv('/src/data/zk2/20230919_analyze3.csv',index=False)

def makeLevels1(userDf, usd='r1usd', N=32):
    # `makeLevels1`函数接受一个包含用户数据的DataFrame（`userDf`），一个表示用户收入的列名（`usd`，默认为'r1usd'），以及分组的数量（`N`，默认为8）。
    # 其中第0组特殊处理，第0组是收入等于0的用户。
    # 过滤收入大于0的用户进行后续分组，分为N-1组，每组的总收入大致相等。
    # 根据收入列（`usd`）对用户DataFrame（`userDf`）进行排序。
    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值。
    # 计算所有这些用户的总收入。
    # 计算每组的目标收入（总收入除以分组数量）。
    # 初始化当前收入（`current_usd`）和组索引（`group_index`）。
    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入。然后，将该用户的收入值存储到`levels`数组中，并将当前收入重置为0，组索引加1。当组索引达到N-1时，停止遍历。
    # 返回`levels`数组。
    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N - 1)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    levels[N-2] = 9999999.99
    return levels

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    return userDfCopy

# df 要addCv之后的结果
def cvCheck(cvMap,df,cv='cv'):
    dfCopy = df.copy(deep=True).reset_index(drop=True)
    dfCopy['count'] = 1

    dfT1 = dfCopy.loc[dfCopy['install_date'] < '20230501']
    dfT2 = dfCopy.loc[dfCopy['install_date'] >= '20230501']

    for cv1 in cvMap['cv'].values:
        print('cv',cv1)
        cvDf = dfCopy.loc[dfCopy[cv] == cv1]
        df1 = cvDf.loc[cvDf['install_date'] < '20230501']
        df2 = cvDf.loc[cvDf['install_date'] >= '20230501']
        # 计算cv1 在df1和df2中的count占比
        print('2023年5月之前 用户数占比：(%d/%d) %.2f'%(df1['count'].sum(),dfT1['count'].sum(),df1['count'].sum()/dfT1['count'].sum()))
        print('2023年5月之后 用户数占比：(%d/%d) %.2f'%(df2['count'].sum(),dfT2['count'].sum(),df2['count'].sum()/dfT2['count'].sum()))
        # 计算cv1 在df1和df2中的revenue_1d占比
        print('2023年5月之前 revenue_1d占比：(%.2f/%.2f) %.2f'%(df1['revenue_1d'].sum(),dfT1['revenue_1d'].sum(),df1['revenue_1d'].sum()/dfT1['revenue_1d'].sum()))
        print('2023年5月之后 revenue_1d占比：(%.2f/%.2f) %.2f'%(df2['revenue_1d'].sum(),dfT2['revenue_1d'].sum(),df2['revenue_1d'].sum()/dfT2['revenue_1d'].sum()))

def mind3():
    df = pd.read_csv('/src/data/zk2/20230919_analyze3.csv',dtype={'install_date':str})

    levels = makeLevels1(df,usd='revenue_1d',N=4)
    cvMap = makeCvMap(levels)
    print(cvMap)
    df = addCv(df,cvMap,usd='revenue_1d',cv='cv')
    cvCheck(cvMap,df)

    df.drop(columns=['game_uid'],inplace=True)
    groupDf = df.groupby(['install_date','cv']).sum().reset_index()
    for cv in cvMap['cv'].values:
        print('cv',cv)
        cvDf = groupDf.loc[groupDf['cv'] == cv]
        df1 = cvDf.loc[cvDf['install_date'] < '20230501']
        df2 = cvDf.loc[cvDf['install_date'] >= '20230501']
        print('2023年5月之前 相关系数：')
        print(df1.corr(method='spearman')['revenue_7d'])
        print('2023年5月之后 相关系数：')
        print(df2.corr(method='spearman')['revenue_7d'])
    
mind3()

# 结论：低付费金额的相关性差距较大，可见高付费用户的表现比较稳定，这里是超过8美元的用户
#    cv  min_event_revenue  max_event_revenue        avg
# 0   0          -1.000000           0.000000   0.000000
# 1   1           0.000000           8.892012   4.446006
# 2   2           8.892012          35.937333  22.414673
# cv 0
# 2023年5月之前 相关系数：
# cv                 NaN
# revenue_1d         NaN
# revenue_2d    0.937813
# revenue_3d    0.971877
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64
# 2023年5月之后 相关系数：
# cv                 NaN
# revenue_1d         NaN
# revenue_2d    0.563361
# revenue_3d    0.637775
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64
# cv 1
# 2023年5月之前 相关系数：
# cv                 NaN
# revenue_1d    0.849849
# revenue_2d    0.894310
# revenue_3d    0.918612
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64
# 2023年5月之后 相关系数：
# cv                 NaN
# revenue_1d    0.357568
# revenue_2d    0.759082
# revenue_3d    0.798785
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64
# cv 2
# 2023年5月之前 相关系数：
# cv                 NaN
# revenue_1d    0.808174
# revenue_2d    0.928515
# revenue_3d    0.976053
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64
# 2023年5月之后 相关系数：
# cv                 NaN
# revenue_1d    0.771558
# revenue_2d    0.888303
# revenue_3d    0.936703
# revenue_7d    1.000000
# Name: revenue_7d, dtype: float64


def getData4():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid
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
                    WHEN event_timestamp - install_timestamp < 86400 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_24h,
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
            install_date
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

# getData4().to_csv('/src/data/zk2/20230919_analyze4.csv',index=False)

def mind4():
    df = pd.read_csv('/src/data/zk2/20230919_analyze4.csv',dtype={'install_date':str})
    df1 = df.loc[df['install_date'] < '20230501']
    df2 = df.loc[df['install_date'] >= '20230501']
    print('2023年5月之前 相关系数：')
    # print(df1.corr()['revenue_7d'])
    print(df1.corr())
    print('2023年5月之后 相关系数：')
    # print(df2.corr()['revenue_7d'])
    print(df2.corr())

# mind4()

# 结论：24小时的表现比1日的表现好一些，但是暂时还没有后续想法
# 2023年5月之前 相关系数：
#              revenue_1d  revenue_24h  revenue_7d
# revenue_1d     1.000000     0.970372    0.931331
# revenue_24h    0.970372     1.000000    0.974220
# revenue_7d     0.931331     0.974220    1.000000
# 2023年5月之后 相关系数：
#              revenue_1d  revenue_24h  revenue_7d
# revenue_1d     1.000000     0.955517    0.784430
# revenue_24h    0.955517     1.000000    0.851688
# revenue_7d     0.784430     0.851688    1.000000

def mind5():
    df = pd.read_csv('/src/data/zk2/20230919_analyze3.csv',dtype={'install_date':str})

    # levels = makeLevels1(df,usd='revenue_1d',N=4)
    df.drop(columns=['game_uid'],inplace=True)

    for i in range(1,10,2):
        levels = [i,9999999.99]

        cvMap = makeCvMap(levels)
        print(cvMap)
        df = addCv(df,cvMap,usd='revenue_1d',cv='cv')
        cvCheck(cvMap,df)

        
        groupDf = df.groupby(['install_date','cv']).sum().reset_index()
        for cv in cvMap['cv'].values:
            print('cv',cv)
            cvDf = groupDf.loc[groupDf['cv'] == cv]
            df1 = cvDf.loc[cvDf['install_date'] < '20230501']
            df2 = cvDf.loc[cvDf['install_date'] >= '20230501']
            print('2023年5月之前 相关系数：')
            print(df1.corr()['revenue_7d'])
            print('2023年5月之后 相关系数：')
            print(df2.corr()['revenue_7d'])

# mind5()

def mind3_1():
    df = pd.read_csv('/src/data/zk2/20230919_analyze3.csv',dtype={'install_date':str})

    levels = makeLevels1(df,usd='revenue_1d',N=3)
    cvMap = makeCvMap(levels)
    print(cvMap)
    df = addCv(df,cvMap,usd='revenue_1d',cv='cv')
    df['count'] = 1

    tList = [
        ['20230101','20230201'],
        ['20230201','20230301'],
        ['20230301','20230401'],
        ['20230401','20230501'],
        ['20230501','20230601'],
        ['20230601','20230701'],
        ['20230701','20230801'],
        ['20230801','20230901']
    ]

    userCountList = []
    r1dSumList = []

    for t in tList:
        t0Df = df.loc[(df['install_date'] >= t[0]) & (df['install_date'] < t[1])]
        userCount = t0Df['count'].sum()
        r1dSum = t0Df['revenue_1d'].sum()
        userCountList.append(userCount)
        r1dSumList.append(r1dSum)

    df.drop(columns=['game_uid'],inplace=True)
    groupDf = df.groupby(['install_date','cv']).sum().reset_index()
    for cv in cvMap['cv'].values:
        if cv == 0:
            continue
        print('cv',cv)
        cvDf = groupDf.loc[groupDf['cv'] == cv]
        
        for t in tList:
            print(t[0],'~',t[1])
            tDf = cvDf.loc[(cvDf['install_date'] >= t[0]) & (cvDf['install_date'] < t[1])]
            print('用户数占比：(%d/%d) %.2f'%(tDf['count'].sum(),userCountList[tList.index(t)],tDf['count'].sum()/userCountList[tList.index(t)]))
            print('revenue_1d占比：(%.2f/%.2f) %.2f'%(tDf['revenue_1d'].sum(),r1dSumList[tList.index(t)],tDf['revenue_1d'].sum()/r1dSumList[tList.index(t)]))
            print('相关系数：%.2f%%'%(tDf.corr(method='spearman')['revenue_7d']['revenue_1d'] * 100))
            print('')

# mind3_1()