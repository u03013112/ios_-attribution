import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getData2File(sql,filename,v = False):
    if v:
        print(sql)
    df = execSql(sql)
    df.to_csv(filename,index=False)
    return df

# 获得10小时、14小时付费金额
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
                    WHEN 
                        HOUR(FROM_UNIXTIME(event_timestamp)) <= 10 
                    THEN 
                        event_revenue_usd
                    ELSE 0
                END
            ) AS 10hour_revenue,
            SUM(
                CASE
                    WHEN 
                        HOUR(FROM_UNIXTIME(event_timestamp)) <= 14 
                    THEN 
                        event_revenue_usd
                    ELSE 0
                END
            ) AS 14hour_revenue
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 1
        GROUP BY
            install_date
        ;
    '''
    return getData2File(sql,getFilename('tt_data1'),True)

# getData1()

# 获得3日付费金额
def getData2():
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
            SUM(event_revenue_usd) AS revenue_within_3_days
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) <= 2
        GROUP BY
            install_date
        ;
    '''
    return getData2File(sql,getFilename('tt_data2'),True)

# getData2()

# 获得10小时、14小时付费金额，分用户，为了可以计算付费分布
def getData3():
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
            game_uid,
            country_code,
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            SUM(
                CASE
                    WHEN 
                        HOUR(FROM_UNIXTIME(event_timestamp)) <= 10 
                    THEN 
                        event_revenue_usd
                    ELSE 0
                END
            ) AS 10hour_revenue,
            SUM(
                CASE
                    WHEN 
                        HOUR(FROM_UNIXTIME(event_timestamp)) <= 14 
                    THEN 
                        event_revenue_usd
                    ELSE 0
                END
            ) AS 14hour_revenue
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 1
        GROUP BY
            game_uid,
            country_code,
            install_date
        ;
    '''
    return getData2File(sql,getFilename('tt_data3'),True)

# getData3()


def makeLevels1(userDf, usd='r1usd', N=32):
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


# 获得付费分布，输入getData3的结果
# 将10小时付费金额，分档10个，然后计算每天每个档位的付费人数，以及付费人数占比
# 暂时只有10小时的付费分布，14小时需要的时候再计算
def payData():
    df = pd.read_csv(getFilename('tt_data3'))

    # 10小时付费金额，分档10个
    levels = makeLevels1(df,usd='10hour_revenue',N=10)
    cvMapDf = makeCvMap(levels)
    df10 = addCv(df,cvMapDf,usd='10hour_revenue',cv='cv10')

    df10 = df10.groupby(['install_date','cv10']).agg({
        'game_uid':'count',
        '10hour_revenue':'sum'    
    }).reset_index()
    df10 = df10.sort_values(['install_date','cv10'])

    # print(df10.head(10))
    # 计算每一天的 不同cv10 付费人数占比
    df10['pay_rate10'] = df10.groupby(['install_date'])['game_uid'].apply(lambda x: x / x.sum())
    # print(df10.head(10))
    df_pivot10 = df10.pivot(index='install_date', columns='cv10', values='pay_rate10')
    df_pivot10.fillna(0, inplace=True)
    # 重命名列名
    df_pivot10.columns = ['pay_rate_' + str(int(col)) for col in df_pivot10.columns]

    # 重置索引，使得install_date变为普通列
    df_pivot10.reset_index(inplace=True)

    return df_pivot10

# payData()

# 获得国家分布，输入getData3的结果
# 将10小时付费金额，按照国家分组，按照金额降序排列选出9个国家，剩下的国家统一到一起算作other，一共10个分组
# 计算每个分组的付费人数，以及付费人数占比
# 暂时只有10小时的付费分布，14小时需要的时候再计算
def countryData():
    df = pd.read_csv(getFilename('tt_data3'))

    df10 = df.groupby(['country_code']).agg({
        'game_uid':'count',
        '10hour_revenue':'sum'
    }).reset_index()
    # 暂时先用game_uid来计算，国家选取和最终计算分组，都先按照game_uid来计算
    df10 = df10.sort_values(['game_uid'],ascending=False)
    countryList = df10['country_code'].values.tolist()
    countryList = countryList[:9]
    df.loc[df['country_code'].isin(countryList)==False,'country_code'] = 'other'
    df10 = df.groupby(['install_date','country_code']).agg({
        'game_uid':'count',
        '10hour_revenue':'sum'
    }).reset_index()
    df10 = df10.sort_values(['install_date','country_code'])

    df10['country_rate'] = df10.groupby(['install_date'])['game_uid'].apply(lambda x: x / x.sum())
    df_pivot10 = df10.pivot(index='install_date', columns='country_code', values='country_rate')
    df_pivot10.fillna(0, inplace=True)
    # 重命名列名
    df_pivot10.columns = ['country_rate_' + str(col) for col in df_pivot10.columns]

    # 重置索引，使得install_date变为普通列
    df_pivot10.reset_index(inplace=True)

    return df_pivot10

# print(countryData())


# 获得数据，10小时的X值
# 10小时付费金额，付费分布（10个档位），国家分布（10个国家）
# 最终结果是 21 列数据
# 每一行是一天
def getData10X():
    df1 = pd.read_csv(getFilename('tt_data1'))
    df1 = df1[['install_date','10hour_revenue']]
    
    df2 = payData()
    df = df1.merge(df2,on='install_date',how='left')
    df3 = countryData()
    df = df.merge(df3,on='install_date',how='left')

    df = df.sort_values(['install_date'])

    # print(df.head(10))
    return df

# getData10X()

def getDataY():
    df = pd.read_csv(getFilename('tt_data2'))
    df = df[['install_date','revenue_within_3_days']]
    df = df.sort_values(['install_date'])
    # print(df.head(10))
    return df

# getDataY()

if __name__ == '__main__':
    x = getData10X()
    x = x[['install_date','10hour_revenue']]
    y = getDataY()
    y = y[['install_date','revenue_within_3_days']]
    
    df = x.merge(y,on='install_date',how='left')
    print(df.corr())
    
    df['r3/r1'] = df['revenue_within_3_days'] / df['10hour_revenue']
    df.fillna(0, inplace=True)
    
    # 打印df['r3/r1']的25%、50%、75%分位数
    print(df['r3/r1'].quantile([0.25,0.5,0.75]))