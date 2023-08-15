# 算1月，3月，6月的48小时cv

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.predSkan.tools.ai import purgeRetCsv

def getDataFromMC(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r1usd,
            sum(
                case
                    when event_timestamp - install_timestamp <= 2 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r2usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= {sinceDayStr}
            and day <= {untilDayStr}
        group by
            install_date,
            customer_user_id
    '''

    print(sql)
    df = execSql(sql)
    return df

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
    target_usd = total_usd / (N)

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
    for index, row in cvMapDf.iterrows():
        cv1 = row['cv']
        avg = row['avg']

        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),'avg'
        ] = avg

    print(row)
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    userDfCopy.loc[userDfCopy[usd]>max,'avg'] = row['max_event_revenue']
    return userDfCopy

def check(userDf, cvMapDf,usd='r1usd'):
    df = addCv(userDf, cvMapDf)
    dfGroup = df.groupby('cv').sum().reset_index()
    dfGroup['mape'] = abs(dfGroup['avg'] - dfGroup[usd]) / dfGroup[usd]
    mape = dfGroup['mape'].mean()
    return mape

# 获取最近1个月的48小时CvMap，并计算mape
def main1():
    userDf = pd.read_csv('/src/data/zk2/iosr2Usd20230101_20230808.csv')
    # 截取1个月数据，用install_date过滤，2023-07-01到2023-07-31
    userDf = userDf[(userDf['install_date'] >= '2023-07-01')
        & (userDf['install_date'] <= '2023-07-31')                
    ]
    levels = makeLevels1(userDf,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    cvMapDf.to_csv('/src/data/zk2/iosr2UsdMap1.csv', index=False)
    mape = check(userDf, cvMapDf,usd='r2usd')
    print(mape)

if __name__ == '__main__':
    # userDf = getDataFromMC()
    # userDf.to_csv('/src/data/zk2/iosr2Usd20230101_20230808.csv', index=False)

    main1()