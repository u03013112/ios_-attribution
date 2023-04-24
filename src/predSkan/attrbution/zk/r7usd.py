# 判断不同媒体，不同平台的用户，在首日付费金额相似的情况下，7日总付费金额是否相似。
# 由于需要分媒体，此方案暂时只能由安卓来进行判断。
# 判断付费总金额是否相似需要一些数学指标。具体需要哪些指标？这些指标如何判断相似？
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

# 获取用户数据
# 从AF事件表中获得用户数据
# 海外安卓
# 安装日期从2023-01-01~2023-04-01
# 获得用户的24小时内付费金额记作r1usd
# 获得用户48小时内付费金额记作r2usd
# 获得用户72小时内付费金额记作r3usd
# 获得用户的168小时内付费金额记作r7usd
def getDataFromMC():
    sql = '''
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
            ) as r2usd,
            sum(
                    case
                            when event_timestamp - install_timestamp <= 3 * 24 * 3600 then cast (event_revenue_usd as double)
                            else 0
                    end
            ) as r3usd,
            sum(
                    case
                            when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                            else 0
                    end
            ) as r7usd,
            media_source as media
    from
            ods_platform_appsflyer_events
    where
            app_id = 'com.topwar.gp'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20230101
            and day <= 20230410
            and install_time >= '2023-01-01'
            and install_time < '2023-04-01'
    group by
            install_date,
            customer_user_id,
            media_source
    '''

    df = execSql(sql)
    df.to_csv(getFilename('android_20230101_20230401'),index=False)
    return df

def loadData():
    df = pd.read_csv(getFilename('android_20230101_20230401'))
    # customer_user_id 改名为uid
    df.rename(columns={'customer_user_id':'uid'}, inplace=True)
    # 只保留media在'googleadwords_int','bytedanceglobal_int','Facebook Ads','snapchat_int'中的行
    df = df[df['media'].isin(['googleadwords_int','bytedanceglobal_int','Facebook Ads','snapchat_int'])]
    return df

# 这里我们假设需要将用户的r1usd分为31个档位（付费用户分档1~31，非付费用户档位是0）
# 这里要将待分档字段作为输入字段，之后需要针对r2usd或者r3usd进行分档
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

def cvMapFixAvg1(userDf,cvMapDf,usd='r1usd'):
    min = cvMapDf['min_event_revenue'][1]
    max = cvMapDf['max_event_revenue'][1]
    cv1UserDf = userDf[(userDf[usd]>min) & (userDf[usd]<=max)]
    cvMapDf.at[1, 'avg'] = cv1UserDf[usd].mean()
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv',usdp='r1usdp'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf[cv].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        avg = cvMapDf['avg'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),usdp
        ] = avg
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    userDfCopy.loc[userDfCopy[usd]>max,usdp] = avg

    return userDfCopy[['uid',cv,usdp]]

def r1usd():
    # 针对r1usd进行分档，目前常见的分档是63或者31。
    # 但是为了更加容易的让人可以进行观察，可以尝试分4或者8档。
    # 然后针对r1usd的分档，计算每个档位用的对应的r7usd的如下指标
    # 均值（Mean）
    # 中位数（Median）
    # 标准差（Standard Deviation）
    # 相关系数（Correlation Coefficient）
    # 并判断不同媒体的r1usd相同档位下，r7usd是否相似

    # userDf = getDataFromMC()

    df = loadData()
    # 这里统一分成64档，然后再按照不每16个档位进行合并
    levels = makeLevels1(df,N=64)
    cvMapDf = makeCvMap(levels)
    cvMapDf = cvMapFixAvg1(df,cvMapDf)
    print(cvMapDf)
    tmpDf = addCv(df,cvMapDf)
    df = df.merge(tmpDf,how='left',on='uid')
    df.to_csv(getFilename('androidR1usd0'),index=False)

    df = pd.read_csv(getFilename('androidR1usd0'))

    media_grouped_data = df.groupby(['media', 'cv'])['r7usd'].agg(['mean', 'median', 'std']).reset_index()

    correlation_data = []

    for media in ('googleadwords_int','bytedanceglobal_int','Facebook Ads','snapchat_int'):
        media_data = df.loc[df['media'] == media]
        correlation = media_data[['cv', 'r7usd']].corr().iloc[0, 1]
        correlation_data.append({'media': media, 'correlation': correlation})

    correlation_df = pd.DataFrame(correlation_data)

    # 将相关系数添加到media_grouped_data中
    media_grouped_data = media_grouped_data.merge(correlation_df, on='media', how='left')

    media_grouped_data.to_csv(getFilename('androidR1usd5'), index=False)

if __name__ == '__main__':
    # getDataFromMC()    
    r1usd()
    

