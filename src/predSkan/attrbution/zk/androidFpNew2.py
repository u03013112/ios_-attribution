# 与旧的androidFpNew.py相比，做如下优化
# 时间改为2022-07~2023-03，这个时间的部分用户属性已经从数数获得过，所以这样比较省事
# 将部分其他用户属性加入到撞库中
# 暂时不考虑cv位数，但是也不要太过分
# 尝试将首日付费金额，用户等级，能量花费，付费次数，英雄升星等属性进行分档，暂定分档数为8
# 这样得到的档位数应该是8*8*8*8*8=32768
# 先看看效果，然后再缩减档位数

import time
import datetime
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score


import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk/%s.%s'%(filename,ext)

def getDataFromMC():
    sql = '''
        WITH installs AS (
            SELECT
                customer_user_id AS uid,
                to_char(
                    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                    "yyyy-mm-dd"
                ) AS install_date,
                install_timestamp,
                media_source
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20220701'
                AND '20230308'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-07-01', "yyyy-mm-dd")
                AND to_date('2023-03-01', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                customer_user_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name = 'af_purchase'
                AND zone = 0
                AND day BETWEEN '20220701'
                AND '20230308'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-07-01', "yyyy-mm-dd")
                AND to_date('2023-03-01', "yyyy-mm-dd")
        )
        SELECT
            installs.uid,
            installs.install_date,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 86400
                ),
                0
            ) AS r1usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 3 * 86400
                ),
                0
            ) AS r3usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd,
            installs.install_timestamp,
            COALESCE(
                max(purchases.event_timestamp) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 1 * 86400
                ),
                0
            ) AS last_timestamp,
            installs.media_source
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.uid,
            installs.install_date,
            installs.install_timestamp,
            installs.media_source
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('androidFp04'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp04'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 uid 改名 appsflyer_id
    # df = df.rename(columns={'uid':'appsflyer_id'})

    return df

def mergeData():
    # 将mc数据与数数数据合并
    mcData = loadData()
    # 数数数据
    ssData = pd.read_csv('/src/data/demoSsAllMakeLabel.csv')
    ssData = ssData[['uid','countUserLevelMax','ENERGY','countPayCount','countHeroStarUp']]

    # # 打印mcData所有列和类型
    # print(mcData.dtypes)
    # # 打印ssData所有列和类型
    # print(ssData.dtypes)

    # 将mcData的uid转成str
    mcData['uid'] = mcData['uid'].astype(str)
    ssData['uid'] = ssData['uid'].astype(str)
    
    # merge两个Df，用uid作为key，取两个Df的交集
    mergeData = mcData.merge(ssData,on='uid',how='inner')
    mergeData.to_csv(getFilename('androidFpMergeData'), index=False)


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
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    return userDfCopy

def dataStep1():
    df = pd.read_csv(getFilename('androidFpMergeData'))
    # r1usd
    levels = makeLevels1(df,usd='r1usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r1usd',cv='cv r1usd')
    # countUserLevelMax
    levels = makeLevels1(df,usd='countUserLevelMax',N=8)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='countUserLevelMax',cv='cv countUserLevelMax')
    # ENERGY
    levels = makeLevels1(df,usd='ENERGY',N=8)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='ENERGY',cv='cv ENERGY')
    # countPayCount
    levels = makeLevels1(df,usd='countPayCount',N=8)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='countPayCount',cv='cv countPayCount')
    # countHeroStarUp
    levels = makeLevels1(df,usd='countHeroStarUp',N=8)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='countHeroStarUp',cv='cv countHeroStarUp')

    df.to_csv(getFilename('androidFpMergeDataStep1'), index=False)
    

def dataStep2():
    df = pd.read_csv(getFilename('androidFpMergeDataStep1'))
    # 目前有列'cv r1usd','cv countUserLevelMax','cv ENERGY','cv countPayCount','cv countHeroStarUp'
    # 其中'cv r1usd'的范围是0~31，其他的范围是0~7
    # 希望将这5列合成1列，最终列名cv
    # cv = 'cv countHeroStarUp' * 1 + 'cv countPayCount' * 8 + 'cv ENERGY' * 64 + 'cv countUserLevelMax' * 512 + 'cv r1usd' * 4096
    
    # df['cv'] = df['cv countHeroStarUp'] + df['cv countPayCount'] * 8 + df['cv ENERGY'] * 64 + df['cv countUserLevelMax'] * 512 + df['cv r1usd'] * 4096

    # df['cv'] = df['cv countUserLevelMax'] + df['cv ENERGY'] * 8 + df['cv r1usd'] * 64

    df['cv'] = df['cv countUserLevelMax'] + df['cv r1usd'] * 8

    # df['cv'] = df['cv r1usd']


    df.to_csv(getFilename('androidFpMergeDataStep2'), index=False)


def dataStep3():
    df = pd.read_csv(getFilename('androidFpMergeDataStep2'))
    df = df [[
        'uid',
        'install_date',
        'r1usd',
        'r3usd',
        'r7usd',
        'install_timestamp',
        'last_timestamp',
        'media',
        'cv'
    ]]
    df.to_csv(getFilename('androidFpMergeDataStep3'), index=False)

# 暂时就只关心这4个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int'
]

# 制作一个模拟的SKAN报告
def makeSKAN():
    df = pd.read_csv(getFilename('androidFpMergeDataStep3'))
    # 过滤，只要媒体属于mediaList的条目
    df = df.loc[df['media'].isin(mediaList)]
    # 重排索引
    df = df.reset_index(drop=True)

    cvDf = df

    # 添加postback_timestamp
    # 如果用户的r1usd == 0，postback_timestamp = install_timestamp + 24小时 + 0~24小时之间随机时间
    # 如果用户的r1usd > 0，postback_timestamp = last_timestamp + 24小时 + 0~24小时之间随机时间
    # 添加postback_timestamp
    zero_r1usd_mask = cvDf['r1usd'] == 0
    non_zero_r1usd_mask = cvDf['r1usd'] > 0

    cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=zero_r1usd_mask.sum())
    cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'last_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=non_zero_r1usd_mask.sum())

    # print(cvDf.head(30))

    
    skanDf = cvDf[['postback_timestamp','media','cv']]

    # postback_timestamp 转成 int
    # cv转成 int
    skanDf['postback_timestamp'] = skanDf['postback_timestamp'].astype(int)
    skanDf['cv'] = skanDf['cv'].astype(int)

    skanDf.to_csv(getFilename('skan2'), index=False)
    return skanDf

# 计算合法的激活时间范围
def skanAddValidInstallDate(skanDf):
    # 计算skan报告中的用户有效激活时间范围
    # 具体方式如下
    # cv = 0 的用户，有效时间范围是 (postback时间 - 48小时) ~ (postback时间 - 24小时)
    # cv > 0 的用户，有效时间范围是 (postback时间 - 72小时) ~ (postback时间 - 24小时)
    # 将每个用户的有效范围记录到skanDf中，记作min_valid_install_timestamp和max_valid_install_timestamp
    # 为了方便查看，请将postback时间戳和min_valid_install_timestamp和max_valid_install_timestamp都转换为日期格式也记录到skanDf中
    # 命名为postback_date，min_valid_install_date，max_valid_install_date
    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 48*3600).astype(int)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 72*3600).astype(int)
    skanDf.loc[:, 'max_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 24*3600).astype(int)

    # 将时间戳转换为日期格式
    skanDf['postback_date'] = pd.to_datetime(skanDf['postback_timestamp'], unit='s')
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')
    return skanDf

# 为了debug，匹配范围精确到天,逻辑修改
# 添加一列‘install_date’，记录用户的激活日期，从install_timestamp转换而来
# 然后添加min_valid_install_timestamp和max_valid_install_timestamp，分别为install_date的0：00：00和23：59：59的时间戳
# 为了方便查看，请将postback时间戳和min_valid_install_timestamp和max_valid_install_timestamp都转换为日期格式也记录到skanDf中
# 命名为postback_date，min_valid_install_date，max_valid_install_date
def skanAddValidInstallDate2(skanDf):
    # 将install_timestamp转换为日期格式
    skanDf['install_date'] = pd.to_datetime(skanDf['install_timestamp'], unit='s').dt.date
    
    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['install_date']).astype(int) / 10**9
    skanDf['max_valid_install_timestamp'] = (pd.to_datetime(skanDf['install_date']) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).astype(int) / 10**9
    
    # 将postback_timestamp，min_valid_install_timestamp和max_valid_install_timestamp转换为日期格式
    skanDf['postback_date'] = pd.to_datetime(skanDf['postback_timestamp'], unit='s').dt.date
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s').dt.date
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s').dt.date
    
    return skanDf

# 激活时间范围 进行近似
# 近似成分钟
def skanValidInstallDate2Min(skanDf,N = 60):
    # skanDf 中列 min_valid_install_timestamp 和 max_valid_install_timestamp 按分钟取整
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // N * N
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // N * N
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')
    return skanDf

# skan数据分组，按照media,cv,min_valid_install_timestamp和max_valid_install_timestamp分组，统计每组的用户数
def skanGroupby(skanDf):
    skanDf['user_count'] = 1

    
    skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
        {'user_count': 'sum'}
    ).reset_index()

    return skanGroupbyDf

# 制作待归因用户Df
def makeUserDf():
    userDf = pd.read_csv(getFilename('androidFpMergeDataStep3'))

    userDf = userDf[['uid','install_timestamp','r1usd','r3usd','r7usd','cv']]
    userDf['cv'] = userDf['cv'].astype(int)
    return userDf

def userInstallDate2Min(userDf,N = 60):
    userDf['install_timestamp'] = userDf['install_timestamp'] // N * N
    return userDf

# user数据分组，按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
def userGroupby(userDf):
    # 按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
    # 将分组结果保存到userGroupbyDf中
    # userGroupbyDf的列名为install_timestamp,cv,user_count和r7usd
    # user_count是每组的用户数
    # r7usd是每组的r7usd汇总
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'uid':'count','r1usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'uid':'user_count'}, inplace=True)
    return userGroupbyDf

from tqdm import tqdm
def meanAttribution(userDf, skanDf):
    for media in mediaList:
        userDf['%s count'%(media)] = 0

    unmatched_rows = 0
    unmatched_user_count = 0

    # 使用tqdm包装skanDf.iterrows()以显示进度条
    for index, row in tqdm(skanDf.iterrows(), total=len(skanDf)):
        media = row['media']
        cv = row['cv']
        min_valid_install_timestamp = row['min_valid_install_timestamp']
        max_valid_install_timestamp = row['max_valid_install_timestamp']

        condition = (
            (userDf['cv'] == cv) &
            (userDf['install_timestamp'] >= min_valid_install_timestamp) &
            (userDf['install_timestamp'] <= max_valid_install_timestamp)
        )
        matching_rows = userDf[condition]
        num_matching_rows = len(matching_rows)

        if num_matching_rows > 0:
            z = row['user_count']
            m = matching_rows['user_count'].sum()
            count = z / m

            userDf.loc[condition, '%s count'%(media)] += count
        else:
            print(f"Unmatched row: {row}")
            unmatched_rows += 1
            unmatched_user_count += row['user_count']

    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_user_count_ratio = unmatched_user_count / skanDf['user_count'].sum()
    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched user count ratio: {unmatched_user_count_ratio:.2%}")

    userDf.to_csv(getFilename('attribution1ReStep2'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep2','parquet'), index=False)
    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
    # for media in mediaList:
    #     print(f"Processing media: {media}")
    #     userDf[media + ' count'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x if item['media'] == media]))

    # Drop the 'attribute' column
    # userDf = userDf.drop(columns=['attribute'])

    # userDf.to_csv(getFilename('attribution1ReStep6'), index=False)
    # userDf = pd.read_csv(getFilename('attribution1ReStep6'))
    # print("Results saved to file attribution1ReStep6")
    # 原本的列：install_timestamp,cv,user_count,r7usd,googleadwords_int count,Facebook Ads count,bytedanceglobal_int count,snapchat_int count
    # 最终生成列：install_date,media,r7usdp
    # 中间过程：
    # install_date 是 install_timestamp（unix秒） 转换而来，精确到天
    # 将原本的 r7usd / user_count * media count 生成 media r7usd
    # 再将media r7usd 按照 media 和 install_date 分组，求和，生成 r7usdp，media 单拆出一列
    # Convert 'install_timestamp' to 'install_date'
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date

    # Calculate media r7usd
    for media in mediaList:
        media_count_col = media + ' count'
        userDf[media + ' r1usd'] = userDf['r1usd'] * userDf[media_count_col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]
        userDf[media + ' user_count'] = userDf['user_count'] * userDf[media_count_col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDf_r1usd = userDf[['install_date'] + [media + ' r1usd' for media in mediaList]]
    userDf_r7usd = userDf[['install_date'] + [media + ' r7usd' for media in mediaList]]

    # 对两个子数据框分别进行melt操作
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='media', value_name='r1usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r1usd', '')
    userDf_r1usd = userDf_r1usd.groupby(['install_date', 'media']).sum().reset_index()
    userDf_r1usd.to_csv(getFilename('userDf_r1usd'), index=False )
    print(userDf_r1usd.head())
    
    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf_r7usd['media'] = userDf_r7usd['media'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'media']).sum().reset_index()
    userDf_r7usd.to_csv(getFilename('userDf_r7usd'), index=False )
    print(userDf_r7usd.head())

    # 还需要统计每个媒体的首日用户数
    userDf_count = userDf[['install_date'] + [media + ' user_count' for media in mediaList]]
    userDf_count = userDf_count.melt(id_vars=['install_date'], var_name='media', value_name='count')
    userDf_count['media'] = userDf_count['media'].str.replace(' user_count', '')
    userDf_count = userDf_count.groupby(['install_date', 'media']).sum().reset_index()
    userDf_count.to_csv(getFilename('userDf_count'), index=False )
    print(userDf_count.head())
    # ，和付费用户数
    userDf_payCount = userDf.loc[userDf['r1usd'] >0,['install_date'] + [media + ' user_count' for media in mediaList]]
    userDf_payCount = userDf_payCount.melt(id_vars=['install_date'], var_name='media', value_name='payCount')
    userDf_payCount['media'] = userDf_payCount['media'].str.replace(' user_count', '')
    userDf_payCount = userDf_payCount.groupby(['install_date', 'media']).sum().reset_index()
    userDf_payCount.to_csv(getFilename('userDf_payCount'), index=False )
    print(userDf_payCount.head())

    # 将两个子数据框连接在一起
    userDf = userDf_r1usd.merge(userDf_r7usd, on=['install_date', 'media'])
    print('merge1')
    userDf = userDf.merge(userDf_count, on=['install_date', 'media'])
    print('merge2')
    userDf = userDf.merge(userDf_payCount, on=['install_date', 'media'])
    print('merge3')

    # Save to CSV
    userDf.to_csv(getFilename('attribution1Ret'), index=False)
    return userDf

from sklearn.metrics import r2_score
def checkRet(retDf):
    # 读取原始数据
    rawDf = loadData()
    # 只保留mediaList的用户
    rawDf = rawDf[rawDf['media'].isin(mediaList)]
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['media', 'install_date']).agg({'r7usd': 'sum','user_count':'sum'}).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date'], how='left',suffixes=('', 'p'))
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf.to_csv(getFilename('attribution1RetCheck'), index=False)
    # 计算整体的MAPE和R2
    MAPE = rawDf['MAPE'].mean()
    # R2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    # print('R2:', R2)
    # 分媒体计算MAPE和R2
    for media in mediaList:
        mediaDf = rawDf[rawDf['media'] == media]
        MAPE = mediaDf['MAPE'].mean()
        # R2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdp'])
        # print(f"Media: {media}, MAPE: {MAPE}, R2: {R2}")
        print(f"Media: {media}, MAPE: {MAPE}")


def debug():
    df = pd.read_csv(getFilename('attribution1RetCheck'))
    r7PR1 = df['r7usd'] / df['r1usd']
    print(r7PR1.mean())
    r7pPR1 = df['r7usdp'].mean() / df['r1usd'].mean()
    print(r7pPR1)


    # facebookDf = df[df['media'] == 'Facebook Ads']

    # r7PR1 = facebookDf['r7usd'] / facebookDf['r1usd']
    # print(r7PR1.mean())
    # r7pPR1 = facebookDf['r7usdp'] / facebookDf['r1usd']
    # print(r7pPR1.mean())

    # facebookDf['r7usdp2'] = facebookDf['r7usd'] * (r7PR1/r7pPR1)

    # facebookDf['MAPE2'] = abs(facebookDf['r7usd'] - facebookDf['r7usdp2']) / facebookDf['r7usd']

    # print(facebookDf['MAPE2'].mean())


if __name__ == '__main__':
    # getDataFromMC()
    # mergeData()
    # dataStep1()
    # dataStep2()
    # dataStep3()

    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)
    # # skanDf = skanAddValidInstallDate2(skanDf)

    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS5'),index=False)
    # skanDf = pd.read_csv(getFilename('skanAOS5'))
    # skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS5G'),index=False)
    # print('skan data group len:',len(skanDf))

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # userDf.to_csv(getFilename('userAOS5'),index=False)
    # userDf = pd.read_csv(getFilename('userAOS5'))
    # userDf = userInstallDate2Min(userDf,N = 600)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS5G'),index=False)
    # print('user data group len:',len(userDf))

    # userDf = pd.read_csv(getFilename('userAOS5G'))
    # skanDf = pd.read_csv(getFilename('skanAOS5G'))
    

    # skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    # userDf = meanAttribution(userDf, skanDf)
    # # userDf = pd.read_parquet(getFilename('attribution1ReStep2','parquet'))
    # userDf = meanAttributionResult(userDf)
    # # userDf = meanAttributionResult(None)

    # # meanAttributionResultDebug(userDf)

    # userDf = pd.read_csv(getFilename('attribution1Ret'))
    # checkRet(userDf)
    # # # checkRetDebug(pd.read_csv(getFilename('attribution1RetDebug')))

    # userDf = attribution1(userDf,skanDf)
    # userDf.to_csv(getFilename('attribution1Ret'),index=False)
    # userDf = pd.read_csv(getFilename('attribution1Ret'))
    # result1(userDf,'2022')

    # debugResult1()
    
    debug()
   
