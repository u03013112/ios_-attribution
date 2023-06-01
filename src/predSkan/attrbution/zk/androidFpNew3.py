# 与旧的androidFp2.py相比，做如下修改
# 1、原来是用24小时数据做融合归因，这里改为48小时
# 2、尝试添加在线时长作为额外的特征
# 其他部分沿用旧逻辑

import time
import datetime
import numpy as np
import pandas as pd

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
                appsflyer_id AS uid,
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
                AND day BETWEEN '20220101'
                AND '20230408'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-01-01', "yyyy-mm-dd")
                AND to_date('2023-04-01', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                appsflyer_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name = 'af_purchase'
                AND zone = 0
                AND day BETWEEN '20220101'
                AND '20230408'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-01-01', "yyyy-mm-dd")
                AND to_date('2023-04-08', "yyyy-mm-dd")
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
                        purchases.event_timestamp <= installs.install_timestamp + 2 * 86400
                ),
                0
            ) AS r2usd,
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
    df.to_csv(getFilename('androidFp05'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp05'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 uid 改名 appsflyer_id
    df = df.rename(columns={'uid':'appsflyer_id'})

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
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    return userDfCopy
# 暂时就只关心这4个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int'
]

# 制作一个模拟的SKAN报告
def makeSKAN():
    df = loadData()
    # 过滤，只要媒体属于mediaList的条目
    df = df.loc[df['media'].isin(mediaList)]
    # 重排索引
    df = df.reset_index(drop=True)

    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    cvDf = addCv(df,cvMapDf,usd='r2usd',cv='cv')
    
    # print(cvDf.head(10))

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
    df = loadData()
    
    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    userDf = addCv(df,cvMapDf,usd='r2usd',cv='cv')

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r2usd','r3usd','r7usd','cv']]
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
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'appsflyer_id':'count','r1usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'appsflyer_id':'user_count'}, inplace=True)
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
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date

    # Calculate media r7usd
    for media in mediaList:
        media_count_col = media + ' count'
        userDf[media + ' r3usd'] = userDf['r3usd'] * userDf[media_count_col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]
        userDf[media + ' user_count'] = userDf['user_count'] * userDf[media_count_col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDf_r1usd = userDf[['install_date'] + [media + ' r3usd' for media in mediaList]]
    userDf_r7usd = userDf[['install_date'] + [media + ' r7usd' for media in mediaList]]

    # 对两个子数据框分别进行melt操作
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='media', value_name='r3usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r3usd', '')
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
    userDf_payCount = userDf.loc[userDf['r3usd'] >0,['install_date'] + [media + ' user_count' for media in mediaList]]
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
    userDf.to_csv(getFilename('attribution1Ret3'), index=False)
    return userDf


# 结论验算，从原始数据中找到媒体的每天的r7usd，然后和结果对比，计算MAPE与R2
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
    rawDf.to_csv(getFilename('attribution5RetCheck'), index=False)
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

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def mean3():
    df = pd.read_csv(getFilename('attribution5RetCheck'))
    df = df[['media','install_date','r1usd','r7usd','r7usdp','MAPE']]
    df = df.sort_values('install_date')

    for media in mediaList:
        mediaDf = df[df['media'] == media].copy()
        print(media)
        print('按天MAPE：',mediaDf['MAPE'].mean())
        print('整体MAPE：',(mediaDf['r7usd'].mean() - mediaDf['r7usdp'].mean())/mediaDf['r7usd'].mean())

        mediaDf['r7usd_3d'] = mediaDf['r7usd'].rolling(3).mean()
        mediaDf['r7usdp_3d'] = mediaDf['r7usdp'].rolling(3).mean()
        mediaDf['mape_3d'] = abs(mediaDf['r7usd_3d'] - mediaDf['r7usdp_3d']) / mediaDf['r7usd_3d']
        print('3日均线MAPE：',mediaDf['mape_3d'].mean())

        mediaDf['r7usd_7d'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp_7d'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['mape_7d'] = abs(mediaDf['r7usd_7d'] - mediaDf['r7usdp_7d']) / mediaDf['r7usd_7d']
        print('7日均线MAPE：',mediaDf['mape_7d'].mean())

        mediaDf['r7usd_3ema'] = mediaDf['r7usd'].ewm(span=3).mean()
        mediaDf['r7usdp_3ema'] = mediaDf['r7usdp'].ewm(span=3).mean()
        mediaDf['mape_3ema'] = abs(mediaDf['r7usd_3ema'] - mediaDf['r7usdp_3ema']) / mediaDf['r7usd_3ema']
        print('3日EMA MAPE：',mediaDf['mape_3ema'].mean())

        mediaDf['r7usd_7ema'] = mediaDf['r7usd'].ewm(span=7).mean()
        mediaDf['r7usdp_7ema'] = mediaDf['r7usdp'].ewm(span=7).mean()
        mediaDf['mape_7ema'] = abs(mediaDf['r7usd_7ema'] - mediaDf['r7usdp_7ema']) / mediaDf['r7usd_7ema']
        print('7日EMA MAPE：',mediaDf['mape_7ema'].mean())

        # 每一张图都要有3张小图，竖着排列，x坐标保持一致，方便竖着对比。
        # install_date是str，改为日期类型，并且作为x。图例中x不要每个点都显示，太密看不清，一个月1个点就好。
        # 第一张小图 划线 r7usd，r7usdp,r7usd_7d,r7usdp_7d,r7usd_7ema,r7usdp_7ema
        # 由于图上线太多，将r7usd，r7usdp 透明度调大，浅浅的就好，
        # 全用实线，不要虚线，不同颜色
        # 第二张小图 划线 r7usd/r1usd,r7usd_7d/r1usd_7d,r7usd_7ema/r1usd_7ema
        # 第三张小图 划线 r7usdp_7d/r1usd_7d,r7usdp_7ema/r1usd_7ema
        # 有需要的数据上面没有提到的写代码计算，比如r1usd_7d，r1usd_7ema
        # 图片保存在 /src/data/zk/{media}.jpg
        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 15), sharex=True)

        # 第一张小图
        ax1.plot(mediaDf['install_date'], mediaDf['r7usd'], label='r7usd', alpha=0.3)
        ax1.plot(mediaDf['install_date'], mediaDf['r7usdp'], label='r7usdp', alpha=0.3)
        ax1.plot(mediaDf['install_date'], mediaDf['r7usd_7d'], label='r7usd_7d')
        ax1.plot(mediaDf['install_date'], mediaDf['r7usdp_7d'], label='r7usdp_7d')
        ax1.plot(mediaDf['install_date'], mediaDf['r7usd_7ema'], label='r7usd_7ema')
        ax1.plot(mediaDf['install_date'], mediaDf['r7usdp_7ema'], label='r7usdp_7ema')
        ax1.legend()
        ax1.set_title(f"{media} - r7usd, r7usdp, r7usd_7d, r7usdp_7d, r7usd_7ema, r7usdp_7ema")

        # 计算r1usd_7d和r1usd_7ema
        mediaDf['r1usd_7d'] = mediaDf['r1usd'].rolling(7).mean()
        mediaDf['r1usd_7ema'] = mediaDf['r1usd'].ewm(span=7).mean()

        # 第二张小图
        ax2.plot(mediaDf['install_date'], mediaDf['r7usd'] / mediaDf['r1usd'], label='r7usd/r1usd',alpha=0.3)
        ax2.plot(mediaDf['install_date'], mediaDf['r7usd_7d'] / mediaDf['r1usd_7d'], label='r7usd_7d/r1usd_7d')
        ax2.plot(mediaDf['install_date'], mediaDf['r7usd_7ema'] / mediaDf['r1usd_7ema'], label='r7usd_7ema/r1usd_7ema')
        ax2.legend()
        ax2.set_title(f"{media} - r7usd/r1usd, r7usd_7d/r1usd_7d, r7usd_7ema/r1usd_7ema")
        ax2.set_ylim(0, 6.0)
        # 第三张小图
        ax3.plot(mediaDf['install_date'], mediaDf['r7usdp_7d'] / mediaDf['r1usd_7d'], label='r7usdp_7d/r1usd_7d')
        ax3.plot(mediaDf['install_date'], mediaDf['r7usdp_7ema'] / mediaDf['r1usd_7ema'], label='r7usdp_7ema/r1usd_7ema')
        ax3.legend()
        ax3.set_title(f"{media} - r7usdp_7d/r1usd_7d, r7usdp_7ema/r1usd_7ema")

        # 设置x轴日期格式和间隔
        ax3.xaxis.set_major_locator(mdates.MonthLocator())
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # 保存图片
        plt.savefig(f'/src/data/zk/debug13_{media}.jpg')
        plt.close(fig)

def debug1():
    df = pd.read_csv(getFilename('attribution5RetCheck'))
    r7usdSum = df['r7usd'].sum()
    r7usdpSum = df['r7usdp'].sum()

    print('r7usdSum/r7usdpSum:',r7usdSum/r7usdpSum)

    for media in mediaList:
        print('media:',media)
        mediaDf = df[df['media'] == media]
        r7usdSum = mediaDf['r7usd'].sum()
        r7usdpSum = mediaDf['r7usdp'].sum()
        print('r7usdSum/r7usdpSum:',r7usdSum/r7usdpSum)

if __name__ == '__main__':
    # getDataFromMC()

    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)
    
    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS5'),index=False)
    # skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS5G'),index=False)
    # print('skan data group len:',len(skanDf))

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # userDf.to_csv(getFilename('userAOS5'),index=False)
    # userDf = userInstallDate2Min(userDf,N = 600)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS5G'),index=False)
    # print('user data group len:',len(userDf))

    # # # userDf = pd.read_csv(getFilename('userAOS5G'))
    # # # skanDf = pd.read_csv(getFilename('skanAOS5G'))   

    # skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    # userDf = meanAttribution(userDf, skanDf)
    userDf = pd.read_csv(getFilename('attribution1ReStep2'))
    userDf = meanAttributionResult(userDf)

    # # # userDf = pd.read_csv(getFilename('attribution1Ret'))
    checkRet(userDf)

    # debug1()
    # mean3()
    
