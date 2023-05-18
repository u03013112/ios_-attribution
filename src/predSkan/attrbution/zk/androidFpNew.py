# 与旧的androidFp.py相比，做如下优化
# 1、将用户安装时间进行分钟取整，大幅降低数据行数（之后可能需要做出更大或者更小的取整）
# 2、规范skan表和userDf表的列名与格式
# 3、在规范格式下进行方案的代码编写
# 4、计算每个媒体的归因差异，包括MAPE 与 R2
# 5、修复之前的sql错误（bug），要包括0付费用户
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
    df.to_csv(getFilename('androidFp03'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp03'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 uid 改名 appsflyer_id
    df = df.rename(columns={'uid':'appsflyer_id'})

    return df

def getCvMap():
    # 加载CV Map
    cvMapDf = pd.read_csv('/src/afCvMap2304.csv')
    cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf

def addCv(df,cvMapDf):
    # 打印cvMapDf，每行都打印，中间不能省略
    # pd.set_option('display.max_rows', None)
    # print(cvMapDf)
    df.loc[:,'cv'] = 0
    for index, row in cvMapDf.iterrows():
        df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']),'cv'] = row['conversion_value']
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(),'cv'] = cvMapDf['conversion_value'].max()
    return df

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

    cvMapDf = getCvMap()
    cvDf = addCv(df,cvMapDf)

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

    if __debug__:
        skanDf = cvDf[['postback_timestamp','media','cv','appsflyer_id','install_timestamp']]
    else:
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

    if __debug__:
        skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
            {'appsflyer_id': lambda x: ','.join(x), 'user_count': 'sum'}
        ).reset_index()
    else:
        skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
            {'user_count': 'sum'}
        ).reset_index()

    return skanGroupbyDf

# 制作待归因用户Df
def makeUserDf():
    df = loadData()
    # 是否要处理IDFA数据？
    # 如果要处理应该怎么处理？
    # 暂时放弃处理IDFA，相信SSOT
    cvMapDf = getCvMap()
    userDf = addCv(df,cvMapDf)

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r3usd','r7usd','cv']]
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
# def attribution1(userDf,skanDf):
#     # 1. 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
#     for media in mediaList:
#         userDf[media + ' count'] = 0

#     # 2. 遍历skanDf，每行做如下处理
#     for index, row in skanDf.iterrows():        
#         media = row['media']
#         cv = row['cv']
#         min_valid_install_timestamp = row['min_valid_install_timestamp']
#         max_valid_install_timestamp = row['max_valid_install_timestamp']

#         # 在userDf中找到符合条件的行
#         condition = (
#             (userDf['cv'] == cv) &
#             (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#             (userDf['install_timestamp'] <= max_valid_install_timestamp)
#         )
#         matching_rows = userDf[condition]
#         num_matching_rows = len(matching_rows)

#         if num_matching_rows > 0:
#             userDf.loc[condition, media + ' count'] += 1 / num_matching_rows

#     # 3. 检查userDf中是否有行的所有的media count列的和大于1，如果有，统计一下有多少行，占比（行数/总行数）是多少
#     media_counts_sum = userDf[[media + ' count' for media in mediaList]].sum(axis=1)
#     invalid_rows = media_counts_sum > 1
#     num_invalid_rows = invalid_rows.sum()
#     total_rows = len(userDf)
#     invalid_ratio = num_invalid_rows / total_rows

#     print(f"Invalid rows: {num_invalid_rows}")
#     print(f"Invalid ratio: {invalid_ratio:.2%}")

#     # 4. 返回userDf
#     return userDf

# def attribution1(userDf, skanDf):
#     for media in mediaList:
#         userDf[media + ' count'] = 0

#     # 使用tqdm包装skanDf.iterrows()以显示进度条
#     for index, row in tqdm(skanDf.iterrows(), total=len(skanDf)):
#         media = row['media']
#         cv = row['cv']
#         min_valid_install_timestamp = row['min_valid_install_timestamp']
#         max_valid_install_timestamp = row['max_valid_install_timestamp']

#         condition = (
#             (userDf['cv'] == cv) &
#             (userDf['install_timestamp'] > min_valid_install_timestamp) &
#             (userDf['install_timestamp'] <= max_valid_install_timestamp)
#         )
#         matching_rows = userDf[condition]
#         num_matching_rows = len(matching_rows)

#         if num_matching_rows > 0:
#             userDf.loc[condition, media + ' count'] += 1 / num_matching_rows

#     media_counts_sum = userDf[[media + ' count' for media in mediaList]].sum(axis=1)
#     invalid_rows = media_counts_sum > 1
#     num_invalid_rows = invalid_rows.sum()
#     total_rows = len(userDf)
#     invalid_ratio = num_invalid_rows / total_rows

#     print(f"Invalid rows: {num_invalid_rows}")
#     print(f"Invalid ratio: {invalid_ratio:.2%}")

#     return userDf

def attribution1(userDf, skanDf):
    for media in mediaList:
        userDf[media + ' count'] = 0

    unmatched_count = 0  # 添加一个计数器来统计未匹配到的行数

    # 使用tqdm包装skanDf.iterrows()以显示进度条
    for index, row in tqdm(skanDf.iterrows(), total=len(skanDf)):
        media = row['media']
        cv = row['cv']
        min_valid_install_timestamp = row['min_valid_install_timestamp']
        max_valid_install_timestamp = row['max_valid_install_timestamp']

        condition = (
            (userDf['cv'] == cv) &
            (userDf['install_timestamp'] > min_valid_install_timestamp) &
            (userDf['install_timestamp'] <= max_valid_install_timestamp)
        )
        matching_rows = userDf[condition]
        num_matching_rows = len(matching_rows)

        if num_matching_rows > 0:
            userDf.loc[condition, media + ' count'] += 1 / num_matching_rows
        else:
            unmatched_count += 1  # 如果没有匹配到任何行，计数器加1

    media_counts_sum = userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    invalid_rows = media_counts_sum > 1
    num_invalid_rows = invalid_rows.sum()
    total_rows = len(userDf)
    invalid_ratio = num_invalid_rows / total_rows

    unmatched_ratio = unmatched_count / len(skanDf)  # 计算未匹配行数占总行数的比例

    print(f"Invalid rows: {num_invalid_rows}")
    print(f"Invalid ratio: {invalid_ratio:.2%}")
    print(f"Unmatched rows: {unmatched_count}")
    print(f"Unmatched ratio: {unmatched_ratio:.2%}")

    return userDf

def result1(userDf,message):
    # 原始数据
    rawDf = loadData()
    rawDf = rawDf.loc[rawDf['media'].isin(mediaList)]
    # 重排索引
    rawDf = rawDf.reset_index(drop=True)
    rawDf = rawDf.groupby(['install_date','media']).agg({'r1usd':'sum','r7usd':'sum'}).reset_index()
    
    # 归因后数据
    # 转化安装日期，精确到天
    userDf.loc[:,'install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    for media in mediaList:
        userDf.loc[:,media+' r1usd'] = userDf[media+' count'] * userDf['r1usd']
        userDf.loc[:,media+' r7usd'] = userDf[media+' count'] * userDf['r7usd']
    
    userDf = userDf.groupby(['install_date']).agg('sum').reset_index()
    
    # 将rawDf和userDf的'install_date'类型统一成str
    rawDf.loc[:,'install_date'] = rawDf['install_date'].astype(str)
    userDf.loc[:,'install_date'] = userDf['install_date'].astype(str)

    for media in mediaList:
        rawMediaDf = rawDf.loc[rawDf['media'] == media]
        userMediaDf = userDf[['install_date',media+' r1usd',media+' r7usd']]
        # userMediaDf列重命名
        userMediaDf = userMediaDf.rename(columns={media+' r1usd':'r1usd',media+' r7usd':'r7usd'})
        
        mergeDf = rawMediaDf.merge(userMediaDf,on='install_date',how='left',suffixes=('_raw','_att'))
        mergeDf.loc[:,'mape7'] = abs(mergeDf['r7usd_att'] - mergeDf['r7usd_raw']) / mergeDf['r7usd_raw']

        mape7 = mergeDf['mape7'].mean()

        print(media+' mape7: '+str(mape7))
        mergeDf.to_csv(getFilename(media+'_%s_result'%(message)),index=False)

        # 在这里根据mergeDf中的‘r1usd_att’和‘r1usd_raw’，‘r7usd_att’和‘r7usd_raw’画图
        # mergeDf的'install_date'作为x轴
        # mergeDf的'r1usd_att'和'r1usd_raw'，‘r7usd_att’和‘r7usd_raw’作为y轴
        mergeDf['install_date'] = pd.to_datetime(mergeDf['install_date'])

        # 绘制r7usd图表
        fig, ax1 = plt.subplots(figsize=(16, 6))  # 将宽度从10英寸增加到12英寸

        ax1.plot(mergeDf['install_date'], mergeDf['r7usd_att'], label='r7usd_att', color='red')
        ax1.plot(mergeDf['install_date'], mergeDf['r7usd_raw'], label='r7usd_raw', color='blue')
        ax1.set_xlabel('Install Date')
        ax1.set_ylabel('USD')
        ax1.legend(loc='upper left')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))

        # 创建双Y轴并绘制mape7
        ax2 = ax1.twinx()
        ax2.plot(mergeDf['install_date'], mergeDf['mape7'], label='mape7', color='green', linestyle='--')
        ax2.set_ylabel('MAPE')
        ax2.legend(loc='upper right')
        ax2.tick_params(axis='y', labelcolor='green')

        plt.title(f'{media} - r7usd (Raw vs Attributed) and MAPE7')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("/src/data/zk/%s_%s_result7.jpg" % (media, message))

def debugResult1(message='2022'):
    skanDf = pd.read_csv(getFilename('skanAOS3G'))
    skanDf['install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s').dt.date.astype(str)
    # skanDf进行按照media，cv，install_date分组，user_count 求和
    skanDf = skanDf.groupby(['media','cv','install_date']).agg({'user_count':'sum'}).reset_index()
    # 按照media，cv,install_date排序
    skanDf = skanDf.sort_values(by=['media','cv','install_date'])
    # skanDf的user_count计算3日均线，记作user_count3
    skanDf['user_count3'] = skanDf['user_count'].rolling(3).mean()

    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(media+'_%s_result'%(message)))
        mediaSkanDf0 = skanDf.loc[skanDf['media'] == media]

        # mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        for N in (1,):
            for M in (1,3,5,10,15,30):
                # 将mediaSkanDf过滤，只保留cv高于N的行，然后按照install_date汇总， user_count求和
                mediaSkanDf = mediaSkanDf0.loc[mediaSkanDf0['cv'] > N]
                mediaSkanDf = mediaSkanDf.groupby(['install_date']).agg({'user_count':'sum'}).reset_index()
                # print(mediaSkanDf.head())

                # 根据media和install_date merge mediaDf和mediaSkanDf
                mergeDf = mediaDf.merge(mediaSkanDf,on=['install_date'],how='left')
                # print(mergeDf.head())
                # 过滤user_count小于M的行不要，然后统计MAPE
                mergeDf = mergeDf.loc[mergeDf['user_count'] > M]
                # print(mergeDf.head())
                mape = mergeDf['mape7'].mean()
                print(media+' '+str(N)+' '+str(M)+' mape7: '+str(mape))

def meanAttribution(userDf, skanDf):
    userDf['attribute'] = [list() for _ in range(len(userDf))]
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
            # count = 1 / num_matching_rows
            attribution_item = {'media': media, 'skan index': index, 'count': count}
            userDf.loc[condition, 'attribute'] = userDf.loc[condition, 'attribute'].apply(lambda x: x + [attribution_item])
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

def meanAttributionAdv(userDf,skanDf):
    userDf['temp_index'] = userDf.index
    
    watchDog = 0
    while True:
        start_time = time.time()
        watchDog += 1
        if watchDog > 10:
            print('watchDog > 10')
            break
        print('while:', watchDog)
        userDf['count_sum'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x]))
        invalid_rows = userDf[userDf['count_sum'] > 1]
        # 打印invalid_rows行数和行数占比
        print(f"Invalid rows: {len(invalid_rows)} ({len(invalid_rows) / len(userDf):.2%})")

        if invalid_rows.empty:
            break

        for row_index, invalid_row in invalid_rows.iterrows():
            attribute_list = invalid_row['attribute']
            min_count_item = min(attribute_list, key=lambda x: x['count'])
            attribute_list = list(attribute_list)  # 将numpy.ndarray转换为Python列表
            attribute_list.remove(min_count_item)
            userDf.at[row_index, 'attribute'] = attribute_list

            skan_index = min_count_item['skan index']
            affected_rows = userDf[userDf['attribute'].apply(lambda x: any(item['skan index'] == skan_index for item in x))]
            num_affected_rows = len(affected_rows)

            if num_affected_rows > 0:
                z = skanDf.loc[skan_index,'user_count']
                m = affected_rows['user_count'].sum()
                count = z / m
                for _, affected_row in affected_rows.iterrows():
                    temp_index = affected_row['temp_index']
                    userDf.loc[userDf['temp_index'] == temp_index, 'attribute'] = userDf.loc[userDf['temp_index'] == temp_index, 'attribute'].apply(
                        lambda x: [item if item['skan index'] != skan_index else {**item, 'count': count} for item in x]
                    )

        elapsed_time = time.time() - start_time
        print(f"Elapsed time for iteration {watchDog}: {elapsed_time:.2f} seconds")

    userDf.drop(columns=['temp_index'], inplace=True)
    userDf.to_csv(getFilename('attribution1ReStep4'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep4','parquet'), index=False)
    return userDf

# 优化后的meanAttributionAdv，提高运行效率
def meanAttributionAdv2(userDf, skanDf):
    userDf['temp_index'] = userDf.index

    def update_attribute(attribute_list, skan_index, count):
        return [item if item['skan index'] != skan_index else {**item, 'count': count} for item in attribute_list]

    watchDog = 0
    while True:
        start_time = time.time()
        watchDog += 1
        if watchDog > 10:
            print('watchDog > 10')
            break
        print('while:', watchDog)
        userDf['count_sum'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x]))
        invalid_rows = userDf[userDf['count_sum'] > 1]
        print(f"Invalid rows: {len(invalid_rows)} ({len(invalid_rows) / len(userDf):.2%})")

        if invalid_rows.empty:
            break

        for loop_index, (row_index, invalid_row) in enumerate(invalid_rows.iterrows()):
            start_loop_time = time.time()

            attribute_list = invalid_row['attribute']
            min_count_item = min(attribute_list, key=lambda x: x['count'])
            attribute_list = list(attribute_list)
            attribute_list.remove(min_count_item)
            userDf.at[row_index, 'attribute'] = attribute_list

            skan_index = min_count_item['skan index']
            affected_rows = userDf[userDf['attribute'].apply(lambda x: any(item['skan index'] == skan_index for item in x))]
            num_affected_rows = len(affected_rows)

            if num_affected_rows > 0:
                z = skanDf.loc[skan_index, 'user_count']
                m = affected_rows['user_count'].sum()
                count = z / m

                userDf.loc[affected_rows.index, 'attribute'] = userDf.loc[affected_rows.index, 'attribute'].apply(
                    lambda x: update_attribute(x, skan_index, count)
                )
            elapsed_loop_time = time.time() - start_loop_time
            progress = (loop_index + 1) / len(invalid_rows)
            remaining_time = (len(invalid_rows) - (loop_index + 1)) * elapsed_loop_time
            print(f"Progress: {loop_index + 1}/{len(invalid_rows)} ({progress:.2%}), Elapsed time for loop: {elapsed_loop_time:.2f} seconds, Remaining time: {time.strftime('%H:%M:%S', time.gmtime(remaining_time))}")

        elapsed_time = time.time() - start_time
        print(f"Elapsed time for iteration {watchDog}: {elapsed_time:.2f} seconds")

        userDf.to_csv(getFilename('attribution1ReStep4'), index=False)
        userDf.to_parquet(getFilename('attribution1ReStep4', 'parquet'), index=False)
        print("Results saved to files.")

    # Save the results in each iteration
    userDf.drop(columns=['temp_index'], inplace=True)

    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
    for media in mediaList:
        print(f"Processing media: {media}")
        userDf[media + ' count'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x if item['media'] == media]))

    # Drop the 'attribute' column
    userDf = userDf.drop(columns=['attribute'])

    userDf.to_csv(getFilename('attribution1ReStep6'), index=False)
    userDf = pd.read_csv(getFilename('attribution1ReStep6'))
    print("Results saved to file attribution1ReStep6")
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
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date'], how='left')
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    rawDf.to_csv(getFilename('attribution1RetCheck'), index=False)
    # 计算整体的MAPE和R2
    MAPE = rawDf['MAPE'].mean()
    R2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    print('R2:', R2)
    # 分媒体计算MAPE和R2
    for media in mediaList:
        mediaDf = rawDf[rawDf['media'] == media]
        MAPE = mediaDf['MAPE'].mean()
        R2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdp'])
        print(f"Media: {media}, MAPE: {MAPE}, R2: {R2}")

def checkRetDebug(retDf):
    # 读取原始数据
    rawDf = loadData()
    cvMapDf = getCvMap()
    rawDf = addCv(rawDf,cvMapDf)
    # 只保留mediaList的用户
    rawDf = rawDf[rawDf['media'].isin(mediaList)]
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['media', 'install_date','cv']).agg({'r7usd': 'sum'}).reset_index()

    # rawDf 和 retDf 进行合并
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date','cv'], how='left')
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    rawDf.to_csv(getFilename('attribution1RetCheck'), index=False)

    # 分媒体计算MAPE和R2
    for media in mediaList:
        for cv in range(32):
            mediaDf = rawDf[
                (rawDf['media'] == media) &
                (rawDf['cv'] == cv)
                ]
            try:
                MAPE = mediaDf['MAPE'].mean()
            except:
                MAPE = 0
            try:
                R2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdp'])
            except:
                R2 = 0
            print(f"Media: {media},cv:{cv} MAPE: {MAPE}, R2: {R2}")

def cv_group(cv):
    if 0 <= cv < 10:
        return "0-10"
    elif 11 <= cv < 20:
        return "11-20"
    elif 21 <= cv < 32:
        return "21-32"
    
def debug():
    userDf2 = pd.read_csv(getFilename('userAOS3G'))
    userDf2 = userDf2[['install_timestamp','cv','r3usd']]

    userDf = pd.read_parquet(getFilename('attribution1ReStep2','parquet'))
    userDf = userDf.merge(userDf2,on=['install_timestamp','cv'],how='left')

    userDf.to_parquet(getFilename('attribution1ReStep2R3usd','parquet'), index=False)
    userDf.to_csv(getFilename('attribution1ReStep2R3usd'), index=False)

def debug2():
    rawDf = loadData()
    cvMapDf = getCvMap()
    rawDf = addCv(rawDf, cvMapDf)
    rawDf = rawDf[['install_date', 'media', 'cv', 'r7usd']]
    rawDf['cv_group'] = rawDf['cv'].apply(cv_group)
    rawDf['install_date'] = pd.to_datetime(rawDf['install_date'])

    # 获取所有唯一的媒体和安装日期
    
    unique_install_dates = rawDf['install_date'].unique()

    result = []

    for media in mediaList:
        for install_date in unique_install_dates:
            data = rawDf[(rawDf['media'] == media) & (rawDf['install_date'] == install_date)]
            cv_distribution = data['cv_group'].value_counts(normalize=True)
            r7usd_mean = data.groupby('cv_group')['r7usd'].mean()

            result.append([media, install_date, 
                           cv_distribution.get("0-10", 0), cv_distribution.get("11-20", 0), cv_distribution.get("21-32", 0), 
                           r7usd_mean.get("0-10", 0), r7usd_mean.get("11-20", 0), r7usd_mean.get("21-32", 0)])

    result_df = pd.DataFrame(result, columns=['media', 'install_date', 
                                              'cv_dist_0-10', 'cv_dist_11-20', 'cv_dist_21-32', 
                                              'r7usd_mean_0-10', 'r7usd_mean_11-20', 'r7usd_mean_21-32'])
    
    # 分别按照不同的cv_group排序，并将结果保存到不同的CSV文件中
    for cv_groupName in ["0-10", "11-20", "21-32"]:
        sorted_df = result_df.sort_values(by=[f'cv_dist_{cv_groupName}', 'install_date', 'media'])
        sorted_df.to_csv(f'/src/data/zk/debugCvG{cv_groupName}.csv', index=False)

def debug3():
    # 读取CSV文件
    data = pd.read_csv('/src/data/zk/debugCvG11-20.csv')

    # 计算相关系数
    correlation_0_10 = data['cv_dist_0-10'].corr(data['r7usd_mean_0-10'])
    correlation_11_20 = data['cv_dist_11-20'].corr(data['r7usd_mean_11-20'])
    correlation_21_32 = data['cv_dist_21-32'].corr(data['r7usd_mean_21-32'])

    # 打印相关系数
    print("cv_dist_0-10 和 r7usd_mean_0-10 的相关系数：", correlation_0_10)
    print("cv_dist_11-20 和 r7usd_mean_11-20 的相关系数：", correlation_11_20)
    print("cv_dist_21-32 和 r7usd_mean_21-32 的相关系数：", correlation_21_32)

    correlation_cv11_20_r7usd21_32 = data['cv_dist_11-20'].corr(data['r7usd_mean_21-32'])
    correlation_cv21_32_r7usd11_20 = data['cv_dist_21-32'].corr(data['r7usd_mean_11-20'])

    print("cv_dist_11-20 和 r7usd_mean_21-32 的相关系数：", correlation_cv11_20_r7usd21_32)
    print("cv_dist_21-32 和 r7usd_mean_11-20 的相关系数：", correlation_cv21_32_r7usd11_20)

def debug4():
    rawDf = loadData()
    cvMapDf = getCvMap()
    rawDf = addCv(rawDf, cvMapDf)
    rawDf = rawDf[['install_date', 'media', 'cv', 'r7usd']]
    rawDf['cv_group'] = rawDf['cv'].apply(cv_group)
    rawDf['install_date'] = pd.to_datetime(rawDf['install_date'])

    # 过滤，过滤只保留media在mediaList中的数据
    rawDf = rawDf[rawDf['media'].isin(mediaList)]

    # 按照media和 install_date的月 和cv_group分组，计算每组的r7usd的均值
    # 计算每月每个cv_group各媒体与所有媒体的均值的偏差（MAPE）
    # 将install_date列设置为月初，以便按月分组
    rawDf['install_month'] = rawDf['install_date'].apply(lambda x: x.replace(day=1))

    # 按照media、install_month和cv_group分组，计算每组的r7usd的均值
    media_monthly_cv_group_mean = rawDf.groupby(['media', 'install_month', 'cv_group'])['r7usd'].mean().reset_index()

    # 按照install_month和cv_group分组，计算所有媒体的r7usd的均值
    all_media_monthly_cv_group_mean = rawDf.groupby(['install_month', 'cv_group'])['r7usd'].mean().reset_index()

    # 将两个数据框合并，以便计算MAPE
    merged_df = media_monthly_cv_group_mean.merge(all_media_monthly_cv_group_mean, on=['install_month', 'cv_group'], suffixes=('_media', '_all'))

    # 计算每月每个cv_group各媒体与所有媒体的均值的偏差（MAPE）
    merged_df['mape'] = abs(merged_df['r7usd_media'] - merged_df['r7usd_all']) / merged_df['r7usd_all'] * 100

    merged_df.to_csv('/src/data/zk/debug4.csv', index=False)
    return merged_df

def debug5(message='2022'):
    skanDf = pd.read_csv(getFilename('skanAOS3G'))
    skanDf['install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s').dt.date.astype(str)
    skanDf = skanDf.groupby(['media', 'cv', 'install_date']).agg({'user_count': 'sum'}).reset_index()
    skanDf = skanDf.sort_values(by=['media', 'cv', 'install_date'])
    skanDf['user_count3'] = skanDf['user_count'].rolling(3).mean()

    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(media + '_%s_result' % (message)))
        mediaSkanDf0 = skanDf.loc[skanDf['media'] == media]

        fig, ax1 = plt.subplots(figsize=(12, 6))

        # 绘制mape7曲线
        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        ax1.plot(mediaDf['install_date'], mediaDf['mape7'], label='mape7', color='red')
        ax1.set_xlabel('Install Date')
        ax1.set_ylabel('MAPE')
        ax1.legend(loc='upper left')
        ax1.tick_params(axis='y', labelcolor='red')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))

        ax2 = ax1.twinx()

        for N in (1, 3, 5, 10,):
            mediaSkanDf = mediaSkanDf0.loc[mediaSkanDf0['cv'] > N]
            mediaSkanDf = mediaSkanDf.groupby(['install_date']).agg({'user_count': 'sum'}).reset_index()
            mediaSkanDf['install_date'] = pd.to_datetime(mediaSkanDf['install_date'])

            # 绘制不同的N对应的用户数量曲线
            ax2.plot(mediaSkanDf['install_date'], mediaSkanDf['user_count'], label=f'User Count (N={N})', linestyle='--')
            ax2.set_ylabel('User Count')
            ax2.legend(loc='upper right')
            ax2.tick_params(axis='y', labelcolor='black')

        plt.title(f'{media} - MAPE7 and User Count for Different N Values')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("/src/data/zk/%s_%s_result7.jpg" % (media, 'debug5'))

def debug6(message='2022'):
    skanDf = pd.read_csv(getFilename('skanAOS3G'))
    skanDf['install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s').dt.date.astype(str)
    skanDf = skanDf.groupby(['media', 'cv', 'install_date']).agg({'user_count': 'sum'}).reset_index()
    skanDf = skanDf.sort_values(by=['media', 'cv', 'install_date'])

    # 声明过滤条件N和M
    N = 1
    M = 30

    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(media + '_%s_result' % (message)))
        mediaSkanDf0 = skanDf.loc[skanDf['media'] == media]

        # 第一步：统计cv > N的用户数，并将结果添加为新列
        mediaSkanDf0['user_count_filtered'] = mediaSkanDf0.apply(lambda row: row['user_count'] if row['cv'] > N else 0, axis=1)

        # 第二步：排除用户数小于M的数据
        mediaSkanDf_filtered = mediaSkanDf0[mediaSkanDf0['user_count_filtered'] >= M]

        # 合并mediaDf和mediaSkanDf_filtered
        mergeDf = mediaDf.merge(mediaSkanDf_filtered, on='install_date', how='inner')

        fig, ax1 = plt.subplots(figsize=(12, 6))

        # 绘制mape7曲线
        mergeDf['install_date'] = pd.to_datetime(mergeDf['install_date'])
        ax1.plot(mergeDf['install_date'], mergeDf['mape7'], label='mape7', color='red')
        ax1.set_xlabel('Install Date')
        ax1.set_ylabel('MAPE')
        ax1.legend(loc='upper left')
        ax1.tick_params(axis='y', labelcolor='red')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))

        ax2 = ax1.twinx()

        # 绘制过滤后的用户数量曲线
        ax2.plot(mergeDf['install_date'], mergeDf['user_count_filtered'], label=f'User Count (Filtered)', linestyle='--')
        ax2.set_ylabel('User Count')
        ax2.legend(loc='upper right')
        ax2.tick_params(axis='y', labelcolor='black')

        plt.title(f'{media} - MAPE7 and Filtered User Count')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("/src/data/zk/%s_%s_result7.jpg" % (media, 'debug6'))


# def attribution2Re(userDf, skanDf):
#     userDf['media'] = ''
#     userDf['skan index'] = -1
#     userDf['re att count'] = 0

#     unmatched_rows = 0
#     unmatched_rows_re = 0
#     unmatched_revenue = 0
#     unmatched_revenue_re = 0

#     def attribution_process(index, row):
#         nonlocal unmatched_rows, unmatched_rows_re, unmatched_revenue, unmatched_revenue_re

#         media = row['media']
#         cv = row['cv']
#         min_valid_install_timestamp = row['min_valid_install_timestamp']
#         max_valid_install_timestamp = row['max_valid_install_timestamp']

#         condition = (
#             (userDf['cv'] == cv) &
#             (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#             (userDf['install_timestamp'] <= max_valid_install_timestamp)
#         )
#         matching_rows = userDf[condition]

#         if not matching_rows[matching_rows['media'] == ''].empty:
#             selected_row = matching_rows[matching_rows['media'] == '']['install_timestamp'].idxmax()
#             userDf.loc[selected_row, 'media'] = media
#             userDf.loc[selected_row, 'skan index'] = index
#         else:
#             # 扩大匹配范围
#             min_valid_install_timestamp -= 24 * 60 * 60  # 减少24小时，单位为秒
#             condition_expanded = (
#                 (userDf['cv'] == cv) &
#                 (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#                 (userDf['install_timestamp'] <= max_valid_install_timestamp)
#             )
#             matching_rows_expanded = userDf[condition_expanded]

#             if not matching_rows_expanded[matching_rows_expanded['media'] == ''].empty:
#                 selected_row = matching_rows_expanded[matching_rows_expanded['media'] == '']['install_timestamp'].idxmax()
#                 userDf.loc[selected_row, 'media'] = media
#                 userDf.loc[selected_row, 'skan index'] = index
#             else:
#                 if not matching_rows.empty:
#                     min_re_att_count = matching_rows['re att count'].min()
#                     min_re_att_count_rows = matching_rows[matching_rows['re att count'] == min_re_att_count]

#                     threshold = 10
#                     if min_re_att_count > threshold:
#                         unmatched_rows_re += 1
#                         unmatched_revenue_re += row['skad_revenue']
#                         return

#                     selected_row = min_re_att_count_rows['install_timestamp'].idxmax()
#                     prev_skan_index = userDf.loc[selected_row, 'skan index']

#                     userDf.loc[selected_row, 'media'] = media
#                     userDf.loc[selected_row, 'skan index'] = index
#                     userDf.loc[selected_row, 're att count'] += 1

#                     if prev_skan_index != -1:
#                         prev_skan_row = skanDf.loc[prev_skan_index]
#                         attribution_process(prev_skan_index, prev_skan_row)
#                 else:
#                     unmatched_rows += 1
#                     unmatched_revenue += row['skad_revenue']
#                     return

#     skanDf = skanDf.sort_values(by='postback_timestamp', ascending=False)

#     for index, row in skanDf.iterrows():
#         attribution_process(index, row)

#     unmatched_ratio = unmatched_rows / len(skanDf)
#     unmatched_ratio_re = unmatched_rows_re / len(skanDf)
#     unmatched_revenue_ratio = unmatched_revenue / skanDf['skad_revenue'].sum()
#     unmatched_revenue_ratio_re = unmatched_revenue_re / skanDf['skad_revenue'].sum()

#     print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
#     print(f"Unmatched revenue ratio: {unmatched_revenue_ratio:.2%}")
#     print(f"Unmatched rows ratio re: {unmatched_ratio_re:.2%}")
#     print(f"Unmatched revenue ratio re: {unmatched_revenue_ratio_re:.2%}")

#     userDf.to_csv(getFilename('attribution2Re0509'), index=False)
#     userDf.to_parquet(getFilename('attribution2Re0509','parquet'), index=False)

#     return userDf

def attribution2Re(userDf, skanDf):
    userDf['media'] = ''
    userDf['skan index'] = -1
    userDf['re att count'] = 0

    unmatched_rows = 0
    unmatched_rows_re = 0
    unmatched_revenue = 0
    unmatched_revenue_re = 0

    def attribution_process(index, row):
        nonlocal unmatched_rows, unmatched_rows_re, unmatched_revenue, unmatched_revenue_re

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

        if not matching_rows[matching_rows['media'] == ''].empty:
            selected_row = matching_rows[matching_rows['media'] == '']['install_timestamp'].idxmax()
            userDf.loc[selected_row, 'media'] = media
            userDf.loc[selected_row, 'skan index'] = index
        else:
            # 扩大匹配范围
            min_valid_install_timestamp -= 24 * 60 * 60  # 减少24小时，单位为秒
            condition_expanded = (
                (userDf['cv'] == cv) &
                (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                (userDf['install_timestamp'] <= max_valid_install_timestamp)
            )
            matching_rows_expanded = userDf[condition_expanded]

            if not matching_rows_expanded[matching_rows_expanded['media'] == ''].empty:
                selected_row = matching_rows_expanded[matching_rows_expanded['media'] == '']['install_timestamp'].idxmax()
                userDf.loc[selected_row, 'media'] = media
                userDf.loc[selected_row, 'skan index'] = index
            else:
                if not matching_rows.empty:
                    min_re_att_count = matching_rows['re att count'].min()
                    min_re_att_count_rows = matching_rows[matching_rows['re att count'] == min_re_att_count]

                    threshold = 10
                    if min_re_att_count > threshold:
                        unmatched_rows_re += 1
                        unmatched_revenue_re += row['skad_revenue']
                        return

                    selected_row = min_re_att_count_rows['install_timestamp'].idxmax()
                    prev_skan_index = userDf.loc[selected_row, 'skan index']

                    userDf.loc[selected_row, 'media'] = media
                    userDf.loc[selected_row, 'skan index'] = index
                    userDf.loc[selected_row, 're att count'] += 1

                    if prev_skan_index != -1:
                        prev_skan_row = skanDf.loc[prev_skan_index]
                        attribution_process(prev_skan_index, prev_skan_row)
                else:
                    unmatched_rows += 1
                    unmatched_revenue += row['skad_revenue']
                    return

    skanDf = skanDf.sort_values(by='postback_timestamp', ascending=False)

    with tqdm(total=len(skanDf), desc="Processing rows", ncols=100) as pbar:
        for index, row in skanDf.iterrows():
            attribution_process(index, row)
            pbar.update(1)

    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_ratio_re = unmatched_rows_re / len(skanDf)
    unmatched_revenue_ratio = unmatched_revenue / skanDf['skad_revenue'].sum()
    unmatched_revenue_ratio_re = unmatched_revenue_re / skanDf['skad_revenue'].sum()

    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched revenue ratio: {unmatched_revenue_ratio:.2%}")
    print(f"Unmatched rows ratio re: {unmatched_ratio_re:.2%}")
    print(f"Unmatched revenue ratio re: {unmatched_revenue_ratio_re:.2%}")

    userDf.to_csv(getFilename('attribution2Re0509'), index=False)
    userDf.to_parquet(getFilename('attribution2Re0509','parquet'), index=False)

    return userDf

def main2(message='0509'):
    skanDf = pd.read_csv(getFilename('skanAOS3'))
    userDf = pd.read_csv(getFilename('userAOS3'))
    
    userDf = attribution2Re(userDf, skanDf)

    userDf = pd.read_csv(getFilename('attribution2Re0509'))
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date.astype(str)
    # 按install_date，media分组，r7usd汇总
    userDf = userDf.groupby(['install_date', 'media']).agg({'r7usd': 'sum'}).reset_index()

    rawDf = loadData()
    rawDf = rawDf.loc[rawDf['media'].isin(mediaList)]
    rawDf = rawDf.groupby(['install_date','media']).agg({'r7usd':'sum'}).reset_index()
    
    for media in mediaList:
        rawMediaDf = rawDf.loc[rawDf['media'] == media]
        userMediaDf = userDf[['install_date',media+' r7usd']]
        # userMediaDf列重命名
        userMediaDf = userMediaDf.rename(columns={media+' r7usd':'r7usd'})
        
        mergeDf = rawMediaDf.merge(userMediaDf,on='install_date',how='left',suffixes=('_raw','_att'))
        mergeDf.loc[:,'mape7'] = abs(mergeDf['r7usd_att'] - mergeDf['r7usd_raw']) / mergeDf['r7usd_raw']

        mape7 = mergeDf['mape7'].mean()

        print(media+' mape7: '+str(mape7))
        mergeDf.to_csv(getFilename(media+'_%s_result'%(message)),index=False)

        # 在这里根据mergeDf中的‘r1usd_att’和‘r1usd_raw’，‘r7usd_att’和‘r7usd_raw’画图
        # mergeDf的'install_date'作为x轴
        # mergeDf的'r1usd_att'和'r1usd_raw'，‘r7usd_att’和‘r7usd_raw’作为y轴
        mergeDf['install_date'] = pd.to_datetime(mergeDf['install_date'])

        # 绘制r7usd图表
        plt.figure(figsize=(10, 6))
        plt.plot(mergeDf['install_date'], mergeDf['r7usd_att'], label='r7usd_att', linestyle='-', marker='o')
        plt.plot(mergeDf['install_date'], mergeDf['r7usd_raw'], label='r7usd_raw', linestyle='--', marker='o')
        plt.xlabel('Install Date')
        plt.ylabel('USD')
        plt.title(f'{media} - r7usd (Raw vs Attributed)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))
        plt.tight_layout()
        plt.savefig("/src/data/zk/%s_%s_result7.jpg" % (media, message))



import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import numpy as np
from matplotlib.dates import DateFormatter

def draw():
    df = pd.read_csv(getFilename('attribution1RetCheck'))
    df['install_date'] = pd.to_datetime(df['install_date'])

    for media in mediaList:
        media_df = df[df['media'] == media]

        fig, ax1 = plt.subplots(figsize=(18, 6))

        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd')
        ax1.plot(media_df['install_date'], media_df['r7usdp'], label='r7usdp')
        ax1.set_ylabel('r7usd and r7usdp')
        ax1.set_xlabel('Install Date')

        ax2 = ax1.twinx()
        ax2.plot(media_df['install_date'], media_df['MAPE'], label='MAPE', linestyle='--', color='red')
        ax2.set_ylabel('MAPE')

        # Generate a range of months for x-axis
        month_range = pd.date_range(media_df['install_date'].min(), media_df['install_date'].max(), freq='MS')

        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(month_range, rotation=45)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

        plt.savefig(f'/src/data/zk/att1_{media}.jpg', bbox_inches='tight')
        plt.close()

def debug7():
    for media in mediaList:
        df = pd.read_csv(getFilename(media+'_%s_result'%('2022')))
        print(media)
        # 按install_date的月份分组，重新计算mape7（r7usd_att和r7usd_raw的mape）
        # month 是 install_date 字符串截取后的月份, 例如 '2021-01-01'截取之后变成'2021-01'
        df['month'] = df['install_date'].apply(lambda x: x[0:7])
        df['month'] = pd.to_datetime(df['month'])
        df.set_index('month', inplace=True)

        df = df.groupby('month').agg({'r7usd_att':'sum','r7usd_raw':'sum'})
        df['mape7'] = abs(df['r7usd_att'] - df['r7usd_raw']) / df['r7usd_raw']
        print('media: '+media)
        print('mape7: '+str(df['mape7'].mean()))

        df.to_csv(getFilename(media+'_%s_result'%('month')))
        # 画图，month作为x轴，r7usd_att和r7usd_raw作为y轴
        # 另外双Y轴，另一个Y轴是mape7
        # 图片保存到'/src/data/zk/att1_{media}_month.jpg'
        fig, ax1 = plt.subplots(figsize=(18, 6))
        ax1.plot(df.index, df['r7usd_att'], label='r7usd_att')
        ax1.plot(df.index, df['r7usd_raw'], label='r7usd_raw')
        ax1.set_ylabel('r7usd_att and r7usd_raw')
        ax1.set_xlabel('Month')
        ax2 = ax1.twinx()
        ax2.plot(df.index, df['mape7'], label='mape7', linestyle='--', color='red')
        ax2.set_ylabel('mape7')
        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
        plt.xticks(df.index[::1], rotation=45)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')
        plt.savefig(f'/src/data/zk/att1_{media}_month.jpg', bbox_inches='tight')

def debug8():
    dfTotal = pd.read_csv(getFilename('attribution1RetCheck'))
    for media in mediaList:
        # df = pd.read_csv(getFilename(media+'_%s_result'%('2022')))
        df = dfTotal[dfTotal['media'] == media].copy()
        print(media)
        # 按install_date的月份分组，重新计算mape7（r7usd_att和r7usd_raw的mape）
        # month 是 install_date 字符串截取后的月份, 例如 '2021-01-01'截取之后变成'2021-01'
        df['month'] = df['install_date'].apply(lambda x: x[0:7])
        df['month'] = pd.to_datetime(df['month'])
        df.set_index('month', inplace=True)

        df = df.groupby('month').agg({'r7usdp':'sum','r7usd':'sum'})
        df['mape7'] = abs(df['r7usdp'] - df['r7usd']) / df['r7usd']
        print('media: '+media)
        print('mape7: '+str(df['mape7'].mean()))

        df.to_csv(getFilename(media+'_%s_result'%('month')))
        # 画图，month作为x轴，r7usd_att和r7usd_raw作为y轴
        # 另外双Y轴，另一个Y轴是mape7
        # 图片保存到'/src/data/zk/att1_{media}_month.jpg'
        fig, ax1 = plt.subplots(figsize=(18, 6))
        ax1.plot(df.index, df['r7usdp'], label='r7usdp')
        ax1.plot(df.index, df['r7usd'], label='r7usd')
        ax1.set_ylabel('r7usdp and r7usd')
        ax1.set_xlabel('Month')
        ax2 = ax1.twinx()
        ax2.plot(df.index, df['mape7'], label='mape7', linestyle='--', color='red')
        ax2.set_ylabel('mape7')
        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
        plt.xticks(df.index[::1], rotation=45)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')
        plt.savefig(f'/src/data/zk/att1_{media}_month.jpg', bbox_inches='tight')

        
def afSkan():
    # # 计算af方式计算安装时间带来的首日付费金额，与原始数据的差异
    # df = loadData()
    # # 过滤，只要媒体属于mediaList的条目
    # df = df.loc[df['media'].isin(mediaList)]
    # # 重排索引
    # df = df.reset_index(drop=True)

    # cvMapDf = getCvMap()
    # cvDf = addCv(df,cvMapDf)

    # zero_r1usd_mask = cvDf['r1usd'] == 0
    # non_zero_r1usd_mask = cvDf['r1usd'] > 0

    # cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=zero_r1usd_mask.sum())
    # cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'last_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=non_zero_r1usd_mask.sum())

    # # 按照AF的计算方式计算激活时间
    # # 激活日期是基于回传接收日期推算的，具体方法如下：回传接收日期 - 36小时 - [末次互动范围平均小时数]。默认[末次互动范围平均小时数]为12小时，但如果转化值为0，则末次互动范围平均小时数也为0。
    # # 即：
    # # 非付费用户 激活日期 = 回传接收日期 - 36小时
    # # 付费用户 激活日期 = 末次互动日期 - 48小时

    # cvDf.loc[zero_r1usd_mask, 'install_timestamp_af'] = cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] - 36 * 3600
    # cvDf.loc[non_zero_r1usd_mask, 'install_timestamp_af'] = cvDf.loc[non_zero_r1usd_mask, 'last_timestamp'] - 48 * 3600

    # # 用install_timestamp_af转化成install_date_af，精确到天，并转成字符串
    # cvDf['install_date_af'] = pd.to_datetime(cvDf['install_timestamp_af'], unit='s').dt.date.astype(str)

    # cvDf.to_csv(getFilename('cvDf'))
    # print(cvDf.head(10))
    cvDf = pd.read_csv(getFilename('cvDf'))

    afDf = cvDf.groupby(['media', 'install_date_af']).agg({'r1usd': 'sum'})
    rawDf = cvDf.groupby(['media', 'install_date']).agg({'r1usd': 'sum','r7usd':'sum'})

    # 重命名afDf的索引名称以匹配rawDf
    afDf.index.names = ['media', 'install_date']

    # 使用left_index=True和right_index=True参数合并数据框
    df = rawDf.merge(afDf, left_index=True, right_index=True, how='left',suffixes=('_raw','_af'))
    print(df.corr())
    # 打印r1usd_raw和r7usd的R2
    print('r1usd_raw and r7usd R2:',r2_score(df['r1usd_raw'],df['r7usd']))
    df.to_csv(getFilename('cvDf2'))


import seaborn as sns
import matplotlib.dates as mdates

def debug9():
    df = pd.read_csv(getFilename('attribution1RetCheck'))
    df['install_date'] = pd.to_datetime(df['install_date'])
    facebookDf = df.loc[df['media'] == 'Facebook Ads']
    mape = facebookDf['MAPE'].mean()
    print('MAPE:', '{:.2%}'.format(mape))

    plt.figure(figsize=(28, 6))

    ax1 = plt.gca()  # 获取当前的Axes对象
    sns.lineplot(x='install_date', y='r7usd', data=facebookDf, label='r7usd')
    sns.lineplot(x='install_date', y='r7usdp', data=facebookDf, label='r7usdp')
    ax1.legend(loc='upper left')  # 将左侧y轴的图例放置在左上角

    ax2 = plt.twinx()
    sns.lineplot(x='install_date', y='MAPE', data=facebookDf, ax=ax2, linestyle='--', label='MAPE')

    ax2.fill_between(facebookDf['install_date'], 0.37, facebookDf['MAPE'], where=(facebookDf['MAPE'] > 0.37), color='red', alpha=0.5)
    ax2.legend(loc='upper right')  # 将右侧y轴的图例放置在右上角

    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)

    plt.savefig('/src/data/zk/facebook.jpg')
def debug9_5():
    
    df2 = loadData()
    df2 = df2.loc[df2['media'] == 'Facebook Ads']
    # 统计每天的付费用户数量，和r1usd总金额
    df2 = df2.loc[df2['r1usd'] > 0]
    df2 = df2.groupby('install_date').agg({'r1usd': 'sum', 'appsflyer_id': 'count'})
    # 重命名列名，将r1usd改名为pay_count
    df2.rename(columns={'appsflyer_id': 'pay_count'}, inplace=True)

    df = pd.read_csv(getFilename('attribution1RetCheck'))
    df = df.loc[df['media'] == 'Facebook Ads']

    mergeDf = df.merge(df2, left_on='install_date', right_index=True, how='left')
    mergeDf.to_csv(getFilename('attribution1RetCheck2'))

def debug9_6():
    # 将attribution1RetCheck2和attribution1Ret合并，也就是真实数据与融合归因数据合并
    attribution1RetCheck2 = pd.read_csv(getFilename('attribution1RetCheck2'))
    attribution1Ret = pd.read_csv(getFilename('attribution1Ret'))
    mergeDf = attribution1RetCheck2.merge(attribution1Ret, on=['install_date','media'], how='left', suffixes=('', '_att'))

    # 列改名
    mergeDf.rename(columns={
        'count': 'user_count_att', 
        'payCount': 'pay_count_att',
    }, inplace=True)

    # 计算r1usd与r1usd_att的MAPE，结果在列'r1usd MAPE'
    mergeDf['r1usd MAPE'] = abs(mergeDf['r1usd'] - mergeDf['r1usd_att'])/mergeDf['r1usd']

    # 计算r7usd与r7usd_att的MAPE，结果在列'r7usd MAPE'
    mergeDf['r7usd MAPE'] = abs(mergeDf['r7usd'] - mergeDf['r7usd_att'])/mergeDf['r7usd']
    
    # 计算user_count和user_count_att的MAPE，结果在列'user_count MAPE'
    mergeDf['user_count MAPE'] = abs(mergeDf['user_count'] - mergeDf['user_count_att'])/mergeDf['user_count']

    # 计算pay_count和pay_count_att的MAPE，结果在列'pay_count MAPE'
    mergeDf['pay_count MAPE'] = abs(mergeDf['pay_count'] - mergeDf['pay_count_att'])/mergeDf['pay_count']

    mergeDf = mergeDf[[
        'install_date',
        'media',
        'r1usd',
        'r1usd_att',
        'r1usd MAPE',
        'r7usd',
        'r7usd_att',
        'r7usd MAPE',
        'user_count',
        'user_count_att',
        'user_count MAPE',
        'pay_count',
        'pay_count_att',
        'pay_count MAPE',
    ]]
    print(mergeDf.head(10))
    mergeDf.to_csv(getFilename('attribution1RetCheck3'),index=False)


def debug10():
    df = pd.read_csv(getFilename('attribution1RetCheck2'))
    facebookDf = df.loc[df['media'] == 'Facebook Ads'].copy()

    # 计算带符号的MAPE，判断整体的偏向是高估还是低估
    facebookDf['偏差'] = (facebookDf['r7usdp'] - facebookDf['r7usd'])/facebookDf['r7usd']
    pc = facebookDf['偏差'].mean()
    print('偏差:',format(pc,'.2%'))

    # 尝试直接所有数值按照此偏差进行修正
    facebookDf['r7usdp2'] = facebookDf['r7usdp'] - facebookDf['r7usdp'] * pc
    facebookDf['MAPE2'] = abs(facebookDf['r7usdp2'] - facebookDf['r7usd'])/facebookDf['r7usd']
    mape2 = facebookDf['MAPE2'].mean()
    print('所有数据偏差修正 MAPE2:',format(mape2,'.2%'))

    # 只统计MAPE > 0.37的数据偏差
    facebookDf = df.loc[df['media'] == 'Facebook Ads'].copy()
    facebookDf['偏差'] = (facebookDf['r7usdp'] - facebookDf['r7usd'])/facebookDf['r7usd']
    facebookDf2 = facebookDf.loc[facebookDf['MAPE'] > 0.37].copy()

    pc2 = facebookDf2['偏差'].mean()
    print('MAPE > 0.37的数据偏差:',format(pc2,'.2%'))
    # 尝试只对MAPE > 0.37的数据进行修正
    facebookDf['r7usdp2'] = facebookDf['r7usdp']
    facebookDf.loc[facebookDf['MAPE'] > 0.37,'r7usdp2'] = facebookDf['r7usdp'] - facebookDf['r7usdp'] * pc2
    facebookDf['MAPE2'] = abs(facebookDf['r7usdp2'] - facebookDf['r7usd'])/facebookDf['r7usd']
    mape2 = facebookDf['MAPE2'].mean()
    print('MAPE > 0.37的数据偏差修正 MAPE2:',format(mape2,'.2%'))

    # 尝试找到MAPE > 0.37的数据出现的规律
    # 猜测1：MAPE > 0.37的数据出现在用户数（user_count）较少的行，统计MAPE高的行的user_count平均值与所有行的user_count平均值的比值
    facebookDf = df.loc[df['media'] == 'Facebook Ads'].copy()
    facebookDf['偏差'] = (facebookDf['r7usdp'] - facebookDf['r7usd'])/facebookDf['r7usd']
    facebookDf2 = facebookDf.loc[facebookDf['MAPE'] > 0.37].copy()
    print('MAPE > 0.37的数据user_count平均值:',facebookDf2['pay_count'].mean())
    print('所有数据user_count平均值:',facebookDf['pay_count'].mean())
    print('MAPE > 0.37的数据user_count平均值/所有数据user_count平均值:',facebookDf2['pay_count'].mean()/facebookDf['pay_count'].mean())

    # 猜测2：MAPE > 0.37的数据出现在用户数（user_count）变化较为明显的行，即在前一天的用户数与后一天的用户数差异较大的行
    # 计算MAPE高的行的前一天的用户数与后一天的用户数差异，和所有行的前一天的用户数与后一天的用户数差异的比值
    facebookDf = df.loc[df['media'] == 'Facebook Ads'].copy()
    facebookDf['偏差'] = (facebookDf['r7usdp'] - facebookDf['r7usd'])/facebookDf['r7usd']
    facebookDf2 = facebookDf.loc[facebookDf['MAPE'] > 0.37].copy()
    facebookDf2['user_count_diff'] = (facebookDf2['pay_count'] - facebookDf2['pay_count'].shift(1)) / facebookDf2['pay_count']
    facebookDf2['user_count_diff'] = facebookDf2['user_count_diff'].fillna(0)
    print('MAPE > 0.37的数据user_count_diff平均值:',facebookDf2['user_count_diff'].mean())

    facebookDf['user_count_diff'] = (facebookDf['pay_count'] - facebookDf['pay_count'].shift(1)) / facebookDf['pay_count']
    facebookDf['user_count_diff'] = facebookDf['user_count_diff'].fillna(0)
    print('所有数据user_count_diff平均值:',facebookDf['user_count_diff'].mean())

    print('MAPE > 0.37的数据user_count_diff平均值/所有数据user_count_diff平均值:',facebookDf2['user_count_diff'].mean()/facebookDf['user_count_diff'].mean())


if __name__ == '__main__':
    # getDataFromMC()

    # skanDf = makeSKAN()
    # # skanDf = skanAddValidInstallDate(skanDf)
    # skanDf = skanAddValidInstallDate2(skanDf)

    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS4'),index=False)
    # skanDf = pd.read_csv(getFilename('skanAOS3'))
    # skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS4G'),index=False)
    # print('skan data group len:',len(skanDf))

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # userDf.to_csv(getFilename('userAOS3'),index=False)
    # userDf = pd.read_csv(getFilename('userAOS3'))
    # userDf = userInstallDate2Min(userDf,N = 600)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS3G'),index=False)
    # print('user data group len:',len(userDf))

    # userDf = pd.read_csv(getFilename('userAOS3G'))
    # skanDf = pd.read_csv(getFilename('skanAOS3G'))
    # skanDf = pd.read_csv(getFilename('skanAOS4G'))   

    # skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    # userDf = meanAttribution(userDf, skanDf)
    # userDf = pd.read_parquet(getFilename('attribution1ReStep2','parquet'))
    # # meanAttributionAdv2(userDf,skanDf)

    userDf = pd.read_parquet(getFilename('attribution1ReStep2R3usd','parquet'))
    userDf = meanAttributionResult(userDf)
    # userDf = meanAttributionResult(None)

    # meanAttributionResultDebug(userDf)

    # userDf = pd.read_csv(getFilename('attribution1Ret'))
    # checkRet(userDf)
    # # # checkRetDebug(pd.read_csv(getFilename('attribution1RetDebug')))

    # userDf = attribution1(userDf,skanDf)
    # userDf.to_csv(getFilename('attribution1Ret'),index=False)
    # userDf = pd.read_csv(getFilename('attribution1Ret'))
    # result1(userDf,'2022')

    # debugResult1()
    
    # debug()
    # debug0()
    # draw()
    # debug2()
    # debug3()
    # debug4()
    # debug5()
    # debug6()

    # main2()
    # debug7()
    # debug8()
    # afSkan()
    # debug9()
    # debug9_5()
    # debug9_6()
    # debug10()