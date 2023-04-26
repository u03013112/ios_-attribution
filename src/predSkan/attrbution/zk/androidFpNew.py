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

def getFilename(filename):
    return '/src/data/zk/%s.csv'%(filename)

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
                AND day BETWEEN '20230101'
                AND '20230408'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
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
                AND day BETWEEN '20230101'
                AND '20230408'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
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
    df.to_csv(getFilename('androidFp02'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp02'))
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
        skanDf = cvDf[['postback_timestamp','media','cv','appsflyer_id']]
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

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r7usd','cv']]
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
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'appsflyer_id':'count','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'appsflyer_id':'user_count'}, inplace=True)
    return userGroupbyDf


# 归因方案1 重分配改进版（分组版本）
# 步骤如下
# 1、给userDf添加列，列名为attribute,默认值是[]。里面用于存储归因的媒体名，skan index，和对应的count。
# 格式是{'media': 'media1', 'skan index': 1, 'count': 0.5},attribute列存储的是这种结构的列表。
# 2、遍历skanDf，每行做如下处理：获取media，cv，user_count，min_valid_install_timestamp和max_valid_install_timestamp。
# 在userDf中匹配 userDf.cv == skanDf.cv，并且 skanDf.max_valid_install_timestamp >= userDf.install_timestamp >= skanDf.min_valid_install_timestamp 的行。
# 该行的attribute列的列表添加一个字典，media，skan index都取自skanDf这一行。
# count的值是M/N，M是skanDf此行中的user_count；N是userDf符合上面条件的行的user_count的和。
# 比如skanDf 行中 user_count 是 5，匹配到userDf中共有两行，user_count分别是2和3，那么count的值是5/5=1。
# 3、找到userDf中attribute列中count的和大于1的行，对这些行做如下处理：
# 找到attribute列中count最小的1个元素。
# 从attribute列中删除这个元素。
# 找到usdDf所有attribute列中包含此skan index的行。重新计算count，count为M/N，M是skanDf行中的user_count；N为剩余usdDf所有attribute列中包含此skan index的user_count的总和。
# 4、检查userDf中是否还有行的attribute列中count的和大于1，如果有重复步骤3。（有循环次数上限）
# 第4步骤完成后保存一个临时csv，用于记录归因过程中的结果。getFilename('attribution1ReStep4')
# 5、汇总userDf中attribute列中的media，计算每个media的count的和，作为最终的归因结果。
# 格式采用之前归因方案1的格式，即 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'
# count值是从attribute列中的按照media汇总count的值。
# 6、返回userDf
# 输入参数：
# userDf 拥有列 install_timestamp,cv,user_count,r7usd
# skanDf 拥有列 media,cv,min_valid_install_timestamp,max_valid_install_timestamp,user_count
def attribution1Re(userDf, skanDf, mediaList = mediaList):
    # 1. 给userDf添加列，列名为attribute,默认值是[]。
    userDf['attribute'] = [list() for _ in range(len(userDf))]
    unmatched_rows = 0
    unmatched_user_count = 0

    # 2. 遍历skanDf，每行做如下处理
    for index, row in skanDf.iterrows():
        media = row['media']
        cv = row['cv']
        min_valid_install_timestamp = row['min_valid_install_timestamp']
        max_valid_install_timestamp = row['max_valid_install_timestamp']

        # 在userDf中找到符合条件的行
        condition = (
            (userDf['cv'] == cv) &
            (userDf['install_timestamp'] >= min_valid_install_timestamp) &
            (userDf['install_timestamp'] <= max_valid_install_timestamp)
        )
        matching_rows = userDf[condition]
        num_matching_rows = len(matching_rows)

        if num_matching_rows > 0:
            count = 1 / num_matching_rows
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

    userDf.to_csv(getFilename('attribution1ReStep2'))

    # 3. 找到userDf中attribute列中count的和大于1的行，对这些行做如下处理
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

        if invalid_rows.empty:
            break

        for _, invalid_row in invalid_rows.iterrows():
            attribute_list = invalid_row['attribute']
            min_count_item = min(attribute_list, key=lambda x: x['count'])
            attribute_list.remove(min_count_item)

            skan_index = min_count_item['skan index']
            affected_rows = userDf[userDf['attribute'].apply(lambda x: any(item['skan index'] == skan_index for item in x))]
            num_affected_rows = len(affected_rows)

            if num_affected_rows > 0:
                count = 1 / num_affected_rows
                for _, affected_row in affected_rows.iterrows():
                    appsflyer_id = affected_row['appsflyer_id']
                    userDf.loc[userDf['appsflyer_id'] == appsflyer_id, 'attribute'] = userDf.loc[userDf['appsflyer_id'] == appsflyer_id, 'attribute'].apply(
                        lambda x: [item if item['skan index'] != skan_index else {**item, 'count': count} for item in x]
                    )

        elapsed_time = time.time() - start_time
        print(f"Elapsed time for iteration {watchDog}: {elapsed_time:.2f} seconds")

    userDf.to_csv(getFilename('attribution1ReStep4'))

    # 5. 汇总userDf中attribute列中的media，计算每个media的count的和，作为最终的归因结果
    for media in mediaList:
        userDf[media + ' count'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x if item['media'] == media]))

    # 6. 返回userDf
    userDf.to_csv(getFilename('attribution1ReStep6'))
    return userDf

if __name__ == '__main__':
    getDataFromMC()

    skanDf = makeSKAN()
    skanDf = skanAddValidInstallDate(skanDf)
    print('skan data len:',len(skanDf))
    skanDf.to_csv(getFilename('skanAOS2'),index=False)
    # skanDf = pd.read_csv(getFilename('skanAOS2'))
    skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    skanDf = skanGroupby(skanDf)
    skanDf.to_csv(getFilename('skanAOS2G'),index=False)
    print('skan data group len:',len(skanDf))

    userDf = makeUserDf()
    print('user data len:',len(userDf))
    userDf.to_csv(getFilename('userAOS2'),index=False)
    # userDf = pd.read_csv(getFilename('userAOS2'))
    userDf = userInstallDate2Min(userDf,N = 600)
    userDf = userGroupby(userDf)
    userDf.to_csv(getFilename('userAOS2G'),index=False)
    print('user data group len:',len(userDf))

    userDf = pd.read_csv(getFilename('userAOS2G'))
    skanDf = pd.read_csv(getFilename('skanAOS2G'))   

    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    attribution1Re(userDf, skanDf)
    
