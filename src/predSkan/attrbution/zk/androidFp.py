# 根据FuncPlus公司方案，进行Android数据验证
# 主体思路
# 1、根据postback时间进行安装时间有效范围的计算
# 2、在有效时间范围内找到所有CV一致的用户
# 3、将所有有效的用户进行平分，平分给所有有可能的媒体
# 实验步骤
# 首先生成模拟SKAN报告
# 1、获得海外安卓用户数据，包括用户id，安装时间，首日付费金额，7日付费金额，安装时间戳，最后付费时间戳，媒体
# 2、通过CV Map给用户添加cv
# 3、通过随机的方式，随机生成用户的postback时间
# 4、生成模拟SKAN报告
# 然后尝试进行归因（方案1）
# 1、根据postback时间，计算用户的安装时间有效范围
# 2、为SKAN报告中每个人（行），找到可以匹配的用户id列表
# 3、按照平均的比例对用户进行归因，即给用户N个属性，N是待归因媒体数量，每个属性的值都是0~1之间的数
# 4、按照归因结果，计算各媒体归因后的7日总金额与真实媒体7日总金额的误差（MAPE，R2）

import datetime
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename):
    return '/src/data/zk/%s.csv'%(filename)

def getDataFromMC():
    # 从AF事件表中获取用户数据
    # 安装日期在2023-01-01~2023-04-01之间
    # 海外安卓
    # 用af id做用户区分
    # 包括af id，安装时间(天），首日付费金额，7日付费金额，安装时间戳，最后付费时间戳（7日内），媒体
    # 生成postback
    sql = '''
        WITH user_event_data AS (
            SELECT
                appsflyer_id,
                MIN(install_timestamp) AS install_timestamp,
                MIN(event_timestamp) AS event_timestamp,
                SUM(event_revenue_usd) AS event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND day >= 20230101
                AND day <= 20230410
                AND install_time >= '2023-01-01'
                AND install_time < '2023-04-01'
                AND event_name = 'af_purchase'
            GROUP BY
                appsflyer_id
        ),
        last_payment_data AS (
            SELECT
                appsflyer_id,
                MAX(event_timestamp) AS last_payment_timestamp
            FROM
                user_event_data
            WHERE
                event_revenue_usd > 0
                AND event_timestamp - install_timestamp <= 24 * 3600
            GROUP BY
                appsflyer_id
        ),
        postback_data AS (
            SELECT
                DISTINCT u.appsflyer_id,
                u.install_timestamp,
                l.last_payment_timestamp,
                CASE
                    WHEN l.last_payment_timestamp IS NOT NULL THEN l.last_payment_timestamp + 24 * 3600 + FLOOR(RANDOM() * (24 * 3600 + 1))
                    ELSE u.install_timestamp + 24 * 3600 + FLOOR(RANDOM() * (24 * 3600 + 1))
                END AS postback_timestamp
            FROM
                user_event_data u
                LEFT JOIN last_payment_data l ON u.appsflyer_id = l.appsflyer_id
        )
        SELECT
            o.appsflyer_id,
            to_char(
                to_date(o.install_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyy-mm-dd'
            ) AS install_date,
            sum(
                CASE
                    WHEN o.event_timestamp - p.install_timestamp <= 1 * 24 * 3600 THEN CAST(o.event_revenue_usd AS DOUBLE)
                    ELSE 0
                END
            ) AS r1usd,
            sum(
                CASE
                    WHEN o.event_timestamp - p.install_timestamp <= 7 * 24 * 3600 THEN CAST(o.event_revenue_usd AS DOUBLE)
                    ELSE 0
                END
            ) AS r7usd,
            p.install_timestamp,
            p.last_payment_timestamp,
            p.postback_timestamp,
            o.media_source AS media
        FROM
            ods_platform_appsflyer_events o
            JOIN postback_data p ON o.appsflyer_id = p.appsflyer_id
        WHERE
            o.app_id = 'com.topwar.gp'
            AND o.zone = 0
            AND o.day >= 20230101
            AND o.day <= 20230410
            AND o.install_time >= '2023-01-01'
            AND o.install_time < '2023-04-01'
            AND o.event_name = 'af_purchase'
        GROUP BY
            install_date,
            o.appsflyer_id,
            p.install_timestamp,
            p.last_payment_timestamp,
            p.postback_timestamp,
            media
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('androidFp01'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp01'))
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

# 暂时就只关心这3个媒体
mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads'
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
    if __debug__:
        skanDf = cvDf[['postback_timestamp','media','cv','appsflyer_id']]
    else:
        skanDf = cvDf[['postback_timestamp','media','cv']]
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

# 制作待归因用户Df
def makeUserDf():
    df = loadData()
    # 是否要处理IDFA数据？
    # 如果要处理应该怎么处理？
    # 暂时放弃处理IDFA，相信SSOT
    cvMapDf = getCvMap()
    userDf = addCv(df,cvMapDf)

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r7usd','cv']]
    return userDf

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 结论计算
# 输入是归因后的userDf
# 要求包括列：install_timestamp, r1usd, r7usd, 所有media count，media count受mediaList影响
def resault(userDf,message):
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
        mergeDf.loc[:,'mape1'] = abs(mergeDf['r1usd_att'] - mergeDf['r1usd_raw']) / mergeDf['r1usd_raw']
        mergeDf.loc[:,'mape7'] = abs(mergeDf['r7usd_att'] - mergeDf['r7usd_raw']) / mergeDf['r7usd_raw']

        mape1 = mergeDf['mape1'].mean()
        mape7 = mergeDf['mape7'].mean()

        print(media+' mape1: '+str(mape1))
        print(media+' mape7: '+str(mape7))
        mergeDf.to_csv(getFilename(media+'_%s_resault'%(message)),index=False)

        # 在这里根据mergeDf中的‘r1usd_att’和‘r1usd_raw’，‘r7usd_att’和‘r7usd_raw’画图
        # mergeDf的'install_date'作为x轴
        # mergeDf的'r1usd_att'和'r1usd_raw'，‘r7usd_att’和‘r7usd_raw’作为y轴
        mergeDf['install_date'] = pd.to_datetime(mergeDf['install_date'])
        # 绘制r1usd图表
        plt.figure(figsize=(10, 6))
        plt.plot(mergeDf['install_date'], mergeDf['r1usd_att'], label='r1usd_att', linestyle='-', marker='o')
        plt.plot(mergeDf['install_date'], mergeDf['r1usd_raw'], label='r1usd_raw', linestyle='--', marker='o')
        plt.xlabel('Install Date')
        plt.ylabel('USD')
        plt.title(f'{media} - r1usd (Raw vs Attributed)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))
        plt.tight_layout()
        plt.savefig("/src/data/zk/%s_%s_resault1.jpg" % (media, message))

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
        plt.savefig("/src/data/zk/%s_%s_resault7.jpg" % (media, message))



# 归因方案1
# 均分归因，步骤如下
# 1、给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
# 2、遍历skanDf，每行做如下处理：获取media，cv，min_valid_install_timestamp和max_valid_install_timestamp。
# 在userDf中找到userDf.cv == skanDf.cv，并且 skanDf.max_valid_install_timestamp >= userDf.install_timestamp >= skanDf.min_valid_install_timestamp 的行。
# 该行的media+' count'列的值加1/N，N是符合上面条件的行数。比如通过cv与时间戳过滤找到符合的行是2，则每行的media+' count'列的值加1/2
# 3、返回前验证，检查userDf中是否有行的所有的media count列的和大于1，如果有，统计一下有多少行，占比（行数/总行数）是多少
# 4、返回userDf
# userDf 拥有列 appsflyer_id  install_timestamp      r1usd      r7usd  cv
# skanDf 拥有列 postback_timestamp  media  cv  min_valid_install_timestamp  max_valid_install_timestamp  postback_date  min_valid_install_date  max_valid_install_date
def attribution1(userDf,skanDf):
    # 1. 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
    for media in mediaList:
        userDf[media + ' count'] = 0

    # 2. 遍历skanDf，每行做如下处理
    for index, row in skanDf.iterrows():
        if __debug__:
            # 如果处于调试模式，仅处理第22676行
            if index != 22676:
                continue
            else:
                print("Debug mode: Processing row 22676")
                print(row)
                
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

        if __debug__:
            # 打印matching_rows
            print("Matching rows:")
            print(matching_rows)

        num_matching_rows = len(matching_rows)

        if num_matching_rows > 0:
            userDf.loc[condition, media + ' count'] += 1 / num_matching_rows

    # 3. 检查userDf中是否有行的所有的media count列的和大于1，如果有，统计一下有多少行，占比（行数/总行数）是多少
    media_counts_sum = userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    invalid_rows = media_counts_sum > 1
    num_invalid_rows = invalid_rows.sum()
    total_rows = len(userDf)
    invalid_ratio = num_invalid_rows / total_rows

    print(f"Invalid rows: {num_invalid_rows}")
    print(f"Invalid ratio: {invalid_ratio:.2%}")

    # 4. 返回userDf
    return userDf


# 
if __name__ == '__main__':

    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)
    # skanDf.to_csv(getFilename('skan'),index=False)

    # userDf = makeUserDf()
    # userDf.to_csv(getFilename('user'),index=False)



    # userDf = attribution1(userDf,skanDf)
    # userDf.to_csv(getFilename('attribution1'),index=False)

    attDf = pd.read_csv(getFilename('attribution1'))
    resault(attDf,'att1')
    
