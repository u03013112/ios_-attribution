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

def meanAttribution(userDf, skanDf):
    userDf['attribute'] = [list() for _ in range(len(userDf))]
    unmatched_rows = 0
    unmatched_user_count = 0

    for index, row in skanDf.iterrows():
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

    userDf.to_csv(getFilename('attribution1ReStep2'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep2','parquet'), index=False)
    return userDf


def meanAttributionAdv(userDf):
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

    userDf.to_csv(getFilename('attribution1ReStep4'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep4','parquet'), index=False)
    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
    for media in mediaList:
        print(f"Processing media: {media}")
        userDf[media + ' count'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x if item['media'] == media]))

    # Drop the 'attribute' column
    userDf = userDf.drop(columns=['attribute'])

    # userDf.to_csv(getFilename('attribution1ReStep6'), index=False)
    # userDf = pd.read_csv(getFilename('attribution1ReStep6'))
    
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
        userDf[media + ' r7usd'] = (userDf['r7usd'] / userDf['user_count']) * userDf[media_count_col]

    # Drop unnecessary columns
    userDf = userDf.drop(columns=['install_timestamp', 'cv', 'user_count', 'r7usd'] + [media + ' count' for media in mediaList])

    # Melt the DataFrame to have 'media' and 'r7usd' in separate rows
    userDf = userDf.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf['media'] = userDf['media'].str.replace(' r7usd', '')

    # Group by 'media' and 'install_date' and calculate the sum of 'r7usd'
    userDf = userDf.groupby(['media', 'install_date']).agg({'r7usd': 'sum'}).reset_index()
    userDf.rename(columns={'r7usd': 'r7usdp'}, inplace=True)

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
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['media', 'install_date']).agg({'r7usd': 'sum'}).reset_index()

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

def debug():
    df = loadData()
    df = df[
        (df['media'] == 'snapchat_int') &
        (df['install_date'] >= '2023-02-15') &
        (df['install_date'] <= '2023-02-20')
    ]
    print(df)


import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def draw():
    df = pd.read_csv(getFilename('attribution1RetCheck'))
    # 将不同的媒体分开画图，图片宽一点
    # install_date作为x轴，每隔7天画一个点
    # 双y轴，y1是r7usd和r7usdp；y2是MAPE（用虚线）。
    # 图片保存到'/src/data/zk/att1_{media}.jpg'
    # Convert 'install_date' to datetime
    df['install_date'] = pd.to_datetime(df['install_date'])

    for media in mediaList:
        media_df = df[df['media'] == media]

        # Create the plot with the specified figure size
        fig, ax1 = plt.subplots(figsize=(18, 6))

        # Plot r7usd and r7usdp on the left y-axis
        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd')
        ax1.plot(media_df['install_date'], media_df['r7usdp'], label='r7usdp')
        ax1.set_ylabel('r7usd and r7usdp')
        ax1.set_xlabel('Install Date')

        # Plot MAPE on the right y-axis with dashed line
        ax2 = ax1.twinx()
        ax2.plot(media_df['install_date'], media_df['MAPE'], label='MAPE', linestyle='--', color='red')
        ax2.set_ylabel('MAPE')

        # Set x-axis to display dates with a 7-day interval
        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(media_df['install_date'][::7], rotation=45)

        # Add legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

        # Save the plot as a jpg image
        plt.savefig(f'/src/data/zk/att1_{media}.jpg', bbox_inches='tight')
        plt.close()

if __name__ == '__main__':
    # getDataFromMC()

    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)
    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS2'),index=False)
    # # skanDf = pd.read_csv(getFilename('skanAOS2'))
    # skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS2G'),index=False)
    # print('skan data group len:',len(skanDf))

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # userDf.to_csv(getFilename('userAOS2'),index=False)
    # # userDf = pd.read_csv(getFilename('userAOS2'))
    # userDf = userInstallDate2Min(userDf,N = 600)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS2G'),index=False)
    # print('user data group len:',len(userDf))

    # userDf = pd.read_csv(getFilename('userAOS2G'))
    # skanDf = pd.read_csv(getFilename('skanAOS2G'))   

    # skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    # meanAttribution(userDf, skanDf)
    userDf = pd.read_parquet(getFilename('attribution1ReStep2','parquet'))
    # meanAttributionAdv(userDf)
    meanAttributionResult(userDf)

    # checkRet(pd.read_csv(getFilename('attribution1Ret')))
    

    
    # debug()
    draw()