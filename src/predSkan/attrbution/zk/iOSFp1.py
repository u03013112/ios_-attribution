# iOS版本，进行归因方案1
# 具体方案参照androidFp
# 由于CV Map改变，所以暂时只处理3月1日及以后数据
# 此文件对应阿里线上（dataworks）iOS归因中的FunPlus01

import io
import pandas as pd
from datetime import datetime, timedelta

# 参数dayStr，是当前的日期，即${yyyymmdd-1}，格式为'20230301'
# 生成安装日期是dayStr - 7的各媒体7日回收金额

# 为了兼容本地调试，要在所有代码钱调用此方法
def init():
    global execSql
    global dayStr
    if 'o' in globals():
        print('this is online version')

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader() as reader:
                pd_df = reader.to_pandas()
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']

    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        dayStr = '20230401'

def getSKANDataFromMC(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=14)
    dayMax = day - timedelta(days=0)
    dayMinStr = dayMin.strftime('%Y%m%d')
    dayMaxStr = dayMax.strftime('%Y%m%d')

    # 修改后的SQL语句
    sql = f'''
        SELECT
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '{dayMinStr}' AND '{dayMaxStr}'
            AND app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 计算合法的激活时间范围
def skanAddValidInstallDate(skanDf):
    # 将postback_timestamp转换为datetime
    skanDf['postback_timestamp'] = pd.to_datetime(skanDf['postback_timestamp'])
    # 将cv转换为整数类型
    skanDf['cv'] = skanDf['cv'].astype(int)

    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=48)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=72)
    skanDf.loc[:, 'max_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=24)
    return skanDf

def getAfDataFromMC(dayStr):
    # 将dayStr转换为日期对象
    day = datetime.strptime(dayStr, '%Y%m%d')

    # 计算dayMinStr和dayMaxStr
    dayMin = day - timedelta(days=14)
    dayMax = day - timedelta(days=0)
    dayMinStr = dayMin.strftime('%Y%m%d')
    dayMaxStr = dayMax.strftime('%Y%m%d')

    # 修改后的SQL语句
    sql = f'''
        SELECT
            appsflyer_id,
            install_timestamp,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN event_revenue_usd ELSE 0 END) as r1usd,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN event_revenue_usd ELSE 0 END) as r7usd,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN '{dayMinStr}' AND '{dayMaxStr}'
            AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('{dayMinStr}', 'yyyyMMdd') AND to_date('{dayMaxStr}', 'yyyyMMdd')
        GROUP BY
            appsflyer_id,
            install_timestamp,
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def getCvMap():
    csv_str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change
id1479198816,0,,,,,,0,24,2023-02-28 10:26:05
id1479198816,1,af_skad_revenue,0,1,0,0.1085,0,24,2023-02-28 10:26:05
id1479198816,2,af_skad_revenue,0,1,0.1085,2.0385,0,24,2023-02-28 10:26:05
id1479198816,3,af_skad_revenue,0,1,2.0385,2.1823,0,24,2023-02-28 10:26:05
id1479198816,4,af_skad_revenue,0,1,2.1823,3.9045,0,24,2023-02-28 10:26:05
id1479198816,5,af_skad_revenue,0,1,3.9045,3.9466,0,24,2023-02-28 10:26:05
id1479198816,6,af_skad_revenue,0,1,3.9466,8.9064,0,24,2023-02-28 10:26:05
id1479198816,7,af_skad_revenue,0,1,8.9064,9.5759,0,24,2023-02-28 10:26:05
id1479198816,8,af_skad_revenue,0,1,9.5759,13.022,0,24,2023-02-28 10:26:05
id1479198816,9,af_skad_revenue,0,1,13.022,13.9664,0,24,2023-02-28 10:26:05
id1479198816,10,af_skad_revenue,0,1,13.9664,17.1351,0,24,2023-02-28 10:26:05
id1479198816,11,af_skad_revenue,0,1,17.1351,24.4163,0,24,2023-02-28 10:26:05
id1479198816,12,af_skad_revenue,0,1,24.4163,28.3681,0,24,2023-02-28 10:26:05
id1479198816,13,af_skad_revenue,0,1,28.3681,32.1223,0,24,2023-02-28 10:26:05
id1479198816,14,af_skad_revenue,0,1,32.1223,38.8436,0,24,2023-02-28 10:26:05
id1479198816,15,af_skad_revenue,0,1,38.8436,45.7686,0,24,2023-02-28 10:26:05
id1479198816,16,af_skad_revenue,0,1,45.7686,49.052,0,24,2023-02-28 10:26:05
id1479198816,17,af_skad_revenue,0,1,49.052,58.907,0,24,2023-02-28 10:26:05
id1479198816,18,af_skad_revenue,0,1,58.907,68.3503,0,24,2023-02-28 10:26:05
id1479198816,19,af_skad_revenue,0,1,68.3503,83.5979,0,24,2023-02-28 10:26:05
id1479198816,20,af_skad_revenue,0,1,83.5979,100.9974,0,24,2023-02-28 10:26:05
id1479198816,21,af_skad_revenue,0,1,100.9974,116.6691,0,24,2023-02-28 10:26:05
id1479198816,22,af_skad_revenue,0,1,116.6691,133.751,0,24,2023-02-28 10:26:05
id1479198816,23,af_skad_revenue,0,1,133.751,158.9723,0,24,2023-02-28 10:26:05
id1479198816,24,af_skad_revenue,0,1,158.9723,185.9247,0,24,2023-02-28 10:26:05
id1479198816,25,af_skad_revenue,0,1,185.9247,214.6985,0,24,2023-02-28 10:26:05
id1479198816,26,af_skad_revenue,0,1,214.6985,261.5803,0,24,2023-02-28 10:26:05
id1479198816,27,af_skad_revenue,0,1,261.5803,295.198,0,24,2023-02-28 10:26:05
id1479198816,28,af_skad_revenue,0,1,295.198,340.911,0,24,2023-02-28 10:26:05
id1479198816,29,af_skad_revenue,0,1,340.911,437.2609,0,24,2023-02-28 10:26:05
id1479198816,30,af_skad_revenue,0,1,437.2609,774.8002,0,24,2023-02-28 10:26:05
id1479198816,31,af_skad_revenue,0,1,774.8002,1292.9452,0,24,2023-02-28 10:26:05
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf

def addCv(df, cvMapDf):
    # 将数据类型转换为数值类型，无法解析的字符串转换为NaN
    df['r1usd'] = pd.to_numeric(df['r1usd'], errors='coerce')
    cvMapDf['min_event_revenue'] = pd.to_numeric(cvMapDf['min_event_revenue'], errors='coerce')
    cvMapDf['max_event_revenue'] = pd.to_numeric(cvMapDf['max_event_revenue'], errors='coerce')
    cvMapDf['conversion_value'] = pd.to_numeric(cvMapDf['conversion_value'], errors='coerce')

    df.loc[:, 'cv'] = 0
    for index, row in cvMapDf.iterrows():
        df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']), 'cv'] = row['conversion_value']
    
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(), 'cv'] = cvMapDf['conversion_value'].max()
    return df# 暂时就只关心这3个媒体

mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads',
    'snapchat_int'
]

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
    skanDf = skanDf.loc[skanDf['media'].isin(mediaList)]
    # 1. 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
    for media in mediaList:
        userDf[media + ' count'] = 0

    userDf['install_timestamp'] = pd.to_datetime(userDf['install_timestamp'], unit='s')

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
        # print(f"media: {media}, cv: {cv}, num_matching_rows: {num_matching_rows}")

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

# 获得安装日期，格式为%Y%m%d
def getInstallDayStr(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    installDayStr = dayMin.strftime('%Y%m%d')
    return installDayStr

# 获得安装日期，格式为%Y-%m-%d
def getInstallDayStr2(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    installDayStr = dayMin.strftime('%Y-%m-%d')
    return installDayStr

# 将结论总结
def result1(userDf,dayStr):
    # 将数据类型转换为数值类型
    for media in mediaList:
        userDf[media + ' count'] = pd.to_numeric(userDf[media + ' count'], errors='coerce')
    userDf['r1usd'] = pd.to_numeric(userDf['r1usd'], errors='coerce')
    userDf['r7usd'] = pd.to_numeric(userDf['r7usd'], errors='coerce')

    for media in mediaList:
        userDf.loc[:,media+' r1usd'] = userDf[media+' count'] * userDf['r1usd']
        userDf.loc[:,media+' r7usd'] = userDf[media+' count'] * userDf['r7usd']
    
    # userDf = userDf.groupby(['install_date']).agg('sum').reset_index()
    # 只保留一天的数据 dayStr - 7
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    dayMinStr = dayMin.strftime('%Y-%m-%d')
    installDayStr = dayMin.strftime('%Y%m%d')
    
    # print(installDayStr)

    df = userDf.loc[userDf['install_date'] == dayMinStr]

    retDf = pd.DataFrame(columns=['install_date','media','r7usd'])
    for media in mediaList:
        r7usd = df['%s r7usd'%media].sum()
        retDf = retDf.append({'install_date':installDayStr,'media':media,'r7usd':r7usd},ignore_index=True)
    
    return retDf

from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='media', type='string', comment='like google,bytedance,facebook'),
            Column(name='r7usd', type='double', comment='d7Revenue')
        ]
        partitions = [
            Partition(name='install_date', type='string', comment='like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('topwar_ios_funplus01', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable(df,installDayStr):
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus01')
        t.delete_partition('install_date=%s'%(installDayStr), if_exists=True)
        with t.open_writer(partition='install_date=%s'%(installDayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')

init()
skanDf = getSKANDataFromMC(dayStr)
# 将cv为空的数据去掉
skanDf = skanDf[skanDf['cv'].notna()]
skanDf = skanAddValidInstallDate(skanDf)
# print(skanDf.head(10))

afDf = getAfDataFromMC(dayStr)
userDf = addCv(afDf,getCvMap())
# print(userDf.head(10))

# userDf.to_csv('/src/data/userDf.csv',index=False)
# skanDf.to_csv('/src/data/skanDf.csv',index=False)

# userDf = pd.read_csv('/src/data/userDf.csv')
# skanDf = pd.read_csv('/src/data/skanDf.csv')
# print(userDf.head(10))
# print(skanDf.head(10))

attDf = attribution1(userDf,skanDf)

# attDf.to_csv('/src/data/attDf.csv',index=False)

retDf = result1(attDf,dayStr)

# debug
# df = userDf.loc[userDf['install_date'] == '2023-03-25']
# print(df['r7usd'].sum())
print(retDf)
createTable()
writeTable(retDf,getInstallDayStr(dayStr))
