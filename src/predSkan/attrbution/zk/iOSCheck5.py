# 针对2023-04-01~2023-05-13的数据，进行归因
# 媒体添加applovin_int
# 另外也是相信模糊归因的，将模糊归因的数据先拿出来，然后再进行归因
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj

def getFilename(filename,ext='csv'):
    return '/src/data/zk/%s.%s'%(filename,ext)

mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads',
    'snapchat_int',
    'applovin_int'
]

def getSKANDataFromMC():
    sql = '''
        SELECT
            event_uuid,
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp,
            install_date
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '20230401' AND '20230513'
            AND app_id = 'id1479198816'
            AND event_name in ('af_skad_install','af_skad_redownload')
            AND media_source in (
                'bytedanceglobal_int',
                'googleadwords_int',
                'Facebook Ads',
                'snapchat_int',
                'applovin_int'
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
    
    # 使用replace替换cv列中的字符串'null'为0
    skanDf['cv'] = skanDf['cv'].replace('null', 0)
    
    # 使用fillna填充cv列中的空值
    skanDf['cv'] = skanDf['cv'].fillna(0)
    
    # 将cv转换为整数类型
    skanDf['cv'] = skanDf['cv'].astype(int)

    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=48)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=72)
    skanDf.loc[:, 'max_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=24)
    
    # 适当地拓宽范围，由于苹果的时间戳并不保证准确，所以拓宽范围，暂定1小时
    # skanDf.loc[:,'min_valid_install_timestamp'] -= pd.Timedelta(hours=24)
    return skanDf

def getAfDataFromMC():
    sql = '''
        SELECT
            appsflyer_id,
            install_timestamp,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN event_revenue_usd ELSE 0 END) as r1usd,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN event_revenue_usd ELSE 0 END) as r7usd,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            media_source as media
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN '20230401' AND '20230513'
            AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('20230401', 'yyyyMMdd') AND to_date('20230513', 'yyyyMMdd')
        GROUP BY
            appsflyer_id,
            install_timestamp,
            install_date,
            media_source
        ;
    '''
    print(sql)
    df = execSql(sql)
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
    df['cv'] = 0
    for index, row in cvMapDf.iterrows():
        df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']),'cv'] = row['conversion_value']
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(),'cv'] = cvMapDf['conversion_value'].max()
    return df

# 制作待归因用户Df
def makeUserDf():
    df = getAfDataFromMC()
    df.to_csv('/src/data/zk/af5.csv',index=False)
    df = pd.read_csv('/src/data/zk/af5.csv')
    # Replace NaN values with empty strings in the 'media' column
    df['media'] = df['media'].fillna('')
    # 过滤掉已经有归因的部分
    df = df.loc[df['media'] == ''].copy()
    
    cvMapDf = getCvMap()
    userDf = addCv(df,cvMapDf)

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r7usd','cv']]
    return userDf

def meanAttribution(userDf, skanDf):
    userDf['attribute'] = [list() for _ in range(len(userDf))]
    unmatched_rows = 0
    unmatched_user_count = 0
    # unmatched_revenue = 0
    unmatched_rows_data = []

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
            z = row['user_count']
            m = matching_rows['user_count'].sum()
            count = z / m
            # count = 1 / num_matching_rows
            attribution_item = {'media': media, 'skan index': index, 'count': count}
            userDf.loc[condition, 'attribute'] = userDf.loc[condition, 'attribute'].apply(lambda x: x + [attribution_item])
        # else:
        #     # print(f"Unmatched row: {row}")
        #     unmatched_rows_data.append(row)
        #     unmatched_rows += 1
        #     unmatched_user_count += row['user_count']
        #     unmatched_revenue += row['skad_revenue']
        else:
            # 尝试扩大匹配范围
            min_valid_install_timestamp -= 48 * 3600
            condition_expanded = (
                (userDf['cv'] == cv) &
                (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                (userDf['install_timestamp'] <= max_valid_install_timestamp)
            )
            matching_rows_expanded = userDf[condition_expanded]
            num_matching_rows_expanded = len(matching_rows_expanded)

            if num_matching_rows_expanded > 0:
                z = row['user_count']
                m = matching_rows_expanded['user_count'].sum()
                count = z / m
                attribution_item = {'media': media, 'skan index': index, 'count': count}
                userDf.loc[condition_expanded, 'attribute'] = userDf.loc[condition_expanded, 'attribute'].apply(lambda x: x + [attribution_item])
            else:
                unmatched_rows_data.append(row)
                unmatched_rows += 1
                unmatched_user_count += row['user_count']
                # unmatched_revenue += row['skad_revenue']
                
    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_user_count_ratio = unmatched_user_count / skanDf['user_count'].sum()
    # unmatched_revenue_ratio = unmatched_revenue / skanDf['skad_revenue'].sum()

    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched user count ratio: {unmatched_user_count_ratio:.2%}")
    # print(f"Unmatched revenue ratio: {unmatched_revenue_ratio:.2%}")

    unmatched_rows_df = pd.DataFrame(unmatched_rows_data)
    unmatched_rows_df.to_csv(getFilename('unmatched_rows'), index=False)

    userDf.to_csv(getFilename('attribution1ReStep0503'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep0503','parquet'), index=False)
    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
    if userDf is not None:    
        for media in mediaList:
            print(f"Processing media: {media}")
            userDf[media + ' count'] = userDf['attribute'].apply(lambda x: sum([item['count'] for item in x if item['media'] == media]))

        # Drop the 'attribute' column
        userDf = userDf.drop(columns=['attribute'])

        userDf.to_csv(getFilename('attribution1ReStep6'), index=False)
    userDf = pd.read_csv(getFilename('attribution1ReStep6'))

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
        userDf[media + ' user_count'] = userDf['user_count'] * userDf[media_count_col]
        userDf[media + ' r1usd'] = userDf['r1usd'] * userDf[media_count_col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]

    # r1usd
    mediaColumns = [media + ' r1usd' for media in mediaList]
    columns = ['install_date','cv'] + mediaColumns
    userDf1 = userDf[columns]

    userDf1 = userDf1.melt(id_vars=['install_date','cv'], value_vars=mediaColumns,var_name='media', value_name='r1usd')
    userDf1['media'] = userDf1['media'].str.replace(' r1usd', '')
    userDf1 = userDf1.groupby(['media', 'install_date','cv']).agg({'r1usd': 'sum'}).reset_index()
    print(userDf1.head())

    # r7usd
    mediaColumns = [media + ' r7usd' for media in mediaList]
    columns = ['install_date','cv'] + mediaColumns
    userDf2 = userDf[columns]

    userDf2 = userDf2.melt(id_vars=['install_date','cv'], value_vars=mediaColumns, var_name='media', value_name='r7usd')
    userDf2['media'] = userDf2['media'].str.replace(' r7usd', '')
    userDf2 = userDf2.groupby(['media', 'install_date','cv']).agg({'r7usd': 'sum'}).reset_index()
    print(userDf2.head())

    # user_count
    mediaColumns = [media + ' user_count' for media in mediaList]
    columns = ['install_date','cv'] + mediaColumns
    userDf3 = userDf[columns]

    userDf3 = userDf3.melt(id_vars=['install_date','cv'], value_vars=mediaColumns, var_name='media', value_name='installs')
    userDf3['media'] = userDf3['media'].str.replace(' user_count', '')
    userDf3 = userDf3.groupby(['media', 'install_date','cv']).agg({'installs': 'sum'}).reset_index()
    print(userDf3.head())

    
    userDf = userDf1.merge(userDf2, on=['install_date', 'media','cv']).merge(userDf3, on=['install_date', 'media','cv'])
    # userDf = userDf1.merge(userDf3, on=['install_date', 'media','cv'])
    userDf.to_csv(getFilename('a51'), index=False)

    # Group by 'media' and 'install_date' and calculate the sum of 'r7usd'
    userDf = userDf.groupby(['media', 'install_date']).agg({'r1usd': 'sum','r7usd': 'sum','installs': 'sum'}).reset_index()
    # userDf.rename(columns={'r7usd': 'r7usdp'}, inplace=True)

    # Save to CSV
    userDf.to_csv(getFilename('a52'), index=False)
    return userDf
    
def main():
    # skanDf = getSKANDataFromMC()
    # skanDf.to_csv('/src/data/zk/skan5.csv', index=False)
    # skanDf = pd.read_csv('/src/data/zk/skan5.csv')
    # skanDf = skanAddValidInstallDate(skanDf)
    # skanDf.to_csv('/src/data/zk/skan.csv', index=False)
    # userDf = makeUserDf()
    # userDf.to_csv('/src/data/zk/user.csv', index=False)

    skanDf = pd.read_csv('/src/data/zk/skan.csv')
    userDf = pd.read_csv('/src/data/zk/user.csv')
    # SSOT数据排除
    skanDf = skanDf.loc[skanDf['cv'] < 32]
    # skanDf = skanDf.loc[
    #     (skanDf['postback_timestamp'] >= '2023-04-05') &
    #     (skanDf['postback_timestamp'] < '2023-04-24')
    # ]

    # 转格式，将格式转成androidFpNewDebug中一致格式，方便直接使用该方法
    # 先将skanDf中的min_valid_install_timestamp和max_valid_install_timestamp由原来的‘2023-03-03 01:01:15’类似格式，转成unix时间戳，单位秒
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['min_valid_install_timestamp']).astype(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = pd.to_datetime(skanDf['max_valid_install_timestamp']).astype(np.int64) // 10 ** 9

    # 再将skanDf与userDf都按照10分钟进行汇总
    N = 600
    userDf['install_timestamp'] = userDf['install_timestamp'] // N * N
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'appsflyer_id':'count','r1usd':'sum','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'appsflyer_id':'user_count'}, inplace=True)

    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // N * N
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // N * N
    skanDf['user_count'] = 1
    # skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
    #     {'user_count': 'sum','skad_revenue': 'sum'}
    # ).reset_index()
    skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
        {'user_count': 'sum', 'event_uuid': lambda x: ','.join(x)}
    ).reset_index()

    userDf = meanAttribution(userGroupbyDf, skanGroupbyDf)
    userDf = pd.read_parquet(getFilename('attribution1ReStep0503','parquet'))
    userDf = meanAttributionResult(userDf)
    # userDf = pd.read_csv(getFilename('attribution1Ret0504'))


    # meanAttributionResult(None)


def getSsotFromMC():
    sql = '''
        select 
            media_source as media, 
        from rg_ai_bj.ads_appsflyer_ssot
        where
            day > '20230401'
            and day < '20230430'
        ;
    '''
    print(sql)
    df = execSqlBj(sql)
    return df

from sklearn.metrics import r2_score
def ret():
    afDf = pd.read_csv('/src/data/zk/af5.csv')
    # 过滤，只保留media在mediaList中的部分
    print('afDf:',afDf['media'].unique())
    # afDf 列 ‘media’中，‘restricted’改为‘Facebook Ads’
    afDf['media'] = afDf['media'].str.replace('restricted','Facebook Ads')
    afDf = afDf.loc[afDf['media'].isin(mediaList)].copy()
    cvMapDf = getCvMap()
    afDf = addCv(afDf,cvMapDf)
    afDf['installs'] = 1
    afDf = afDf[['media','install_date','cv','r1usd','r7usd','installs']]

    afDf = afDf.groupby(['media','install_date','cv']).agg({'r1usd':'sum','r7usd':'sum','installs':'sum'}).reset_index()
    afDf.to_csv('/src/data/zk/af51.csv',index=False)

    resultDf = pd.read_csv('/src/data/zk/a51.csv')
    # afDf和resultDf 列一致，数据拼接
    resultDf = resultDf.append(afDf)

    resultDf = resultDf.groupby(['media','install_date','cv']).agg({'r1usd':'sum','r7usd':'sum','installs':'sum'}).reset_index()
    resultDf.to_csv('/src/data/zk/a51+af51.csv',index=False)
    
    # 
    resultDf2 = resultDf.groupby(['media','install_date']).agg({'r1usd':'sum','r7usd':'sum','installs':'sum'}).reset_index()
    resultDf2.to_csv('/src/data/zk/a51+af51+2.csv',index=False)


def debug1():
    df = pd.read_csv('/src/data/zk/a51+af51.csv')
    df = df[['media','install_date','cv','installs']]

    # 要求每一种 media,install_date的组合，cv都要有0-31的数据，如果没有，则补充，补充的installs为0
    # 使用pivot_table将数据转换为宽格式，行索引为media和install_date，列索引为cv
    wide_df = df.pivot_table(index=['media', 'install_date'], columns='cv', values='installs', fill_value=0)
    # wide_df 添加一列，名为 31，值为0
    wide_df['31.0'] = 0
    
    wide_df.to_csv('/src/data/zk/a51+af51+3.csv',index=True)
    
def debug2():
    df = pd.read_csv('/src/data/zk/a51+af51.csv')
    df = df[['media','install_date','cv','installs']]

    cvMap = getCvMap()
    # cvMap 列改名 conversion_value 改为cv
    cvMap.rename(columns={'conversion_value':'cv'}, inplace=True)
    cvMap['avg'] = (cvMap['min_event_revenue'] + cvMap['max_event_revenue']) / 2
    cvMap = cvMap[['cv','avg']]

    df = df.merge(cvMap, on='cv')
    df['r1usd'] = df['installs'] * df['avg']
    df = df.groupby(['media','install_date']).agg({'r1usd':'sum'}).reset_index()
    df.to_csv('/src/data/zk/a51+af51+4.csv',index=False)
    

if __name__ == '__main__':
    
    # main()
    # ret()
    # debug1()
    debug2()
    

    