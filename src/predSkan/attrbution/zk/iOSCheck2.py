# 与iOSCheck.py的区别是，这里只计算2023-03-01~2023-03-31的数据
# 并且只计算r7usd，并不计算roi

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk/%s.%s'%(filename,ext)

mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads',
    'snapchat_int',
    'applovin_int',
]

def getSKANDataFromMC():
    sql = '''
        SELECT
            event_uuid,
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp,
            install_date,
            skad_revenue,
            min_revenue,
            max_revenue
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '20230401' AND '20230513'
            AND app_id = 'id1479198816'
            AND event_name = 'af_skad_revenue'
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
            ) as install_date
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN '20230401' AND '20230513'
            AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('20230401', 'yyyyMMdd') AND to_date('20230506', 'yyyyMMdd')
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


def addCv2(df):
    # 3月cvMap
    cvMapDf = pd.read_csv('/src/afCvMap2303.csv')
    cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvMapDf3 = cvMapDf.copy()

    # 4月cvMap
    cvMapDf = pd.read_csv('/src/afCvMap2404.csv')
    cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvMapDf4 = cvMapDf.copy()

    # 
    df.loc[:,'cv'] = 0
    
    for index, row in cvMapDf.iterrows():
        df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']),'cv'] = row['conversion_value']
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(),'cv'] = cvMapDf['conversion_value'].max()
    return df

    return 

# 制作待归因用户Df
def makeUserDf():
    df = getAfDataFromMC()
    # 是否要处理IDFA数据？
    # 如果要处理应该怎么处理？
    # 暂时放弃处理IDFA，相信SSOT
    cvMapDf = getCvMap()
    userDf = addCv(df,cvMapDf)

    userDf = userDf[['appsflyer_id','install_timestamp','r1usd','r7usd','cv']]
    return userDf

def attribution1(userDf, skanDf):
    # 1. 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
    for media in mediaList:
        userDf[media + ' count'] = 0

    userDf['install_timestamp'] = pd.to_datetime(userDf['install_timestamp'], unit='s')

    # 将cv == 0 的暂时去掉，不做归因，这个数量太大了
    userDf = userDf[userDf['cv'] > 0].copy()
    skanDf = skanDf[skanDf['cv'] > 0].copy()

    # 2. 使用pandas的向量化操作处理skanDf
    for media in mediaList:
        print(f"Processing {media}")
        unmatched_rows = 0
        unmatched_user_count = 0
        media_rows = skanDf[skanDf['media'] == media]
        for _, row in media_rows.iterrows():
            cv = row['cv']
            min_valid_install_timestamp = row['min_valid_install_timestamp']
            max_valid_install_timestamp = row['max_valid_install_timestamp']

            condition = (
                (userDf['cv'] == cv) &
                (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                (userDf['install_timestamp'] <= max_valid_install_timestamp)
            )
            matching_rows_count = condition.sum()
            if matching_rows_count > 0:
                userDf.loc[condition, media + ' count'] += 1 / matching_rows_count
            else:
                # print(f"Unmatched row: {row}")
                unmatched_rows += 1
                unmatched_user_count += 1

        unmatched_ratio = unmatched_rows / len(media_rows)
        print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")

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

def result1(userDf):
    # 归因后数据
    # 转化安装日期，精确到天
    userDf.loc[:,'install_date'] = pd.to_datetime(userDf['install_timestamp']).dt.date
    for media in mediaList:
        userDf.loc[:,media+' r1usd'] = userDf[media+' count'] * userDf['r1usd']
        userDf.loc[:,media+' r7usd'] = userDf[media+' count'] * userDf['r7usd']
    
    userDf = userDf.groupby(['install_date']).agg('sum').reset_index()
    
    retDf = pd.DataFrame(columns=['media','install_date','r7usd'])

    for media in mediaList:
        userMediaDf = userDf[['install_date',media+' r1usd',media+' r7usd']]
        # userMediaDf列重命名
        userMediaDf = userMediaDf.rename(columns={media+' r1usd':'r1usd',media+' r7usd':'r7usd'})
        # userMediaDf.to_csv('/src/data/zk/%s_%s_result'%(media,message),index=False)
        userMediaDf = userMediaDf.groupby(['install_date']).agg({'r1usd':'sum','r7usd':'sum'}).reset_index()
        userMediaDf['media'] = media
        retDf = retDf.append(userMediaDf)

    return retDf

def meanAttribution(userDf, skanDf):
    userDf['attribute'] = [list() for _ in range(len(userDf))]
    unmatched_rows = 0
    unmatched_user_count = 0
    unmatched_revenue = 0
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
                unmatched_revenue += row['skad_revenue']
                
    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_user_count_ratio = unmatched_user_count / skanDf['user_count'].sum()
    unmatched_revenue_ratio = unmatched_revenue / skanDf['skad_revenue'].sum()

    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched user count ratio: {unmatched_user_count_ratio:.2%}")
    print(f"Unmatched revenue ratio: {unmatched_revenue_ratio:.2%}")

    unmatched_rows_df = pd.DataFrame(unmatched_rows_data)
    unmatched_rows_df.to_csv(getFilename('unmatched_rows'), index=False)

    userDf.to_csv(getFilename('attribution1ReStep0503'), index=False)
    userDf.to_parquet(getFilename('attribution1ReStep0503','parquet'), index=False)
    return userDf

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

    for index, row in skanDf.iterrows():
        attribution_process(index, row)

    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_ratio_re = unmatched_rows_re / len(skanDf)
    unmatched_revenue_ratio = unmatched_revenue / skanDf['skad_revenue'].sum()
    unmatched_revenue_ratio_re = unmatched_revenue_re / skanDf['skad_revenue'].sum()

    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched revenue ratio: {unmatched_revenue_ratio:.2%}")
    print(f"Unmatched rows ratio re: {unmatched_ratio_re:.2%}")
    print(f"Unmatched revenue ratio re: {unmatched_revenue_ratio_re:.2%}")

    userDf.to_csv(getFilename('attribution2Re0504'), index=False)
    userDf.to_parquet(getFilename('attribution2Re0504','parquet'), index=False)

    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
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
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]

    # Drop unnecessary columns
    userDf = userDf.drop(columns=['install_timestamp', 'cv', 'user_count', 'r7usd'] + [media + ' count' for media in mediaList])

    # Melt the DataFrame to have 'media' and 'r7usd' in separate rows
    userDf = userDf.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf['media'] = userDf['media'].str.replace(' r7usd', '')

    # Group by 'media' and 'install_date' and calculate the sum of 'r7usd'
    userDf = userDf.groupby(['media', 'install_date']).agg({'r7usd': 'sum'}).reset_index()
    userDf.rename(columns={'r7usd': 'r7usdp'}, inplace=True)

    # Save to CSV
    userDf.to_csv(getFilename('attribution1Ret0503'), index=False)
    return userDf

def result2(userDf):
    userDf.loc[:,'install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    userDf = userDf.groupby(['install_date','media']).agg({'r7usd':'sum'}).reset_index()
    userDf.rename(columns={'r7usd': 'r7usdp'}, inplace=True)

    # Save to CSV
    userDf.to_csv(getFilename('attribution2Ret0504'), index=False)
    return userDf
    


def main():
    # skanDf = getSKANDataFromMC()
    # skanDf.to_csv('/src/data/zk/skan0.csv', index=False)
    skanDf = pd.read_csv('/src/data/zk/skan0.csv')
    skanDf = skanAddValidInstallDate(skanDf)
    skanDf.to_csv('/src/data/zk/skan.csv', index=False)
    # userDf = makeUserDf()
    # userDf.to_csv('/src/data/zk/user.csv', index=False)
    skanDf = pd.read_csv('/src/data/zk/skan.csv')
    userDf = pd.read_csv('/src/data/zk/user.csv')
    # skanDf 中cv>31的数据，cv-=31
    skanDf.loc[skanDf['cv']>31,'cv'] -= 31
    skanDf = skanDf.loc[
        (skanDf['postback_timestamp'] >= '2023-03-01') &
        (skanDf['postback_timestamp'] < '2023-03-24')
        # (skanDf['postback_timestamp'] < '2023-04-01')
    ]

    # 转格式，将格式转成androidFpNewDebug中一致格式，方便直接使用该方法
    # 先将skanDf中的min_valid_install_timestamp和max_valid_install_timestamp由原来的‘2023-03-03 01:01:15’类似格式，转成unix时间戳，单位秒
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['min_valid_install_timestamp']).astype(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = pd.to_datetime(skanDf['max_valid_install_timestamp']).astype(np.int64) // 10 ** 9

    # 再将skanDf与userDf都按照10分钟进行汇总
    N = 600
    userDf['install_timestamp'] = userDf['install_timestamp'] // N * N
    userGroupbyDf = userDf.groupby(['install_timestamp','cv']).agg({'appsflyer_id':'count','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'appsflyer_id':'user_count'}, inplace=True)

    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // N * N
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // N * N
    skanDf['user_count'] = 1
    # skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
    #     {'user_count': 'sum','skad_revenue': 'sum'}
    # ).reset_index()
    skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp']).agg(
        {'user_count': 'sum', 'skad_revenue': 'sum', 'event_uuid': lambda x: ','.join(x)}
    ).reset_index()

    userDf = meanAttribution(userGroupbyDf, skanGroupbyDf)
    userDf = pd.read_parquet(getFilename('attribution1ReStep0503','parquet'))
    userDf = meanAttributionResult(userDf)
    userDf = pd.read_csv(getFilename('attribution1Ret0503'))

    

def main2():
    skanDf = pd.read_csv('/src/data/zk/skan.csv')
    userDf = pd.read_csv('/src/data/zk/user.csv')
    # skanDf 中cv>31的数据，cv-=31
    skanDf.loc[skanDf['cv']>31,'cv'] -= 31
    skanDf = skanDf.loc[skanDf['postback_timestamp'] < '2023-04-01']

    # 转格式，将格式转成androidFpNewDebug中一致格式，方便直接使用该方法
    # 先将skanDf中的min_valid_install_timestamp和max_valid_install_timestamp由原来的‘2023-03-03 01:01:15’类似格式，转成unix时间戳，单位秒
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['min_valid_install_timestamp']).astype(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = pd.to_datetime(skanDf['max_valid_install_timestamp']).astype(np.int64) // 10 ** 9

    attribution2Re(userDf,skanDf)
    

def debug2():
    # df = getAfDataFromMC()
    # df.to_csv('/src/data/zk/af.csv', index=False)


    df = pd.read_csv('/src/data/zk/af.csv')
    df['install_timestamp2'] = pd.to_datetime(df['install_timestamp'], unit='s').astype(str)
    # print(df.head())
    df = df.loc[
        (df['r1usd'] > 0) &
        # (df['r1usd'] <= 0.1085) &
        (df['install_timestamp2'] >= '2023-03-15 15:22:48') &
        (df['install_timestamp2'] <= '2023-03-17 15:22:48')
    ]

    # userDf = pd.read_csv('/src/data/zk/user.csv')
    # userDf['install_timestamp'] = pd.to_datetime(userDf['install_timestamp'], unit='s')
    # # print(userDf.head())
    # df = userDf.loc[
    #     # (userDf['cv'] == 1) &
    #     (userDf['install_timestamp'] >= '2023-03-15 15:22:48') &
    #     (userDf['install_timestamp'] <= '2023-03-17 15:22:48')
    # ]
    print(df)

def debug():
    df = pd.read_csv('/src/data/zk/skan0.csv')
    # 按照条件汇总，并计算汇总条目
    df['user count'] = 1
    # install_date 是字符串，类似‘2023-03-05 22:15:43’，缩短为‘2023-03-05’
    df['install_date'] = pd.to_datetime(df['install_date']).dt.date.astype(str)
    df = df.fillna(0)
    df = df.groupby(['media','cv','install_date','skad_revenue']).agg({'user count':'sum'}).reset_index()
    df['r1usd'] = (df['skad_revenue']) * df['user count']
    df = df.groupby(['media','install_date']).agg({'r1usd':'sum'}).reset_index()
    ssotDf = pd.read_csv('/src/ssot.csv')
    df['Date'] = pd.to_datetime(df['install_date']).dt.strftime('%Y/%-m/%-d').astype(str)
    df.drop('install_date', axis=1, inplace=True)
    


    # 将ssotDf中的'Media_source'列重命名为'media'
    ssotDf.rename(columns={'Media_source': 'media'}, inplace=True)

    # 按'Date'和'media'列合并两个数据框
    merged_df = pd.merge(df, ssotDf, on=['Date', 'media'],how='right')

    merged_df['Cost'] = merged_df['Cost'].str.replace(',', '').astype(float)
    
    merged_df['roi1'] = merged_df['r1usd'] / merged_df['Cost']
    # 'SKAN ROI（Real）' 列中的百分号去掉，转换为float类型，如果转换失败，将该行的值设为0
    merged_df['roi real'] = merged_df['SKAN ROI（Real）'].str.replace('%', '')
    merged_df['roi real'] = pd.to_numeric(merged_df['roi real'], errors='coerce')/100
    merged_df['roi real'] = merged_df['roi real'].fillna(0)
    
    merged_df['MAPE'] = abs(merged_df['roi1'] - merged_df['roi real']) / merged_df['roi real']

    print(merged_df['MAPE'].mean())

    # 如果需要，您可以将合并后的数据框保存到CSV文件中
    merged_df.to_csv('/src/data/zk/merged_result.csv', index=False)

    return merged_df

import numpy as np
def debug3():
    # df = pd.read_csv('/src/data/zk/skanAOS2GD.csv')
    # # 将appsflyer_id列丢掉
    # df.drop('appsflyer_id', axis=1, inplace=True)
    # print(df.head())

    df = pd.read_csv('/src/data/zk/skan.csv')
    # 目前的min_valid_install_timestamp是‘2023-03-03 01:01:15’类似格式，转成unix时间戳，单位秒
    df['min_valid_install_timestamp'] = pd.to_datetime(df['min_valid_install_timestamp']).astype(np.int64) // 10**9
    df['max_valid_install_timestamp'] = pd.to_datetime(df['max_valid_install_timestamp']).astype(np.int64) // 10**9

    df = df[['media','cv','min_valid_install_timestamp','max_valid_install_timestamp']]

    print(df.head())

from sklearn.metrics import r2_score
def ret():
    # resultDf = pd.read_csv('/src/data/zk/result.csv')
    resultDf = pd.read_csv('/src/data/zk/attribution1Ret0503.csv')
    # resultDf = pd.read_csv('/src/data/zk/attribution2Ret0504.csv')
    # resultDf 列r7usdp改名 r7usd
    resultDf.rename(columns={'r7usdp': 'r7usd'}, inplace=True)
    ssotDf = pd.read_csv('/src/ssot.csv')

    # ssotDf 列 ‘Date’  内容类似 ‘2023/3/1’
    # resultDf 列'install_date' 内容类似 ‘2023-03-01’
    # 将resultDf 列'install_date' 内容转换为 ‘2023/3/1’，列名改为Date
    # ssotDf 列'Media_source'改名为'media'
    # 合并两个df，按照Date,media进行合并。

    # 转换resultDf中的'install_date'列格式，并重命名为'Date'
    resultDf['Date'] = pd.to_datetime(resultDf['install_date']).dt.strftime('%Y/%-m/%-d')
    resultDf.drop('install_date', axis=1, inplace=True)

    # 将ssotDf中的'Media_source'列重命名为'media'
    ssotDf.rename(columns={'Media_source': 'media'}, inplace=True)

    # 按'Date'和'media'列合并两个数据框
    merged_df = pd.merge(resultDf, ssotDf, on=['Date', 'media'],how='right')

    merged_df['Cost'] = merged_df['Cost'].str.replace(',', '').astype(float)
    
    # merged_df['roi1'] = merged_df['r1usd'] / merged_df['Cost']
    merged_df['roi7'] = merged_df['r7usd'] / merged_df['Cost']

    # 添加一列‘MAPE’ ，计算'roi7'和'ROAS(D7)预测'的MAPE
    merged_df['ROAS(D7)预测'] = merged_df['ROAS(D7)预测'].str.replace('%', '').astype(float) / 100
    merged_df['MAPE'] = abs(merged_df['roi7'] - merged_df['ROAS(D7)预测']) / merged_df['ROAS(D7)预测']

    # 筛选所有merged_df['ROAS(D7)预测']不为空并且不为0的行
    merged_df = merged_df[merged_df['ROAS(D7)预测'].notnull() & (merged_df['ROAS(D7)预测'] != 0)]
    print(merged_df['MAPE'].mean())

    # 分媒体统计MAPE
    print(merged_df.groupby('media')['MAPE'].mean())
    # 过滤所有MAPE不为空的行
    merged_df = merged_df[merged_df['MAPE'].notnull()]

    media_r2_scores = merged_df.groupby('media').apply(lambda x: r2_score(x['roi7'], x['ROAS(D7)预测']))
    print(media_r2_scores)

    # 如果需要，您可以将合并后的数据框保存到CSV文件中
    merged_df.to_csv('/src/data/zk/merged_result.csv', index=False)
    
    # 分媒体进行绘图，每个媒体一张图，列‘Date’为x轴，‘roi7’和‘ROAS(D7)预测’为y轴
    # 将merged_df中‘ROAS(D7)预测’列改名为‘ssot’
    merged_df.rename(columns={'ROAS(D7)预测': 'ssot'}, inplace=True)

    for media in merged_df['media'].unique():
        df = merged_df[merged_df['media'] == media]
        df.plot(x='Date', y=['roi7', 'ssot'], title=media)
        # plt.show()
        # 图片保存
        plt.savefig('/src/data/zk/vsSsot{}.png'.format(media))

    return merged_df
    


# 获得skan数据，按照AF的安装日期进行汇总，并获得首日收入金额的汇总
def getSKANAFInstallDateFromMC():
    sql = '''
        SELECT
            media_source as media,
            skad_conversion_value as cv,
            install_date
        FROM
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '20230301' AND '20230415'
            AND app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
            AND media_source in (
                'bytedanceglobal_int',
                'googleadwords_int',
                'Facebook Ads'
            )
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df


def getR1Usd():
    df = getSKANAFInstallDateFromMC()
    df.to_csv('/src/data/zk/r1usd0.csv', index=False)
    df = pd.read_csv('/src/data/zk/r1usd0.csv')

    cvMap = getCvMap()
    print(cvMap)

    df['r1usd'] = 0
    for index, row in cvMap.iterrows():
        cv = row['conversion_value']
        min_event_revenue = row['min_event_revenue']
        max_event_revenue = row['max_event_revenue']
        avg = (min_event_revenue + max_event_revenue) / 2

        df.loc[df['cv'] == cv, 'r1usd'] = avg

    df = df.groupby(['media', 'install_date']).agg({'r1usd': 'sum'}).reset_index()
    df.to_csv('/src/data/zk/r1usd.csv', index=False)
    return df


def ckeckFp1(fp1Df):
    # adCostDf = getAdCost()
    # 将adCostDf中media列中的‘FacebookAds’替换为‘Facebook Ads’
    # adCostDf.loc[:,'media'] = adCostDf['media'].str.replace('FacebookAds','Facebook Ads')
    # adCostDf.to_csv('/src/data/zk/iOSAdCost.csv', index=False)
    adCostDf = pd.read_csv('/src/data/zk/iOSAdCost.csv')

    r1usdDf = pd.read_csv('/src/data/zk/r1usd.csv')

    # fp1Df = pd.read_csv('/src/data/zk/result.csv')
    df = pd.merge(adCostDf, fp1Df, on=['media', 'install_date'], how='left')
    df = pd.merge(df, r1usdDf, on=['media', 'install_date'], how='left')
    df['roi1'] = df['r1usd'] / df['cost']
    df['roi'] = df['r7usd'] / df['cost']
    df['r7/r1'] = df['r7usd'] / df['r1usd']

    df = df.loc[df['install_date']<'2023-04-15']
    df = df.sort_values(by=['media','install_date'], ascending=True)
    return df

import matplotlib.pyplot as plt
def draw(ck1Df):
    # media,install_date,cost,r7usd,r1usd,roi1,roi,r7/r1
    # 图画的宽一些，plt.figure(figsize=(12, 6))即可
    # 用install_date为x轴，roi为y轴，画出每个media的roi折线图
    # 其中install每隔10天取一个点即可
    # 保存为/src/data/zk/ck1.jpg
    # 再画一个install_date为x轴，r7/r1为y轴的折线图
    # 保存为/src/data/zk/ck2.jpg


    plt.figure(figsize=(12, 6))
    ck1Df['install_date'] = pd.to_datetime(ck1Df['install_date'])
    
    # 对install_date进行降采样，每隔10天取一个点
    ck1Df = ck1Df[ck1Df['install_date'].dt.day % 10 == 0]
    
    # 对数据按media分组
    grouped = ck1Df.groupby('media')
    
    # 为每个media绘制ROI折线图
    for media, group in grouped:
        plt.plot(group['install_date'], group['roi'], label=media)

    # 设置图表标题和坐标轴标签
    plt.title('ROI by Media and Install Date')
    plt.xlabel('Install Date')
    plt.ylabel('ROI')

    # 显示图例
    plt.legend()

    # 显示图表
    # plt.show()
    plt.savefig("/src/data/zk/ck1.jpg")

    plt.figure(figsize=(12, 6))
    for media in ck1Df['media'].unique():
        media_data = ck1Df[ck1Df['media'] == media]
        plt.plot(media_data['install_date'], media_data['r7/r1'], label=media)

    plt.xlabel('Install Date')
    plt.ylabel('R7/R1 Ratio')
    plt.legend()
    plt.savefig('/src/data/zk/ck2.jpg')

if __name__ == '__main__':
    main()
    # debug()
    # debug2()
    
    # debug3()

    # main2()
    # result2(pd.read_csv(getFilename('attribution2Re0504')))
    ret()
    

    # getR1Usd()
    # fp1Df = pd.read_csv('/src/data/zk/result.csv')
    # df = ckeckFp1(fp1Df)
    # df.to_csv('/src/data/zk/ck1.csv', index=False)

    # df = pd.read_csv('/src/data/zk/ck1.csv')
    # draw(df)