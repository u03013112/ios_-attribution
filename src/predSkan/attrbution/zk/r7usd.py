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

def getIOSDateFromMC():
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
            app_id = 'id1479198816'
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
    df.to_csv(getFilename('iOS_20230101_20230401'),index=False)
    return df

# iOS不能用AF数据，因为AF不能有效归因，只能用SKAN数据了
# 使用skan数据就不能很好的计算r1usd了，因为中间多次更换cv
# 但是为了有个参考值，可以用一套cvMap来计算r1usd，只用来做横向对比

def getSKANDataFromMC():
    sql = '''
        SELECT
            media_source as media,
            skad_conversion_value as cv,
            install_date
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '20230101' AND '20230415'
            AND app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
            AND skad_conversion_value > 0
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('skan_20230101_20230401'),index=False)
    return df


# 安卓数据中不同媒体人数与媒体首日收入（r1usd）金额分布于iOS有较大差异，需要对安卓数据中的部分媒体进行欠采样处理
# 使得安卓的媒体分布与iOS的媒体分布相似（前4名媒体的人数与收入占比，过小的媒体忽略不计）
# 为了过程可控，需要在过程中先将安卓和iOS的前4名媒体人数、收入占比计算出来并打印到终端
# 然后再进行欠采样，再把欠采样之后的安卓前4名媒体人数、收入占比计算出来并打印到终端
# 最后保存安卓欠采样之后的结果到csv
def sampleAndroidData():
    def calculate_top_media_ratios(df, use_revenue=True):
        top_media = df['media'].value_counts().nlargest(4).index.tolist()
        top_media_data = df[df['media'].isin(top_media)]

        user_count = top_media_data['media'].value_counts()
        user_ratio = user_count / user_count.sum()

        if use_revenue:
            revenue_sum = top_media_data.groupby('media')['r1usd'].sum()
            revenue_ratio = revenue_sum / revenue_sum.sum()
            return pd.DataFrame({'user_ratio': user_ratio, 'revenue_ratio': revenue_ratio})
        else:
            return pd.DataFrame({'user_ratio': user_ratio})

    android_df = pd.read_csv(getFilename('android_20230101_20230401'))
    ios_df = pd.read_csv(getFilename('skan_20230101_20230401'))

    android_ratios = calculate_top_media_ratios(android_df)
    ios_ratios = calculate_top_media_ratios(ios_df, use_revenue=False)

    print("Android top 4 media ratios before undersampling:")
    print(android_ratios)
    print("\niOS top 4 media ratios:")
    print(ios_ratios)

    sampled_android_df = pd.DataFrame()
    for i, android_media in enumerate(android_ratios.index):
        ios_media = ios_ratios.index[i]
        target_ratio = ios_ratios.loc[ios_media, 'user_ratio'] / android_ratios.loc[android_media, 'user_ratio']
        media_data = android_df[android_df['media'] == android_media]
        sampled_media_data = media_data.sample(frac=target_ratio, replace=True)
        sampled_android_df = sampled_android_df.append(sampled_media_data)

    sampled_android_ratios = calculate_top_media_ratios(sampled_android_df)

    print("\nAndroid top 4 media ratios after undersampling:")
    print(sampled_android_ratios)

    sampled_android_df.to_csv(getFilename('sampled_android_20230101_20230401'), index=False)

def androidVsSKAN():
    def calculate_media_ratios(df):
        user_count = df['media'].value_counts()
        user_ratio = user_count / user_count.sum()
        return pd.DataFrame({'user_ratio': user_ratio})

    android_df = pd.read_csv(getFilename('android_20230101_20230401'))
    ios_df = pd.read_csv(getFilename('skan_20230101_20230401'))

    # 过滤安卓数据，仅保留首日付费金额大于 0 的用户
    android_df = android_df[android_df['r1usd'] > 0]

    android_ratios = calculate_media_ratios(android_df)
    ios_ratios = calculate_media_ratios(ios_df)

    # 按照付费用户比率对安卓和 iOS 数据进行降序排列
    android_ratios = android_ratios.sort_values(by='user_ratio', ascending=False)
    ios_ratios = ios_ratios.sort_values(by='user_ratio', ascending=False)

    print("Android media and first-day paid user ratios:")
    print(android_ratios)
    print("\niOS media and first-day paid user ratios:")
    print(ios_ratios)

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

def process_usd(usd, usdp, output_filename):
    df = loadData()
    levels = makeLevels1(df, N=64, usd=usd)
    cvMapDf = makeCvMap(levels)
    cvMapDf = cvMapFixAvg1(df, cvMapDf, usd=usd)
    tmpDf = addCv(df, cvMapDf, usd=usd, usdp=usdp)
    df = df.merge(tmpDf, how='left', on='uid')

    df['cvGroup'] = (df['cv'] // 22).astype(int)

    # media_grouped_data = df.groupby(['media', 'cvGroup'])['r7usd'].agg(['mean', 'median', 'std']).reset_index()
    media_grouped_data = df.groupby(['media', 'cvGroup'])['r7usd'].agg(['mean', 'std', lambda x: x.quantile(0.25), lambda x: x.quantile(0.5), lambda x: x.quantile(0.75), lambda x: x.skew(), lambda x: x.kurtosis()]).reset_index()
    media_grouped_data.columns = ['media', 'cvGroup', 'mean', 'std', 'Q1', 'Q2', 'Q3', 'skew', 'kurtosis']

    correlation_data = []

    for media in ('googleadwords_int', 'bytedanceglobal_int', 'Facebook Ads', 'snapchat_int'):
        media_data = df.loc[df['media'] == media]
        correlation = media_data[[usdp, 'r7usd']].corr().iloc[0, 1]
        correlation_data.append({'media': media, 'correlation': correlation})

    correlation_df = pd.DataFrame(correlation_data)

    # total_grouped_data = df.groupby('cvGroup')['r7usd'].agg(['mean', 'median', 'std']).reset_index()
    total_grouped_data = df.groupby('cvGroup')['r7usd'].agg(['mean', 'std', lambda x: x.quantile(0.25), lambda x: x.quantile(0.5), lambda x: x.quantile(0.75), lambda x: x.skew(), lambda x: x.kurtosis()]).reset_index()
    total_grouped_data.columns = ['cvGroup', 'mean', 'std', 'Q1', 'Q2', 'Q3', 'skew', 'kurtosis']
    total_grouped_data['media'] = 'total'

    total_correlation = df[[usdp, 'r7usd']].corr().iloc[0, 1]
    total_correlation_data = {'media': 'total', 'correlation': total_correlation}

    media_grouped_data = media_grouped_data.append(total_grouped_data, ignore_index=True)
    correlation_df = correlation_df.append(total_correlation_data, ignore_index=True)

    media_grouped_data = media_grouped_data.merge(correlation_df, on='media', how='left')

    # media_grouped_data 排序，按照cvGroup,media排序
    media_grouped_data = media_grouped_data.sort_values(by=['cvGroup', 'media'], ascending=[True, True])
    media_grouped_data.to_csv(getFilename(output_filename), index=False)

def r1usd():
    process_usd('r1usd', 'r1usdp', 'androidR1usd16')

def r2usd():
    process_usd('r2usd', 'r2usdp', 'androidR2usd16')

def r3usd():
    process_usd('r3usd', 'r3usdp', 'androidR3usd16')

def analysis():
    for input, output in (('androidR1usd16', 'androidR1usd16Analysis'), ('androidR2usd16', 'androidR2usd16Analysis'), ('androidR3usd16', 'androidR3usd16Analysis')):
        df = pd.read_csv(getFilename(input))
        media_list = df['media'].unique()
        cvGroup_list = df['cvGroup'].unique()

        comparison_data = []

        def calculate_difference_and_percentage(value1, value2):
            absolute_difference = value1 - value2
            relative_percentage = absolute_difference / value2
            return absolute_difference, relative_percentage

        for cvGroup in cvGroup_list:
            total_data = df.loc[(df['media'] == 'total') & (df['cvGroup'] == cvGroup)]
            total_mean, total_std, total_Q1, total_Q2, total_Q3, total_skew, total_kurt = total_data[['mean', 'std', 'Q1', 'Q2', 'Q3', 'skew', 'kurtosis']].values[0]

            for media in media_list:
                if media == 'total':
                    continue

                media_data = df.loc[(df['media'] == media) & (df['cvGroup'] == cvGroup)]
                media_mean, media_std, media_Q1, media_Q2, media_Q3, media_skew, media_kurt = media_data[['mean', 'std', 'Q1', 'Q2', 'Q3', 'skew', 'kurtosis']].values[0]

                mean_difference, mean_percentage = calculate_difference_and_percentage(media_mean, total_mean)
                std_difference, std_percentage = calculate_difference_and_percentage(media_std, total_std)
                Q1_difference, Q1_percentage = calculate_difference_and_percentage(media_Q1, total_Q1)
                Q2_difference, Q2_percentage = calculate_difference_and_percentage(media_Q2, total_Q2)
                Q3_difference, Q3_percentage = calculate_difference_and_percentage(media_Q3, total_Q3)
                skew_difference, skew_percentage = calculate_difference_and_percentage(media_skew, total_skew)
                kurt_difference, kurt_percentage = calculate_difference_and_percentage(media_kurt, total_kurt)

                comparison_data.append({
                    'cvGroup': cvGroup,
                    'media': media,
                    'mean_percentage': mean_percentage,
                    'std_percentage': std_percentage,
                    'Q1_percentage': Q1_percentage,
                    'Q2_percentage': Q2_percentage,
                    'Q3_percentage': Q3_percentage,
                    'skew_percentage': skew_percentage,
                    'kurt_percentage': kurt_percentage,
                })
        comparison_df = pd.DataFrame(comparison_data)
        comparison_df = comparison_df.round(4)

        comparison_df.to_csv(getFilename(output), index=False)

if __name__ == '__main__':
    # getDataFromMC()    
    # getIOSDateFromMC()
    # getSKANDataFromMC()
    # sampleAndroidData()
    # androidVsSKAN()

    r1usd()
    r2usd()
    r3usd()
    analysis()
    

