# 用与验证iOS撞库结论
# 读取广告信息，并计算撞库获得的7日ROI
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads',
    'snapchat_int',
    'applovin_int'
]

# 获得广告花费 2023-03-01~2023-04-20
def getAdCost():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(cost) as cost
        from
        (
            select
                day,
                mediasource,
                getapppackagev2(
                    app,
                    mediasource,
                    campaign_name,
                    adset_name,
                    ad_name
                ) as app_package,
                campaign_name,
                adset_name,
                ad_name,
                cost
            from
                ods_realtime_mediasource_cost
            where
                app = 102
                and day >= 20230301
                and day < 20230420
                and mediasource in (
                    'bytedanceglobal_int',
                    'googleadwords_int',
                    'FacebookAds'
                )
        )
        where
            app_package = 'id1479198816'
        group by
            mediasource,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def getSKANDataFromMC():
    sql = '''
        SELECT
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp
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
            AND day BETWEEN '20230301' AND '20230423'
            AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('20230301', 'yyyyMMdd') AND to_date('20230415', 'yyyyMMdd')
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
    userDf = userDf[userDf['cv'] > 0]
    skanDf = skanDf[skanDf['cv'] > 0]

    # 2. 使用pandas的向量化操作处理skanDf
    for media in mediaList:
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
        userMediaDf = userMediaDf.groupby(['install_date']).agg({'r7usd':'sum'}).reset_index()
        userMediaDf['media'] = media
        retDf = retDf.append(userMediaDf)

    return retDf



def main():
    # skanDf = getSKANDataFromMC()
    # skanDf.to_csv('/src/data/zk/skan0.csv', index=False)
    # skanDf = pd.read_csv('/src/data/zk/skan0.csv')
    # skanDf = skanAddValidInstallDate(skanDf)
    # skanDf.to_csv('/src/data/zk/skan.csv', index=False)
    # userDf = makeUserDf()
    # userDf.to_csv('/src/data/zk/user.csv', index=False)
    # skanDf = pd.read_csv('/src/data/zk/skan.csv')
    # userDf = pd.read_csv('/src/data/zk/user.csv')

    # attDf = attribution1(userDf,skanDf)
    # attDf.to_csv('/src/data/zk/att.csv', index=False)

    attDf = pd.read_csv('/src/data/zk/att.csv')
    resultDf = result1(attDf)
    resultDf.to_csv('/src/data/zk/result.csv', index=False)

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
    # main()

    # getR1Usd()
    # fp1Df = pd.read_csv('/src/data/zk/result.csv')
    # df = ckeckFp1(fp1Df)
    # df.to_csv('/src/data/zk/ck1.csv', index=False)

    df = pd.read_csv('/src/data/zk/ck1.csv')
    draw(df)