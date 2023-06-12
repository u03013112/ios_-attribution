# 对于 mind.md 的代码实现
# 用安卓的数据集来做实验
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int'
]
# 付费率或者指定金额以上的比率是否对r7usd有足够的关联性
def p1():
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    # 过滤，将media_source不属于mediaList的media_source 改写为 'other'
    df.loc[~df['media_source'].isin(mediaList),'media_source'] = 'other'

    
    grouped_df = df.groupby(['install_date', 'media_source'])



    # 使用 agg() 一次性计算付费率、r1usd之和和r7usd之和
    result_df = grouped_df.agg(
        payRate=('r1usd', lambda x: (x > 0).sum() / len(x)),
        r1usd10=('r1usd', lambda x: (x >= 10).sum() / len(x)),
        r1usd20=('r1usd', lambda x: (x >= 20).sum() / len(x)),
        r1usd50=('r1usd', lambda x: (x >= 50).sum() / len(x)),
        r1usd100=('r1usd', lambda x: (x >= 100).sum() / len(x)),
        r1usdSum=('r1usd', 'sum'),
        r7usdSum=('r7usd', 'sum')
    ).reset_index()

    result_df['r7/r1'] = result_df['r7usdSum'] / result_df['r1usdSum']
    result_df['r7-r1'] = result_df['r7usdSum'] - result_df['r1usdSum']

    for media in mediaList:
        print('media: %s'%media)
        mediaDf = result_df.loc[result_df['media_source'] == media]
        print(mediaDf.corr())
        
def getAdData():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
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
                impressions,
                clicks,
                installs,
                cost
            from
                ods_realtime_mediasource_cost
            where
                app = 102
                and day >= 20220101
                and day < 20230331
        )
        where
            app_package = 'com.topwar.gp'
        group by
            mediasource,
            day
        ;
    '''

    print(sql)
    pd_df = execSql(sql)
    return pd_df
        
def p2():
    # adData = getAdData()
    # adData.to_csv('/src/data/customLayer/adData20220101_20230331.csv', index=False)
    adData = pd.read_csv('/src/data/customLayer/adData20220101_20230331.csv')
    # print(adData['media'].unique())
    # adData media列中 'FacebookAds' 改为 'Facebook Ads'
    adData.loc[adData['media'] == 'FacebookAds','media'] = 'Facebook Ads'
    # # 2022-01-01~2023-03-31 
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    # 过滤，将media_source不属于mediaList的media_source 改写为 'other'
    df.loc[~df['media_source'].isin(mediaList),'media_source'] = 'other'

    df = df.groupby(['install_date', 'media_source']).agg({
        'r1usd':'sum',
        'r7usd':'sum'
    }).reset_index()

    df = df.merge(adData, how='left', left_on=['install_date', 'media_source'], right_on=['install_date', 'media'])

    # print(df.head())
    # df drop掉 media 列
    df.drop(columns=['media'], inplace=True)

    # df['cpm'] = df['cost'] / df['impressions'] * 1000
    # df['cpc'] = df['cost'] / df['clicks']
    # df['cpi'] = df['cost'] / df['installs']
    # df['ctr'] = df['clicks'] / df['impressions']
    # df['cvr'] = df['installs'] / df['clicks']

    df['r7/r1'] = df['r7usd'] / df['r1usd']
    df['r7-r1'] = df['r7usd'] - df['r1usd']
    
    df.to_csv('/src/data/zk2/p2.csv', index=False)

    for media in mediaList:
        print('media: %s'%media)
        mediaDf = df.loc[df['media_source'] == media]
        print(mediaDf.corr())
    
import statsmodels.api as sm
from statsmodels.formula.api import ols
def p3():
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    # 过滤，将media_source不属于mediaList的media_source 改写为 'other'
    df.loc[~df['media_source'].isin(mediaList),'media_source'] = 'other'

    # 将 install_date 转换为 datetime 类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 计算星期几
    df['weekday'] = df['install_date'].dt.dayofweek

    # 计算 r7usd/r1usd
    df['r7_r1_ratio'] = df['r7usd'] / df['r1usd']
    df.loc[df['r1usd'] == 0,'r7_r1_ratio'] = 0

    # # 使用方差分析（ANOVA）评估星期几对 r1usd、r7usd 和 r7usd/r1usd 的影响度
    # for column in ['r1usd', 'r7usd', 'r7_r1_ratio']:
    #     model = ols(f'{column} ~ C(weekday)', data=df).fit()
    #     anova_result = sm.stats.anova_lm(model, typ=2)
    #     print(f"ANOVA result for {column}:")
    #     print(anova_result)
    #     print("\n")

    grouped_by_weekday = df.groupby('weekday')
    mean_values = grouped_by_weekday[['r1usd', 'r7usd', 'r7_r1_ratio']].mean()
    print(mean_values)


import pandas as pd
from statsmodels.tsa.stattools import ccf

# 计算特征与目标值之间的互相关性
def compute_cross_correlation(df, media, feature, target):
    media_df = df.loc[df['media_source'] == media]
    cross_correlation = ccf(media_df[feature], media_df[target])
    return cross_correlation

def analyze_cross_correlations(file_path):
    # 读取数据
    df = pd.read_csv(file_path)

    # 媒体列表
    media_list = mediaList

    # 特征列表
    feature_list = ['r1usd', 'impressions', 'clicks', 'installs', 'cost', 'r7/r1', 'r7-r1']

    # 目标值：7日回收金额
    target = 'r7usd'

    # 计算互相关性
    for media in media_list:
        print('Media:', media)
        for feature in feature_list:
            cross_correlation = compute_cross_correlation(df, media, feature, target)
            print(f'Cross-correlation between {feature} and {target}:', cross_correlation)



if __name__ == '__main__':
    # p1()
    # p2()
    # p3()

    analyze_cross_correlations('/src/data/zk2/p2.csv')