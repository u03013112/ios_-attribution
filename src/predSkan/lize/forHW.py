# 慧文的需求
# 不同媒体的CV增长率是否不同，或者不同媒体的CV分布式又有所不同,12月~2月28日。9月到11月底。

# 首先是不同媒体的CV分布，针对安卓与iOS分别进行分析
# 时间分为两个部分，9月1日到11月30日，和12月1日到2月28日
# 将不容的CV占比放在一张图上

import pandas as pd
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

def getIOSDataFromMC():
    # 从ods_platform_appsflyer_skad_details表中获得
    # 海外iOS用户，安装日期在9月1日~2月28日
    # 按照媒体+安装+cv日期分组，获得每个媒体，每天，每种cv共计多少人（行）

    sql = """
        SELECT
            media_source as media,
            install_date,
            skad_conversion_value as cv,
            COUNT(*) AS user_count
        FROM
            ods_platform_appsflyer_skad_details
        WHERE
            app_id = 'id1479198816'
            AND day BETWEEN '20220901' AND '20230310'
            AND install_date BETWEEN '2022-09-01' AND '2023-02-26'
            AND event_name IN ('af_skad_redownload', 'af_skad_install')
        GROUP BY
            media_source,
            install_date,
            skad_conversion_value
        ;
    """
    print(sql)
    df = execSql(sql)
    return df

import matplotlib.pyplot as plt
def drawIOS():
    # df = getIOSDataFromMC()
    # df.to_csv(getFilename('forHWIos'), index=False)

    df = pd.read_csv(getFilename('forHWIos'))
    # 将media列中的'facebook'替换为'Facebook Ads'
    df.loc[df['media']=='facebook','media'] = 'Facebook Ads'

    # 过滤媒体，只关心 'googleadwords_int','bytedanceglobal_int','applovin_int','Facebook Ads','snapchat_int'
    df = df.loc[df['media'].isin(['googleadwords_int','bytedanceglobal_int','applovin_int','Facebook Ads','snapchat_int'])]

    # 先按照安装日期分为两组9月1日到11月30日，和12月1日到2月28日
    df0 = df.loc[df['install_date']<'20221201']
    df1 = df.loc[df['install_date']>='20221201']

    df0Sum = df0.groupby(['media','cv'])['user_count'].sum().reset_index()
    df1Sum = df1.groupby(['media','cv'])['user_count'].sum().reset_index()

    df0Sum = df0Sum.sort_values(by=['media','cv'])
    df1Sum = df1Sum.sort_values(by=['media','cv'])

    for media in ('googleadwords_int','bytedanceglobal_int','applovin_int','Facebook Ads','snapchat_int'):
        df0Media = df0Sum.loc[df0Sum['media']==media]
        df1Media = df1Sum.loc[df1Sum['media']==media]

        df0Media = df0Media.loc[df0Media['cv']>0]
        df1Media = df1Media.loc[df1Media['cv']>0]

        df0Media['cvp'] = df0Media['user_count'] / df0Media['user_count'].sum()
        df1Media['cvp'] = df1Media['user_count'] / df1Media['user_count'].sum()

        df0Media.to_csv(getFilename('forHWIos0'+media), index=False)
        df1Media.to_csv(getFilename('forHWIos1'+media), index=False)
        
        for i, dfMedia in enumerate([(df0Media, '0'), (df1Media, '1')]):
            plt.figure(figsize=(10, 5))
            plt.bar(dfMedia[0]['cv'], dfMedia[0]['user_count'])
            plt.xlabel('cv')
            plt.ylabel('user_count')
            plt.title(f'{media} - {dfMedia[1]}')
            plt.xticks(range(0, 64))
            plt.savefig(f'/src/data/forHW{media}{dfMedia[1]}.jpg')
            plt.close()

            plt.figure(figsize=(10, 5))
            plt.bar(dfMedia[0]['cv'], dfMedia[0]['cvp'])
            plt.xlabel('cv')
            plt.ylabel('cvp')
            plt.title(f'{media} - {dfMedia[1]} (Percentage)')
            plt.xticks(range(0, 64))
            plt.savefig(f'/src/data/forHW{media}{dfMedia[1]}p.jpg')
            plt.close()


# 分析
def analysis():
    # 读取各媒体cv占比csv
    # 将cv分为低中高3个档位，其中0~10为低，11~40是中，41以上是高
    # 计算每个媒体的低中高占比
    for media in ('googleadwords_int','bytedanceglobal_int','applovin_int','Facebook Ads','snapchat_int'):
        df0Media = pd.read_csv(getFilename('forHWIos0'+media))
        df1Media = pd.read_csv(getFilename('forHWIos1'+media))

        df0MediaLow = df0Media.loc[df0Media['cv']<=10]
        df0MediaMid = df0Media.loc[(df0Media['cv']>10) & (df0Media['cv']<=40)]
        df0MediaHigh = df0Media.loc[df0Media['cv']>40]

        df1MediaLow = df1Media.loc[df1Media['cv']<=10]
        df1MediaMid = df1Media.loc[(df1Media['cv']>10) & (df1Media['cv']<=40)]
        df1MediaHigh = df1Media.loc[df1Media['cv']>40]

        print('{media} 9月1日到11月30日 low rate: {lowRate}, mid rate: {midRate}, high rate: {highRate}'.format(
            media=media,
            lowRate=df0MediaLow['user_count'].sum() / df0Media['user_count'].sum(),
            midRate=df0MediaMid['user_count'].sum() / df0Media['user_count'].sum(),
            highRate=df0MediaHigh['user_count'].sum() / df0Media['user_count'].sum()
        ))

        print('{media} 12月1日到2月28日 low rate: {lowRate}, mid rate: {midRate}, high rate: {highRate}'.format(
            media=media,
            lowRate=df1MediaLow['user_count'].sum() / df1Media['user_count'].sum(),
            midRate=df1MediaMid['user_count'].sum() / df1Media['user_count'].sum(),
            highRate=df1MediaHigh['user_count'].sum() / df1Media['user_count'].sum()
        ))

if __name__ == '__main__':
    # drawIOS()
    analysis()