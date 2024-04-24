# skan翻译尝试
# 利用原始skan进行翻译
# 先翻译media，再尝试翻译campaign
import io
import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql

# 获得skan原始数据
# 翻译media
# https://support.appsflyer.com/hc/en-us/articles/360012640377-SKAN-integrated-partners-list#apple-skadnetwork-ids-of-ad-networks

# 获取一段时间的skan原始数据，按照app_id,day,media,cv分组
def getSKANRawData(startDayStr, endDayStr):

    filename = f'/src/data/skanRawData_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    app_id,
    day,
    skad_ad_network_id,
    skad_conversion_value as cv,
    count(*) as count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between '{startDayStr}' and '{endDayStr}'
    and app_id in ('6448786147','id6450953550','id1479198816','1614358511')
group by
    app_id,
    day,
    skad_ad_network_id,
    skad_conversion_value
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename, dtype={'day': str})
    return df

# 获取一段时间的skan af数据，按照app_id,day,media,cv分组
def getSKANAFData(startDayStr, endDayStr):

    filename = f'/src/data/skanAFData_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    app_id,
    day,
    media_source as media,
    skad_conversion_value as cv,
    count(*) as count
from
    ods_platform_appsflyer_skad_details
where
    day between '{startDayStr}' and '{endDayStr}'
    and app_id in (
        'id1479198816',
        'id1614358511',
        'id6448786147',
        'id6450953550'
    )
group by
    app_id,
    day,
    media_source,
    skad_conversion_value
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename, dtype={'day': str})
    return df

# 对比skan原始数据进行处理，翻译media
def getSKANRawData2(startDayStr, endDayStr):
    df = getSKANRawData(startDayStr, endDayStr)
    skadMapDf = pd.read_csv('SKAdNetworkIDs.csv')
    df = pd.merge(df,skadMapDf,on='skad_ad_network_id',how='left')
    # 将media为空的填充 unknown
    df['media'] = df['media'].fillna('unknown')
    return df

def getAppNameById(app_id):
    if app_id == '6448786147' or app_id == 'id6448786147':
        return 'lastwar'
    elif app_id == 'id6450953550':
        return 'tophero'
    elif app_id == 'id1479198816':
        return 'topwar'
    elif app_id == '1614358511' or app_id == 'id1614358511':
        return 'buid master'
    else:
        return 'unknown'

# 按照媒体进行逐天比对，应该是有对不齐的地方
def main1():
    startDayStr = '20240101'
    endDayStr = '20240401'

    df1 = getSKANRawData2(startDayStr, endDayStr)
    df2 = getSKANAFData(startDayStr, endDayStr)

    # 将app_id与app名称对应
    df1['app_name'] = df1['app_id'].apply(getAppNameById)
    df2['app_name'] = df2['app_id'].apply(getAppNameById)
    appNameList = df1['app_name'].unique()

    # print(df1['media'].unique())
    # print(df2['media'].unique())

    # 统一media名称
    # df2 中的 Facebook Ads 改为 Facebook，googleadwords_int 改为 Google
    df2['media'] = df2['media'].replace('Facebook Ads','Facebook')
    df2['media'] = df2['media'].replace('googleadwords_int','Google')
    df2['media'] = df2['media'].replace('tiktokglobal_int','bytedanceglobal_int')

    # 对比主要媒体
    mediaList = ['Facebook','Google','applovin_int','bytedanceglobal_int','other']
    # 将在mediaList以外的media归为other
    df1.loc[~df1['media'].isin(mediaList),'media'] = 'other'
    df2.loc[~df2['media'].isin(mediaList),'media'] = 'other'

    for appName in appNameList:
        df1App = df1[df1['app_name']==appName]
        df2App = df2[df2['app_name']==appName]

        print(appName)
        for media in mediaList:
            df1Media = df1App[df1App['media']==media]
            df2Media = df2App[df2App['media']==media]

            # 按照app_id,day进行比对，暂时忽略cv
            df1MediaGroup = df1Media.groupby(['app_name','day']).sum().reset_index()
            df2MediaGroup = df2Media.groupby(['app_name','day']).sum().reset_index()

            # 比对
            df = pd.merge(df1MediaGroup,df2MediaGroup,on=['app_name','day'],how='outer',suffixes=('_raw','_af'))
            df = df.fillna(0)
            df['diff'] = (df['count_raw'] - df['count_af'])/df['count_raw']
            print(media)
            print(df)
            df.to_csv(f'/src/data/20240419_skan_{appName}_{media}.csv', index=False)

            # 
            sumRaw = df['count_raw'].sum()
            sumAf = df['count_af'].sum()
            print('sum raw:',sumRaw)
            print('sum af:',sumAf)
            print('sum diff:',(sumRaw-sumAf)/sumRaw)
        print('-----------------------------------')

# 找到媒体翻译失败的条目
def debug():
    startDayStr = '20240101'
    endDayStr = '20240401'

    df1 = getSKANRawData2(startDayStr, endDayStr)

    df = df1[df1['media']=='unknown']
    print(df['skad_ad_network_id'].unique())


    # 去af查找对应的media

    sql = f'''
select
    distinct media_source as media,
    skad_ad_network_id
from
    ods_platform_appsflyer_skad_details
where
    day between '{startDayStr}' and '{endDayStr}'
    and skad_ad_network_id in ({','.join(["'" + str(x) + "'" for x in df['skad_ad_network_id'].unique()])})
;
    '''
    print(sql)
    df = execSql(sql)
    print(df)
    df.to_csv('/src/data/20240419_skan_unknown.csv', index=False)


# 按照媒体进行整体汇总比对，应该可以比逐天对的更齐


if __name__ == '__main__':
    # main1()

    debug()