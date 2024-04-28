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
    AND event_name in (
        'af_skad_install',
        'af_skad_redownload'
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

def getCvMap():
    csv_str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2023-11-28 11:13:03,0,,,
id6448786147,1,af_skad_revenue,0,1,0,0.99,0,24,2023-11-28 11:13:03,0,,,
id6448786147,2,af_skad_revenue,0,1,0.99,1.15,0,24,2023-11-28 11:13:03,0,,,
id6448786147,3,af_skad_revenue,0,1,1.15,1.3,0,24,2023-11-28 11:13:03,0,,,
id6448786147,4,af_skad_revenue,0,1,1.3,2.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,5,af_skad_revenue,0,1,2.98,3.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,6,af_skad_revenue,0,1,3.41,5.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,7,af_skad_revenue,0,1,5.98,7.46,0,24,2023-11-28 11:13:03,0,,,
id6448786147,8,af_skad_revenue,0,1,7.46,9.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,9,af_skad_revenue,0,1,9.09,12.05,0,24,2023-11-28 11:13:03,0,,,
id6448786147,10,af_skad_revenue,0,1,12.05,14.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,11,af_skad_revenue,0,1,14.39,18.17,0,24,2023-11-28 11:13:03,0,,,
id6448786147,12,af_skad_revenue,0,1,18.17,22.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,13,af_skad_revenue,0,1,22.07,26.57,0,24,2023-11-28 11:13:03,0,,,
id6448786147,14,af_skad_revenue,0,1,26.57,32.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,15,af_skad_revenue,0,1,32.09,37.42,0,24,2023-11-28 11:13:03,0,,,
id6448786147,16,af_skad_revenue,0,1,37.42,42.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,17,af_skad_revenue,0,1,42.94,50.34,0,24,2023-11-28 11:13:03,0,,,
id6448786147,18,af_skad_revenue,0,1,50.34,58.56,0,24,2023-11-28 11:13:03,0,,,
id6448786147,19,af_skad_revenue,0,1,58.56,67.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,20,af_skad_revenue,0,1,67.93,80.71,0,24,2023-11-28 11:13:03,0,,,
id6448786147,21,af_skad_revenue,0,1,80.71,100.32,0,24,2023-11-28 11:13:03,0,,,
id6448786147,22,af_skad_revenue,0,1,100.32,116.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,23,af_skad_revenue,0,1,116.94,130.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,24,af_skad_revenue,0,1,130.41,153.76,0,24,2023-11-28 11:13:03,0,,,
id6448786147,25,af_skad_revenue,0,1,153.76,196.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,26,af_skad_revenue,0,1,196.39,235.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,27,af_skad_revenue,0,1,235.93,292.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,28,af_skad_revenue,0,1,292.07,424.48,0,24,2023-11-28 11:13:03,0,,,
id6448786147,29,af_skad_revenue,0,1,424.48,543.77,0,24,2023-11-28 11:13:03,0,,,
id6448786147,30,af_skad_revenue,0,1,543.77,753.61,0,24,2023-11-28 11:13:03,0,,,
id6448786147,31,af_skad_revenue,0,1,753.61,1804,0,24,2023-11-28 11:13:03,0,,,
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf



# 按照媒体进行逐天比对，应该是有对不齐的地方
def main1():
    startDayStr = '20240101'
    endDayStr = '20240425'

    cvMap = getCvMap()
    cvMap.rename(columns={'conversion_value':'cv'}, inplace=True)
    cvMap['avg usd'] = (cvMap['min_event_revenue'] + cvMap['max_event_revenue'])/2
    cvMap = cvMap.fillna(0)
    cvMap = cvMap[['cv','avg usd']]
    df1 = getSKANRawData2(startDayStr, endDayStr)
    df2 = getSKANAFData(startDayStr, endDayStr)

    # 处理SSOT
    df1.loc[df1['cv']>=32,'cv'] -= 32
    df2.loc[df2['cv']>=32,'cv'] -= 32
    
    print(cvMap)

    df1 = pd.merge(df1,cvMap,on='cv',how='left')
    df2 = pd.merge(df2,cvMap,on='cv',how='left')

    df1['usd'] = df1['avg usd'] * df1['count']
    df2['usd'] = df2['avg usd'] * df2['count']

    # 将app_id与app名称对应
    df1['app_name'] = df1['app_id'].apply(getAppNameById)
    df2['app_name'] = df2['app_id'].apply(getAppNameById)
    appNameList = df1['app_name'].unique()

    # print(df1.loc[(df1['app_name'] == 'lastwar') & (df1['day'] == '20240101')])
    # print(df2.loc[(df2['app_name'] == 'lastwar') & (df2['day'] == '20240101')])

    # df1tmp = df1.loc[(df1['app_name'] == 'lastwar') & (df1['day'] == '20240425')]
    # df1tmp = df1tmp.groupby(['cv']).agg({
    #     'count':'sum',
    #     'avg usd':'mean',
    #     'usd':'sum'
    # }).reset_index()
    # print(df1tmp)

    # df2tmp = df2.loc[(df2['app_name'] == 'lastwar') & (df2['day'] == '20240425')]
    # df2tmp = df2tmp.groupby(['cv']).agg({
    #     'count':'sum',
    #     'avg usd':'mean',
    #     'usd':'sum'
    # }).reset_index()
    # print(df2tmp)


    # sumCountRaw = df1tmp['count'].sum()
    # sumCountAf = df2tmp['count'].sum()
    # print('sum count raw:',sumCountRaw)
    # print('sum count af:',sumCountAf)
    # print('sum count diff:',(sumCountRaw-sumCountAf)/sumCountRaw)

    # sumUsdRaw = df1tmp['usd'].sum()
    # sumUsdAf = df2tmp['usd'].sum()
    # print('sum usd raw:',sumUsdRaw)
    # print('sum usd af:',sumUsdAf)
    # print('sum usd diff:',(sumUsdRaw-sumUsdAf)/sumUsdRaw)

    # return




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
            df['usd diff'] = (df['usd_raw'] - df['usd_af'])/df['usd_raw']
            print(media)
            print(df)
            df.to_csv(f'/src/data/20240419_skan_{appName}_{media}.csv', index=False)

            # 
            sumRaw = df['count_raw'].sum()
            sumAf = df['count_af'].sum()
            print('sum raw:',sumRaw)
            print('sum af:',sumAf)
            print('sum diff:',(sumRaw-sumAf)/sumRaw)

            sumUsdRaw = df['usd_raw'].sum()
            sumUsdAf = df['usd_af'].sum()
            print('sum usd raw:',sumUsdRaw)
            print('sum usd af:',sumUsdAf)
            print('sum usd diff:',(sumUsdRaw-sumUsdAf)/sumUsdRaw)
        print('-----------------------------------')

def main2():
    startDayStr = '20240101'
    endDayStr = '20240425'
    
    cvMap = getCvMap()
    cvMap.rename(columns={'conversion_value':'cv'}, inplace=True)
    cvMap['avg usd'] = (cvMap['min_event_revenue'] + cvMap['max_event_revenue'])/2
    cvMap = cvMap.fillna(0)
    cvMap = cvMap[['cv','avg usd']]

    df1 = getSKANRawData2(startDayStr, endDayStr)
    # 处理SSOT
    df1.loc[df1['cv']>=32,'cv'] -= 32

    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]}
    ]

    df1['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df1.loc[df1['cv'].isin(cvGroup['cvList']),'cvGroup'] = cvGroup['name']
    
    df1 = pd.merge(df1,cvMap,on='cv',how='left')
    df1['usd'] = df1['avg usd'] * df1['count']

    df1['app_name'] = df1['app_id'].apply(getAppNameById)

    appNameList = df1['app_name'].unique()


    # 对比主要媒体
    mediaList = ['Facebook','Google','applovin_int','bytedanceglobal_int','twitter','other']
    # 将在mediaList以外的media归为other
    df1.loc[~df1['media'].isin(mediaList),'media'] = 'other'
    
    for appName in appNameList:
        df1App = df1[df1['app_name']==appName]
        if appName != 'lastwar':
            continue
        print(appName)
        for media in mediaList:
            print(media)
            df1Media = df1App[df1App['media']==media]
            df1MediaGroup = df1Media.groupby(['app_name','day','cvGroup']).sum().reset_index()
            # 按天统计，不同cvGroup的count、usd占比
            df1MediaGroup['count_pct'] = df1MediaGroup.groupby(['app_name', 'day'])['count'].transform(lambda x: x / x.sum())
            df1MediaGroup['usd_pct'] = df1MediaGroup.groupby(['app_name', 'day'])['usd'].transform(lambda x: x / x.sum())
            df1MediaGroup = df1MediaGroup[['app_name','day','cvGroup','count','usd','count_pct','usd_pct']]
            # print(df1MediaGroup)
            df1MediaGroup.to_csv(f'/src/data/20240419_skan2_{appName}_{media}.csv', index=False)
            df1MediaGroup2 = df1MediaGroup.groupby(['app_name','cvGroup']).sum().reset_index()
            df1MediaGroup2['count_pct'] = df1MediaGroup2.groupby(['app_name'])['count'].transform(lambda x: x / x.sum())
            print(df1MediaGroup2[['app_name','cvGroup','count_pct']])
            df1MediaGroup2['usd_pct'] = df1MediaGroup2.groupby(['app_name'])['usd'].transform(lambda x: x / x.sum())
            print(df1MediaGroup2[['app_name','cvGroup','usd_pct']])





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
    main2()

    # debug()