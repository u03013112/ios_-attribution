# 分析skan报告中的redownloads分布
# 几个想到的分析：
# 1、不同的app，不同的时间，redownloads分布有什么不同
# 2、redownload的用户，是否有更高的cv


import io
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

sys.path.append('/src')
from src.maxCompute import execSql

# 6448786147 lw
# id6450953550 th
# id1479198816 tw
# 1614358511 bm

def getAppNameById(app_id):
    if app_id == '6448786147':
        return 'lastwar'
    elif app_id == 'id6450953550':
        return 'tophero'
    elif app_id == 'id1479198816':
        return 'topwar'
    elif app_id == '1614358511':
        return 'buid master'
    else:
        return 'unknown'

def getData(startDayStr = '20231001',endDayStr = '20240401'):
    filename = f'/src/data/redownloads_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    app_id,
    day,
    skad_ad_network_id,
    sum(
        case
            when skad_redownload = 'true' then 1
            else 0
        end
    ) as redownloads_count,
    count(*) as total_count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between '{startDayStr}' and '{endDayStr}'
    and app_id in ('6448786147','id6450953550','id1479198816','1614358511')
group by
    app_id
    ,day
    ,skad_ad_network_id
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 分媒体分析
def main1():
    df = getData()

    # 先将skad_ad_network_id转换成media,列：media,skad_ad_network_id
    skadMapDf = pd.read_csv('SKAdNetworkIDs.csv')

    df = pd.merge(df,skadMapDf,on='skad_ad_network_id',how='left')
    # 将media为空的填充 unknown
    df['media'] = df['media'].fillna('unknown')

    df = df.groupby(['app_id','day','media']).sum().reset_index()
    # day是类似20231001的格式，需要转换成日期格式
    df['day'] = pd.to_datetime(df['day'],format='%Y%m%d')

    dfGroupByAppId = df.groupby(['app_id']).sum().reset_index()
    dfGroupByAppId['redoadloads_rate'] = dfGroupByAppId['redownloads_count'] / dfGroupByAppId['total_count']
    print(dfGroupByAppId)

    dfGroupByAppIdAndDay = df.groupby(['app_id','day']).sum().reset_index()
    dfGroupByAppIdAndDay['redoadloads_rate'] = dfGroupByAppIdAndDay['redownloads_count'] / dfGroupByAppIdAndDay['total_count']
    # 画图，将day作为x轴，redownloads_rate作为y轴，每个app_id画一条线
    # 获取所有app_id
    app_ids = dfGroupByAppIdAndDay['app_id'].unique()
    plt.subplots(figsize=(16, 6))
    # 为每个app_id绘制一条线
    for app_id in app_ids:
        appName = getAppNameById(app_id)
        app_data = dfGroupByAppIdAndDay[dfGroupByAppIdAndDay['app_id'] == app_id]
        plt.plot(app_data['day'], app_data['redoadloads_rate'], label=appName)

    plt.legend()
    plt.xlabel('Day')
    plt.ylabel('Redownloads Rate')
    plt.savefig('/src/data/redownloads1.png')
    plt.clf()

    # 分app id，然后分media
    for app_id in app_ids:
        appName = getAppNameById(app_id)
        appDf = df[df['app_id'] == app_id].copy()
        # 画图，每个media画一条线
        plt.subplots(figsize=(16, 6))
        # medias = appDf['media'].unique()
        # 这里要对media进行一定的过滤，因为有一些media的数据量太小，不具有代表性
        # 如果media不属于'Facebook','Google','applovin_int','bytedanceglobal_int'，剩下的统称为（重命名）other，然后合并other
        medias = ['Facebook','Google','applovin_int','bytedanceglobal_int','other']
        appDf['media'] = appDf['media'].apply(lambda x: x if x in medias else 'other')
        appDf = appDf.groupby(['day','media']).sum().reset_index()

        for media in medias:
            media_data = appDf[appDf['media'] == media].copy()
            media_data['redoadloads_rate'] = media_data['redownloads_count'] / media_data['total_count']
            plt.plot(media_data['day'], media_data['redoadloads_rate'], label=media)

        plt.legend()
        plt.xlabel('Day')
        plt.ylabel('Redownloads Rate')
        plt.savefig(f'/src/data/redownloads2_{appName}.png')
        plt.clf()

# 按照下载量分析
def main2():
    df = getData()

    df = df.groupby(['app_id','day']).sum().reset_index()
    df['redoadloads_rate'] = df['redownloads_count'] / df['total_count']
    df['day'] = pd.to_datetime(df['day'],format='%Y%m%d')

    app_ids = df['app_id'].unique()
    
    for app_id in app_ids:
        fig, ax1 = plt.subplots(figsize=(16, 6))
        appName = getAppNameById(app_id)
        app_data = df[df['app_id'] == app_id]

        ax2 = ax1.twinx()  # 创建第二个y轴
        ax1.plot(app_data['day'], app_data['redoadloads_rate'], 'g-')
        ax2.plot(app_data['day'], app_data['total_count'], 'b-')

        ax1.set_xlabel('Day')
        ax1.set_ylabel('Redownloads Rate', color='g')
        ax2.set_ylabel('Downloads Count', color='b')

        plt.savefig(f'/src/data/redownloads3_{appName}.png')
        plt.clf()

# 获得每个app的redownloads分布,与cv分布
def getData2(startDayStr = '20231001',endDayStr = '20240401'):
    filename1 = f'/src/data/redownloads2_total_{startDayStr}_{endDayStr}.csv'
    filename2 = f'/src/data/redownloads2_redownloads_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename1):
        sql1 = f'''
select
    app_id,
    day,
    skad_conversion_value as cv,
    count(*) as total_count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between '{startDayStr}'
    and '{endDayStr}'
    and app_id in (
        '6448786147',
        'id6450953550',
        'id1479198816',
        '1614358511'
    )
group by
    app_id,
    day,
    skad_conversion_value
;
        '''
        print(sql1)
        df1 = execSql(sql1)
        df1.to_csv(filename1, index=False)

        sql2 = f'''
select
    app_id,
    day,
    skad_conversion_value as cv,
    count(*) as redownloads_count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between '{startDayStr}'
    and '{endDayStr}'
    and app_id in (
        '6448786147',
        'id6450953550',
        'id1479198816',
        '1614358511'
    )
    and skad_redownload = 'true'
group by
    app_id,
    day,
    skad_conversion_value
;
        '''
        print(sql2)
        df2 = execSql(sql2)
        df2.to_csv(filename2, index=False)

    else:
        print('read from file:',filename1)
        df1 = pd.read_csv(filename1)
        print('read from file:',filename2)
        df2 = pd.read_csv(filename2)

    return df1,df2


def main3():
    df1,df2 = getData2()
    df = pd.merge(df1,df2,on=['app_id','day','cv'],how='left')
    df['cv'] = pd.to_numeric(df['cv'], errors='coerce')
    df.loc[df['cv']>=32,'cv'] -= 32

    print(df)

    # cv分组，0一组，1~10一组，11~20一组，21~31一组
    cvGroupList = [
        {'name':'free','cvList':[0]},
        {'name':'low','cvList':[1,2,3,4,5,6,7,8,9,10]},
        {'name':'mid','cvList':[11,12,13,14,15,16,17,18,19,20]},
        {'name':'high','cvList':[21,22,23,24,25,26,27,28,29,30,31]},
        {'name':'unknown','cvList':[-1]}
    ]

    df['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df.loc[df['cv'].isin(cvGroup['cvList']),'cvGroup'] = cvGroup['name']
    
    dfTest = df[df['cvGroup'] == 'unknown']
    print('test:',dfTest['cv'].unique())

    appIds = df['app_id'].unique()
    for appId in appIds:
        appDf = df[df['app_id'] == appId].copy()
        appName = getAppNameById(appId)
        appGroupDf = appDf.groupby(['cvGroup']).sum().reset_index()
        totalCountSum = appGroupDf['total_count'].sum()
        redownloadsCountSum = appGroupDf['redownloads_count'].sum()
        print('app:',appName)
        for cvGroup in cvGroupList:
            cvGroupName = cvGroup['name']
            cvGroupTotalCountSum = appGroupDf[appGroupDf['cvGroup'] == cvGroupName]['total_count'].sum()
            cvGroupRedownloadsCountSum = appGroupDf[appGroupDf['cvGroup'] == cvGroupName]['redownloads_count'].sum()
            print(f'{cvGroupName} total rate:{cvGroupTotalCountSum/totalCountSum}')
            print(f'{cvGroupName} redownloads rate:{cvGroupRedownloadsCountSum/redownloadsCountSum}')

        
        




if __name__ == '__main__':
    # main1()
    # main2()
    main3()