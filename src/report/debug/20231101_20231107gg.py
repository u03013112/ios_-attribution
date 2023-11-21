# 尝试回溯融合归因结论
# 用来证明海外iOS Google的7日ROI效果差
# 暂时先用特例时间段来找到方法，后面可能可以尝试用更多的时间段来验证
# 目前的现象是Google和字节的24小时ROI类似，但是7日ROI表现有差异。
# 另外Facebook的24小时和7日ROI表现都很好，可能也需要验证一下。

# 目前发现Google的问题是JP的ROI特别差。所以看看是否可以将这部分的融合归因追溯出来。

# 大体思路
# 1、将所有JP用户找到，找到这些用户对应的融合归因结论，即他们被分给了那些媒体的哪些campaign
# 2、按媒体和campaign进行汇总
# 3、查看这些campaign的SKAN表现，按照道理他们的SKAN表现就很差
# 4、然后还可以再看看这些campaign对应的Cv分布，按照道理Google的Cv分布也应该很差

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%('/src/data/report/debug',filename,ext)

def debugStep1():
    df = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/revenue20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str})
    jpDf = df.loc[
        (df['install_date'] >= '20231022')
        &(df['install_date'] <='20231031') 
        & (df['geoGroup']=='JP')
        ]
    jpDf = jpDf.groupby(['media','campaign_id','campaign_name']).sum().reset_index()

    bytedanceDf = jpDf.loc[jpDf['media']=='bytedanceglobal'].copy()
    googleDf = jpDf.loc[jpDf['media']=='google'].copy()

    bytedanceRevenue24hSum = bytedanceDf['revenue_24h'].sum()
    bytedanceDf['revenue_24h rate'] = bytedanceDf['revenue_24h']/bytedanceRevenue24hSum
    bytedanceDf = bytedanceDf.sort_values(by='revenue_24h rate',ascending=False).reset_index(drop=True)
    print(bytedanceDf[['campaign_name','campaign_id','revenue_24h rate']].head(10))

    googleRevenue24hSum = googleDf['revenue_24h'].sum()
    googleDf['revenue_24h rate'] = googleDf['revenue_24h']/googleRevenue24hSum
    googleDf = googleDf.sort_values(by='revenue_24h rate',ascending=False).reset_index(drop=True)
    print(googleDf[['campaign_name','campaign_id','revenue_24h rate']].head(10))

def debugStep2():
    df = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/adData20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str,'campaign_id':str})
    jpDf = df.loc[
        (df['install_date'] >= '20231022')
        &(df['install_date'] <='20231031') 
        & (df['geoGroup']=='JP')
        ]
    jpDf = jpDf.groupby(['media','campaign_id','campaign_name']).sum().reset_index()

    bytedanceDf = jpDf.loc[jpDf['media']=='bytedanceglobal'].copy()
    googleDf = jpDf.loc[jpDf['media']=='google'].copy()

    bytedanceCostSum = bytedanceDf['cost'].sum()
    bytedanceDf['cost rate'] = bytedanceDf['cost']/bytedanceCostSum
    bytedanceDf = bytedanceDf.sort_values(by='cost rate',ascending=False).reset_index(drop=True)
    print(bytedanceDf[['campaign_name','campaign_id','cost rate']].head(10))

    googleCostSum = googleDf['cost'].sum()
    googleDf['cost rate'] = googleDf['cost']/googleCostSum
    googleDf = googleDf.sort_values(by='cost rate',ascending=False).reset_index(drop=True)
    print(googleDf[['campaign_name','campaign_id','cost rate']].head(10))

def debugStep3():
    df = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/adData20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str})
    jpDf = df.loc[
        (df['install_date'] >= '20231022')
        &(df['install_date'] <='20231031') 
        # & (df['geoGroup']=='JP')
        ]
    jpDf = jpDf.groupby(['media','campaign_id','campaign_name','geoGroup']).sum().reset_index()

    print(jpDf[jpDf['campaign_id']==20568467238])

def debugStep4():
    df = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/revenue20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str})
    jpDf = df.loc[
        (df['install_date'] >= '20231022')
        &(df['install_date'] <= '20231031')
        # & (df['geoGroup']=='JP')
        &(df['campaign_id'] == '20568467238')
        ]
    # jpDf = jpDf.groupby(['media','campaign_id','campaign_name']).sum().reset_index()

    # print(jpDf[jpDf['campaign_id']=='20568467238'])
    # print(jpDf)
    jpDf = jpDf.groupby('geoGroup').sum().reset_index()
    revenue_24hSum = jpDf['revenue_24h'].sum()
    jpDf['revenue_24h rate'] = jpDf['revenue_24h']/revenue_24hSum
    print(jpDf)

def debug():
    revenueDf = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/revenue20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str})

    revenueDf2 = revenueDf.loc[revenueDf['campaign_id']=='20529517126'].groupby('geoGroup').sum().reset_index()
    revenueDf2_24hSum = revenueDf2['revenue_24h'].sum()

    revenueDf2['revenue_24h rate'] = revenueDf2['revenue_24h']/revenueDf2_24hSum
    print(revenueDf2)

    adDataDf = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/adData20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str,'campaign_id':str})
    adDataDf2 = adDataDf.loc[adDataDf['campaign_id']=='20529517126'].groupby('geoGroup').sum().reset_index()
    adDataDf2_costSum = adDataDf2['cost'].sum()

    adDataDf2['cost rate'] = adDataDf2['cost']/adDataDf2_costSum
    print(adDataDf2)

def debug2():
    revenueDf = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/revenue20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str})
    revenueDf = revenueDf.loc[
        (revenueDf['install_date'] >= '20231022')
        &(revenueDf['install_date'] <='20231031')
    ]
    # revenueDf2 = revenueDf.loc[revenueDf['campaign_id']=='20568467238'].reset_index()
    revenueDf2 = revenueDf.loc[(revenueDf['geoGroup']=='JP') & (revenueDf['media']=='google')].reset_index()
    revenueDf2 = revenueDf2.groupby('campaign_id').sum().reset_index()

    adDataDf = pd.read_csv('/src/data/report/iOSWeekly20231101_20231110/adData20231022_20231110_GroupByCampaignAndGeoAndMedia.csv',dtype={'install_date':str,'campaign_id':str})
    adDataDf = adDataDf.loc[
        (adDataDf['install_date'] >= '20231022')
        &(adDataDf['install_date'] <='20231031')
    ]
    # adDataDf2 = adDataDf.loc[adDataDf['campaign_id']=='20568467238'].reset_index()
    adDataDf2 = adDataDf.loc[(adDataDf['geoGroup']=='JP') & (adDataDf['media']=='google')].reset_index()
    adDataDf2 = adDataDf2.groupby('campaign_id').sum().reset_index()

    # revenue = revenueDf2['revenue_24h'].sum()
    # cost = adDataDf2['cost'].sum()
    # print(revenue,cost,revenue/cost)

    df = pd.merge(revenueDf2,adDataDf2,on='campaign_id',how='outer')
    df['roi'] = df['revenue_24h']/df['cost']
    print(df)





if __name__ == '__main__':
    # debugStep1()
    # debugStep2()
    debugStep3()
    # debugStep4()

    # debug()
    # debug2()