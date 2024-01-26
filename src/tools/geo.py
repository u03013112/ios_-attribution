# 分国家

# 目标KPI周期收入占比
# 比如360日回本，那么就是360日收入的分国家占比
# 找到占比比较大的国家，这些国家可能可以单独投放
# 只有这些目前回收接足够大的国家有足够高的天花板，收入过少的国家即时目前付费能力很高，也可能不能支撑投放加量


# CPI的分布
# 将CPI接近的国家进行分组
# 可能还要考虑CPI与回收增长能力的一同分组
# 甚至直接用ROI来分组也是很直接的
# 即CPI+ROI分组，可能需要用到kmeans

# 验收标准制定
# CPI、ROI7、ROI360的方差或者标准差
# 以月为单位，进行ROI360P = ROI7 * mean(ROI360/ROI7)的方式进行ROI360的预测，再计算预测值与实际值的差异MAPE

import os
import pandas as pd
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj
from src.report.media import getIOSMediaGroup01

def getRevenueDataIOSGroupByGeo(startDayStr,endDayStr):
    filename = '/src/data/revenue365_%s_%s_GroupByGeo.csv'%(startDayStr,endDayStr)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')

    sql = f'''
        select
            substring(install_day, 1, 6) AS install_date,
            country as country_code,
            sum(revenue_d7) as revenue_d7,
            sum(revenue_d30) as revenue_d30,
            sum(revenue_d60) as revenue_d60,
            sum(revenue_d360) as revenue_d360
        from dwb_overseas_revenue_allday_afattribution_realtime
        where
            app = 102
            and zone = 0
            and window_cycle = 9999
            and app_package = 'id1479198816'
            and install_day between '{startDayStr}'and '{endDayStr}'
        group by
            install_date,
            country_code
        ;
    '''
    print(sql)
    df = execSql(sql)

    df.to_csv(filename,index=False)
    print('存储在%s'%filename)
    return df

def getRevenueDataAndroidGroupByGeo(startDayStr,endDayStr):
    filename = '/src/data/revenue365_Android_%s_%s_GroupByGeo.csv'%(startDayStr,endDayStr)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')

    sql = f'''
        select
            substring(install_day, 1, 6) AS install_date,
            country as country_code,
            sum(revenue_d7) as revenue_d7,
            sum(revenue_d30) as revenue_d30,
            sum(revenue_d60) as revenue_d60,
            sum(revenue_d360) as revenue_d360
        from dwb_overseas_revenue_allday_afattribution_realtime
        where
            app = 102
            and zone = 0
            and window_cycle = 9999
            and app_package = 'com.topwar.gp'
            and install_day between '{startDayStr}'and '{endDayStr}'
        group by
            install_date,
            country_code
        ;
    '''
    print(sql)
    df = execSql(sql)

    df.to_csv(filename,index=False)
    print('存储在%s'%filename)
    return df

def getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr):
    filename = '/src/data/adData_%s_%s_GroupByGeo.csv'%(startDayStr,endDayStr)

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')

    sql = f'''
        SELECT
            substring(install_day, 1, 6) AS install_date,
            mediasource,
            country as country_code,
            SUM(cost_value_usd) as cost,
            SUM(install) as install
        FROM
            (
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd,
                    ad_install as install
                FROM
                    rg_bi.dwd_overseas_cost_new
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
                    AND window_cycle = 9999
                    AND facebook_segment in ('country', 'N/A')
                    AND install_day >= '20230101'
                UNION ALL
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd,
                    ad_install as install
                FROM
                    rg_bi.dwd_overseas_cost_history
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
                    AND facebook_segment in ('country', 'N/A')
                    AND install_day < '20230101'
            ) AS ct
        WHERE
            ct.install_day BETWEEN '{startDayStr}'
            AND '{endDayStr}'
        GROUP BY
            install_date,
            mediasource,
            country;
    '''

    print(sql)
    adCostDf = execSql(sql)
    print('已获得%d条数据'%len(adCostDf))
    
    # 这是为了去掉tiktokglobal_int，不知道为啥，用新表之后应该不需要了
    adCostDf = adCostDf.loc[adCostDf.mediasource != 'tiktokglobal_int']

    mediaGroupList = getIOSMediaGroup01()
    adCostDf['media'] = 'other'
    for mediaGroup in mediaGroupList:
        adCostDf.loc[adCostDf.mediasource.isin(mediaGroup['codeList']),'media'] = mediaGroup['name']
    
    adCostDf = adCostDf.groupby(
        ['install_date','country_code','media']
        ,as_index=False
    ).agg(
        {    
            'cost':'sum',
            'install':'sum'
        }
    ).reset_index(drop=True)

    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    # adCostDf['campaign_id'] = adCostDf['campaign_id'].astype(str)
    return adCostDf

def getAdDataAndroidGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr):
    filename = '/src/data/adData_Android_%s_%s_GroupByGeo.csv'%(startDayStr,endDayStr)

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')

    sql = f'''
        SELECT
            substring(install_day, 1, 6) AS install_date,
            mediasource,
            country as country_code,
            SUM(cost_value_usd) as cost,
            SUM(install) as install
        FROM
            (
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd,
                    ad_install as install
                FROM
                    rg_bi.dwd_overseas_cost_new
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'com.topwar.gp'
                    AND cost_value_usd > 0
                    AND window_cycle = 9999
                    AND facebook_segment in ('country', 'N/A')
                    AND install_day >= '20230101'
                UNION ALL
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd,
                    ad_install as install
                FROM
                    rg_bi.dwd_overseas_cost_history
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'com.topwar.gp'
                    AND cost_value_usd > 0
                    AND facebook_segment in ('country', 'N/A')
                    AND install_day < '20230101'
            ) AS ct
        WHERE
            ct.install_day BETWEEN '{startDayStr}'
            AND '{endDayStr}'
        GROUP BY
            install_date,
            mediasource,
            country;
    '''

    print(sql)
    adCostDf = execSql(sql)
    print('已获得%d条数据'%len(adCostDf))
    
    # 这是为了去掉tiktokglobal_int，不知道为啥，用新表之后应该不需要了
    adCostDf = adCostDf.loc[adCostDf.mediasource != 'tiktokglobal_int']

    mediaGroupList = getIOSMediaGroup01()
    adCostDf['media'] = 'other'
    for mediaGroup in mediaGroupList:
        adCostDf.loc[adCostDf.mediasource.isin(mediaGroup['codeList']),'media'] = mediaGroup['name']
    
    adCostDf = adCostDf.groupby(
        ['install_date','country_code','media']
        ,as_index=False
    ).agg(
        {    
            'cost':'sum',
            'install':'sum'
        }
    ).reset_index(drop=True)

    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    # adCostDf['campaign_id'] = adCostDf['campaign_id'].astype(str)
    return adCostDf



# 验算，df是df = revenueDf.merge(adDf,on=['install_date','country_code'],how='left')
# retList是类似[{'cluster':0,'countryList':['US','JP']},{'cluster':1,'countryList':['CN','IN']}]
# 计算每个国家按月的预测360ROI，然后计算MAPE
def check(df,retList,revenue_d7='revenue_d7',revenue_d360='revenue_d360'):
    df = df.loc[df['install_date'] < '20230101']
    df = df.loc[(df['cost'] > 1) & (df['install'] > 1)]

    # 先给df添加cluster列
    df['cluster'] = -1
    for ret in retList:
        cluster = ret['cluster']
        countryList = ret['countryList']
        df.loc[df['country_code'].isin(countryList),'cluster'] = cluster

    # 计算每个cluster的整体revenue_d360/revenue_d7
    dfG1 = df.groupby(['cluster'],as_index=False).agg({revenue_d7:'sum',revenue_d360:'sum'}).reset_index(drop=True)
    dfG1['r360/r7'] = dfG1[revenue_d360]/dfG1[revenue_d7]
    dfG1 = dfG1[['cluster','r360/r7']]
    print(dfG1)

    df = df.merge(dfG1,on=['cluster'],how='left')
    df['revenue_d360P'] = df[revenue_d7] * df['r360/r7']
    
    dfG2 = df.groupby(['install_date'],as_index=False).agg({revenue_d360:'sum','revenue_d360P':'sum'}).reset_index(drop=True)
    dfG2['mape'] = abs(dfG2[revenue_d360] - dfG2['revenue_d360P']) / dfG2[revenue_d360]

    # print(dfG2)
    return dfG2['mape'].mean()

def check2(df,retList,revenue_d7='revenue_d7',revenue_d360='revenue_d360'):
    df = df.loc[(df['cost'] > 1) & (df['install'] > 1)].copy(deep=True)  
    df['cluster'] = -1
    for ret in retList:
        cluster = ret['cluster']
        countryList = ret['countryList']
        df.loc[df['country_code'].isin(countryList),'cluster'] = cluster

    df = df.groupby(['install_date','cluster'],as_index=False).agg({revenue_d7:'sum',revenue_d360:'sum'}).reset_index(drop=True)

    trainDf = df.loc[(df['install_date'] >= '202201')&(df['install_date'] <= '202206')]
    testDf = df.loc[(df['install_date'] >= '202207')&(df['install_date'] <= '202212')].copy(deep=True)   

    # print('trainDf:',trainDf)
    # print('testDf:',testDf)

    trainDf = trainDf.groupby(['cluster'],as_index=False).agg({revenue_d7:'sum',revenue_d360:'sum'}).reset_index(drop=True)
    trainDf['r360/r7'] = trainDf[revenue_d360]/trainDf[revenue_d7]
    print(trainDf[['cluster','r360/r7']])

    testDf = testDf.groupby(['cluster'],as_index=False).agg({revenue_d7:'sum',revenue_d360:'sum'}).reset_index(drop=True)
    testDf['p'] = testDf[revenue_d7] * trainDf['r360/r7']

    mape = abs(testDf[revenue_d360].sum() - testDf['p'].sum()) / testDf[revenue_d360].sum()

    print('mape:',mape)


    

def k1(dfRaw,cols,N=4):
    df = dfRaw.copy(deep=True)
    data = df[cols].values
    kmeans = KMeans(n_clusters=N).fit(data)
    # 获取每个数据点的簇标签
    labels = kmeans.labels_
    df['cluster'] = labels

    retList = []
    clusterList = df['cluster'].unique().tolist()
    for cluster in clusterList:
        countryList = df.loc[df['cluster'] == cluster,'country_code'].tolist()
        ret = {
            'cluster':cluster,
            'countryList':countryList
        }
        retList.append(ret)

    return retList

def printRetList(retList):
    # 先按照cluster排序
    retList = sorted(retList,key=lambda x:x['cluster'])
    for ret in retList:
        print(ret['cluster'],':',ret['countryList'])

from sklearn.cluster import KMeans
def main():
    startDayStr = '20210101'
    endDayStr = '20231231'
    revenueDf = getRevenueDataIOSGroupByGeo(startDayStr,endDayStr)
    adDf = getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr)
    adDf = adDf.groupby(['install_date','country_code'],as_index=False).sum().reset_index(drop=True)

    df = revenueDf.merge(adDf,on=['install_date','country_code'],how='left')
    df2 = df.copy(deep=True)

    # 验算用，经过验算，发现没有问题
    # installDateGroupDf = df.groupby(['install_date'],as_index=False).sum().reset_index(drop=True)
    # installDateGroupDf['roi7'] = installDateGroupDf['revenue_d7']/installDateGroupDf['cost']
    # installDateGroupDf['roi360'] = installDateGroupDf['revenue_d360']/installDateGroupDf['cost']
    # installDateGroupDf = installDateGroupDf[['install_date','cost','roi7','roi360']]
    # print(installDateGroupDf)

    # 为了获得360日收入，需要获取至少一年前的数据
    # df = df.loc[df['install_date'] < '20230101']

    df = df.loc[(df['install_date'] >= '202201')&(df['install_date'] <= '202206')]
    df = df.groupby(['country_code'],as_index=False).sum().reset_index(drop=True)
    # df = df.loc[(df['cost'] > 18000) & (df['install'] > 1800)]
    df = df.loc[(df['cost'] > 18000) & (df['install'] > 1800)]

    df['roi7'] = df['revenue_d7']/df['cost']
    df['roi30'] = df['revenue_d30']/df['cost']
    df['roi360'] = df['revenue_d360']/df['cost']
    df['cpi'] = df['cost']/df['install']
    df = df.sort_values(by=['cost'],ascending=False).reset_index(drop=True)

    df['costRate'] = df['cost']/df['cost'].sum()

    df = df[['country_code','costRate','roi7','roi30','roi360','cpi','revenue_d360','revenue_d7','revenue_d30']]
    # print(df)

    # 使用k-means算法对数据进行聚类，分为N个簇
    N = 10
    
    revenue_d7 = 'revenue_d7'

    df['roi360/roi30'] = df['revenue_d360']/df[revenue_d7]
    retList = k1(df,['roi360/roi30'],N)
    printRetList(retList)
    mape = check2(df2,retList,revenue_d7=revenue_d7)
    print('mape:',mape)

    retList2 = [
        {
            'cluster':0,
            'countryList':['US']
        },{
            'cluster':1,
            'countryList':['JP']
        },{
            'cluster':2,
            'countryList':['KR']
        },{
            'cluster':3,
            'countryList':['SA','AE','KW','QA','OM','BH']
        }
    ]
    printRetList(retList2)
    mape = check2(df2,retList2,revenue_d7=revenue_d7)
    print('mape:',mape)

def main2():
    startDayStr = '20210101'
    endDayStr = '20231231'
    revenueDf = getRevenueDataIOSGroupByGeo(startDayStr,endDayStr)
    adDf = getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr)
    adDf = adDf.groupby(['install_date','country_code'],as_index=False).sum().reset_index(drop=True)

    df = revenueDf.merge(adDf,on=['install_date','country_code'],how='left')
    df2 = df.copy(deep=True)

    # 为了获得360日收入，需要获取至少一年前的数据
    df = df.loc[df['install_date'] < '20230101']
    df = df.groupby(['country_code'],as_index=False).sum().reset_index(drop=True)
    df = df.loc[(df['cost'] > 1000) & (df['install'] > 1000)]


    df['roi7'] = df['revenue_d7']/df['cost']
    df['roi360'] = df['revenue_d360']/df['cost']
    df['cpi'] = df['cost']/df['install']
    df = df.sort_values(by=['cost'],ascending=False).reset_index(drop=True)

    df['costRate'] = df['cost']/df['cost'].sum()

    df['r360/r7'] = df['revenue_d360']/df['revenue_d7']
    df = df[['country_code','costRate','r360/r7','cpi']]
    # print(df)

    # 尝试进行分组
    data = df[['cpi','r360/r7']].values
    # 使用k-means算法对数据进行聚类，分为N个簇
    N = 3
    kmeans = KMeans(n_clusters=N, random_state=0).fit(data)
    # 获取每个数据点的簇标签
    labels = kmeans.labels_

    # 将簇标签添加到原始DataFrame中
    df['cluster'] = labels

    retList = []

    clusterList = df['cluster'].unique().tolist()
    for cluster in clusterList:
        countryList = df.loc[df['cluster'] == cluster,'country_code'].tolist()
        ret = {
            'cluster':cluster,
            'countryList':countryList
        }
        retList.append(ret)

    print(retList)

    # 验算
    mape = check(df2,retList)
    print('mape:',mape)


# 潜力国家发现，源自https://rivergame.feishu.cn/wiki/QcJYwxZKmiY31vkDwklcbYu5nrd
# 将潜力国家数据在topwar中验证
# 1、他们在topwar中花费排名
# 2、他们的 ROI7 和 ROI360 以及 R360/R7    
def f1(countryList):
    startDayStr = '20230101'
    endDayStr = '20231231'
    revenueDf = getRevenueDataAndroidGroupByGeo(startDayStr,endDayStr)
    adDf = getAdDataAndroidGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr)
    adDf = adDf.groupby(['install_date','country_code'],as_index=False).sum().reset_index(drop=True)

    df = revenueDf.merge(adDf,on=['install_date','country_code'],how='left')
    
    df = df.groupby(['country_code'],as_index=False).sum().reset_index(drop=True)

    df['cost rate'] = df['cost']/df['cost'].sum()

    # Initialize a dictionary to store country indices
    country_indices = {country: {} for country in countryList}

    # Sort by cost and store indices
    df = df.sort_values(by=['cost'], ascending=False).reset_index(drop=True)
    print(df.head(10))
    for country in countryList:
        index = df.index[df['country_code'] == country].tolist()[0]
        country_indices[country]['cost_index'] = index

    # Sort by ROI360 and store indices
    df['roi360'] = df['revenue_d360'] / df['cost']
    df = df.sort_values(by=['roi360'], ascending=False).reset_index(drop=True)
    for country in countryList:
        index = df.index[df['country_code'] == country].tolist()[0]
        country_indices[country]['roi360_index'] = index

    # Sort by revenue_d360/revenue_d7 and store indices
    df['r360/r7'] = df['revenue_d360'] / df['revenue_d7']
    df = df.sort_values(by=['r360/r7'], ascending=False).reset_index(drop=True)
    for country in countryList:
        index = df.index[df['country_code'] == country].tolist()[0]
        country_indices[country]['r360/r7_index'] = index

    # Merge indices into a single DataFrame
    indices_df = pd.DataFrame.from_dict(country_indices, orient='index')

    # Merge 'roi360' column into the DataFrame
    roi360_df = df[['country_code', 'roi360']].set_index('country_code')
    indices_df = indices_df.join(roi360_df)

    indices_df.rename(columns={
        'cost_index': '花费排名',
        'roi360_index': 'roi360排名',
        'r360/r7_index': 'r360/r7排名'
    }, inplace=True)

    print(indices_df)


if __name__ == '__main__':
    # main()
    f1(['RU','IT','UA','EG','CH', 'ES', 'AT', 'BR', 'BE'])