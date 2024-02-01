# 市场分析，找到与topwar行情有关数据
import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.sensortower.intel import getCustomeFieldsFilterId,getAndroidTopApp,getAndroidDownloadAndRevenue

def getAndroid2023AdCost(country='US'):
    filename = f'/src/data/android{country}2023_adCost.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_month':str})
    else:
        print('从MC获得数据')

    sql = f'''
        SELECT
            SUBSTRING(install_day, 1, 6) as install_month,
            SUM(cost_value_usd) as cost
        FROM
            (
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd
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
                UNION
                ALL
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd
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
            ct.install_day BETWEEN '20230101'
            AND '20231231'
            AND ct.country = '{country}'
        GROUP BY
            install_month
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(filename,index=False)
    return df

# 榜单靠前的app，下载量与广告花费的相关性
# 结论相关性差
def main():
    adCostDf = getAndroidUS2023AdCost()


    months = [
        {'name':'202301','startDate':'2023-01-01','endDate':'2023-01-31'},
        {'name':'202302','startDate':'2023-02-01','endDate':'2023-02-28'},
        {'name':'202303','startDate':'2023-03-01','endDate':'2023-03-31'},
        {'name':'202304','startDate':'2023-04-01','endDate':'2023-04-30'},
        {'name':'202305','startDate':'2023-05-01','endDate':'2023-05-31'},
        {'name':'202306','startDate':'2023-06-01','endDate':'2023-06-30'},
        {'name':'202307','startDate':'2023-07-01','endDate':'2023-07-31'},
        {'name':'202308','startDate':'2023-08-01','endDate':'2023-08-31'},
        {'name':'202309','startDate':'2023-09-01','endDate':'2023-09-30'},
        {'name':'202310','startDate':'2023-10-01','endDate':'2023-10-31'},
        {'name':'202311','startDate':'2023-11-01','endDate':'2023-11-30'},
        {'name':'202312','startDate':'2023-12-01','endDate':'2023-12-31'},
    ]

    filterAllFreeId = '6009d417241bc16eb8e07e9b'
    filter4XStrategyId = '600a22c0241bc16eb899fd71'

    getAndroidTopAppArgList = [
        {'category':'all','custom_fields_filter_id':filterAllFreeId,'limit':30},
        {'category':'game','custom_fields_filter_id':filterAllFreeId,'limit':30},
        {'category':'game','custom_fields_filter_id':filter4XStrategyId,'limit':30},
    ]

    
    retDf = pd.DataFrame()
    
    for getAndroidTopAppArg in getAndroidTopAppArgList:
        for limit in [1,3,5,10,100,500,1000]:
            df = pd.DataFrame()    
            getAndroidTopAppArg['limit'] = limit

            for month in months:
                getAndroidTopAppArg['startDate'] = month['startDate']
                getAndroidTopAppArg['endDate'] = month['endDate']
                
                monthDf = getAndroidTopApp(**getAndroidTopAppArg)
                monthDf['install_month'] = month['name']
                monthDf['category'] = getAndroidTopAppArg['category']
                monthDf['limit'] = getAndroidTopAppArg['limit']
                df = df.append(monthDf)
        
            df = df.merge(adCostDf,on=['install_month'],how='left')

            corr = df.corr()['downloads']['cost']

            retDf = retDf.append({
                'category':getAndroidTopAppArg['category'],
                'custom_fields_filter_id':getAndroidTopAppArg['custom_fields_filter_id'],
                'limit':getAndroidTopAppArg['limit'],
                'corr':corr
            },ignore_index=True)

            print('category:',getAndroidTopAppArg['category'],'custom_fields_filter_id:',getAndroidTopAppArg['custom_fields_filter_id'],'limit:',getAndroidTopAppArg['limit'],'corr:',corr)

    retDf.to_csv('/src/data/corr2.csv',index=False)
    
# 在榜单中找到相关性大的
def main2():
    for country in ['US','KR','JP']:
    # for country in ['KR']:
        adCostDf = getAndroid2023AdCost(country)
        # adCostDf 列 install_month，原本格式类似 202301，改为 2023-01
        adCostDf['install_month'] = adCostDf['install_month'].apply(lambda x:x[:4]+'-'+x[4:])

        appDf = getAndroidTopApp(category='game',countries=country,custom_fields_filter_id='600a22c0241bc16eb899fd71',limit=300,startDate='2023-01-01',endDate='2023-12-31')
        appIdList = appDf['appId'].unique().tolist()

        retDf = pd.DataFrame()

        for addId in appIdList:
            df = getAndroidDownloadAndRevenue(addId,countries = country,startDate='2023-01-01',endDate='2023-12-31')
            df = df[['date','downloads','revenues']]
            df.rename(columns={
                'date':'install_month',
                'downloads':'sensortower_downloads',
                'revenues':'sensortower_revenues'
            },inplace=True)

            df = df.merge(adCostDf,on=['install_month'],how='left')
            corr = df.corr()['sensortower_downloads']['cost']

            retDf = retDf.append({
                'appId':addId,
                'corr':corr
            },ignore_index=True)

            # print('appId:',addId,'corr:',corr)
            # print(df['sensortower_downloads'].sum())

        retDf = retDf.sort_values(['corr'],ascending=False).reset_index(drop=True)
        retDf.to_csv(f'/src/data/corr3_{country}.csv',index=False)
        
        retDf = pd.read_csv(f'/src/data/corr3_{country}.csv')

        # # 找到corr大于0.8的
        retDf = retDf[retDf['corr']>0.8]
        # 找到前20个
        # retDf = retDf.head(20)
        appIdList = retDf['appId'].unique().tolist()

        print('country:',country,'appIdList:',appIdList)
        
        df0 = pd.DataFrame()

        for addId in appIdList:
            df = getAndroidDownloadAndRevenue(addId,countries = country,startDate='2023-01-01',endDate='2023-12-31')
            df = df[['date','downloads','revenues']]
            df.rename(columns={
                'date':'install_month',
                'downloads':'sensortower_downloads',
                'revenues':'sensortower_revenues'
            },inplace=True)

            # print('appId:',addId)
            # print(df['sensortower_downloads'])
            # 如果downloads中有任意一行是0，就跳过
            if df['sensortower_downloads'].min() == 0:
                continue

            df0 = df0.append(df,ignore_index=True)

        df0 = df0.groupby(['install_month']).agg({'sensortower_downloads':'sum','sensortower_revenues':'sum'}).reset_index()

        df0 = df0.merge(adCostDf,on=['install_month'],how='left')
        corr = df0.corr()['sensortower_downloads']['cost']
        print('corr:',corr)


# 与main2区别是不再过滤相似游戏，而是所有APP
def main3():
    for country in ['US','KR','JP']:
    # for country in ['KR']:
        adCostDf = getAndroid2023AdCost(country)
        # adCostDf 列 install_month，原本格式类似 202301，改为 2023-01
        adCostDf['install_month'] = adCostDf['install_month'].apply(lambda x:x[:4]+'-'+x[4:])

        appDf = getAndroidTopApp(category='all',countries=country,custom_fields_filter_id='6009d417241bc16eb8e07e9b',limit=1000,startDate='2023-01-01',endDate='2023-12-31')
        appIdList = appDf['appId'].unique().tolist()

        retDf = pd.DataFrame()

        for addId in appIdList:
            df = getAndroidDownloadAndRevenue(addId,countries = country,startDate='2023-01-01',endDate='2023-12-31')
            df = df[['date','downloads','revenues']]
            df.rename(columns={
                'date':'install_month',
                'downloads':'sensortower_downloads',
                'revenues':'sensortower_revenues'
            },inplace=True)

            df = df.merge(adCostDf,on=['install_month'],how='left')
            corr = df.corr()['sensortower_downloads']['cost']

            retDf = retDf.append({
                'appId':addId,
                'corr':corr
            },ignore_index=True)

            # print('appId:',addId,'corr:',corr)
            # print(df['sensortower_downloads'].sum())

        retDf = retDf.sort_values(['corr'],ascending=False).reset_index(drop=True)
        retDf.to_csv(f'/src/data/corr3All_{country}.csv',index=False)
        
        retDf = pd.read_csv(f'/src/data/corr3All_{country}.csv')

        # # 找到corr大于0.8的
        retDf = retDf[retDf['corr']>0.8]
        # 找到前20个
        # retDf = retDf.head(20)
        appIdList = retDf['appId'].unique().tolist()

        print('country:',country,'appIdList:',appIdList)
        
        df0 = pd.DataFrame()

        for addId in appIdList:
            df = getAndroidDownloadAndRevenue(addId,countries = country,startDate='2023-01-01',endDate='2023-12-31')
            df = df[['date','downloads','revenues']]
            df.rename(columns={
                'date':'install_month',
                'downloads':'sensortower_downloads',
                'revenues':'sensortower_revenues'
            },inplace=True)

            # print('appId:',addId)
            # print(df['sensortower_downloads'])
            # 如果downloads中有任意一行是0，就跳过
            if df['sensortower_downloads'].min() == 0:
                continue

            df0 = df0.append(df,ignore_index=True)

        df0 = df0.groupby(['install_month']).agg({'sensortower_downloads':'sum','sensortower_revenues':'sum'}).reset_index()

        df0 = df0.merge(adCostDf,on=['install_month'],how='left')
        corr = df0.corr()['sensortower_downloads']['cost']
        print('corr:',corr)


if __name__ == '__main__':
    # main2()
    main3()
    

