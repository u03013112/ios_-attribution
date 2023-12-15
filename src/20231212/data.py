import os
import pandas as pd
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj

from src.report.media import getIOSMediaGroup01
from src.report.geo import getIOSGeoGroup01

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
            install_day as install_date,
            mediasource,
            country as country_code,
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
                    cost_value_usd
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
            install_day,
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
            'cost':'sum'
        }
    ).reset_index(drop=True)

    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    # adCostDf['campaign_id'] = adCostDf['campaign_id'].astype(str)
    return adCostDf

# 验证目前KPI设计思路，目前是12个月回本，看目前的结论是否仍然适用，预期结论应该是差不多的
def step1():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # 其中拥有完整12个月数据应该是2021-01-01到2022-12-10

    df365 = df.loc[
        (df['install_date'] >= '202205') &
        (df['install_date'] < '202212')
    ].copy()
    # 按照目前的地理分组
    geoGroupList = getIOSGeoGroup01()
    df365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df365.loc[df365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df365 = df365.groupby(['geoGroup','install_date'],as_index=False).agg({'revenue_d7':'sum','revenue_d360':'sum'}).reset_index(drop=True)
    

    adDf = pd.read_csv('/src/data/adData_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # adDf['install_date'] 从 类似20210101的字符串 转换成 202101 的 str
    adDf['install_date'] = adDf['install_date'].apply(lambda x:x[:6])
    adDf365 = adDf.copy()
    adDf365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adDf365.loc[adDf365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    adDf365 = adDf365.groupby(['geoGroup','install_date'],as_index=False).agg({'cost':'sum'}).reset_index(drop=True)

    df = pd.merge(df365,adDf365,on=['geoGroup','install_date'],how='left')

    df = df.groupby(['geoGroup']).sum().reset_index()

    df['roi7'] = df['revenue_d7']/df['cost']
    df['roi360'] = df['revenue_d360']/df['cost']

    df['r'] = 1/df['roi360']
    df['kpi7'] = df['r']*df['roi7']
    df = df[['geoGroup','roi7','roi360','kpi7']]
    print(df)
    kpi = {
        'US':0.065,
        'KR':0.065,
        'JP':0.055,
        'GCC':0.06,
        'other':0.07
    }
    df['kpi'] = df['geoGroup'].apply(lambda x:kpi[x])
    # 按照kpi要求的话，360日ROI预计应该是
    df['r'] = df['kpi']/df['roi7']
    df['kpi360'] = df['r']*df['roi360']
    df = df[['geoGroup','roi7','roi360','kpi','kpi360']]
    df.to_csv('/src/data/20231212_step1.csv',index=False)

def step2():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    df365 = df.loc[
        # (df['install_date'] >= '202106') &
        (df['install_date'] < '202212')
    ].copy()
    df365 = df365.groupby(['country_code','install_date'],as_index=False).agg({'revenue_d7':'sum','revenue_d360':'sum'}).reset_index(drop=True)
    

    adDf = pd.read_csv('/src/data/adData_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # adDf['install_date'] 从 类似20210101的字符串 转换成 202101 的 str
    adDf['install_date'] = adDf['install_date'].apply(lambda x:x[:6])
    adDf365 = adDf.copy()
    adDf365 = adDf365.groupby(['country_code','install_date'],as_index=False).agg({'cost':'sum'}).reset_index(drop=True)


    df = pd.merge(df365,adDf365,on=['country_code','install_date'],how='left')
    
    df = df.groupby(['country_code']).sum().reset_index()
    df['roi7'] = df['revenue_d7']/df['cost']
    df['roi360'] = df['revenue_d360']/df['cost']

    df['r'] = 1.2/df['roi360']
    df['kpi7'] = df['r']*df['roi7']
    # df = df[['country_code','cost','roi7','roi360','kpi7']]
    # print(df)
    
    # 先排除目前已经分组的国家，JP、KR、US、GCC
    dfFilted = df.loc[ df['country_code'].isin(['JP','KR','US','SA','AE','KW','QA','OM','BH'])==False ].copy()

    # 做简单过滤，将kpi7< US 并且 cost > 10000 的国家筛选出来,将
    usKpi7 = df.loc[df['country_code']=='US','kpi7'].values[0]
    
    dfFilted['geoGroup'] = 'T2'

    dfFilted.loc[
        (dfFilted['kpi7'] < usKpi7) & (dfFilted['cost'] > 20000)
        ,'geoGroup'
    ] = 'T1'

    # 打印T1国家
    print(dfFilted.loc[dfFilted['geoGroup']=='T1'])

    # 重新计算T1的kpi7与T2的kpi7

    dfFilted = dfFilted.groupby(['geoGroup']).sum().reset_index()
    dfFilted['roi7'] = dfFilted['revenue_d7']/dfFilted['cost']
    dfFilted['roi360'] = dfFilted['revenue_d360']/dfFilted['cost']

    dfFilted['r'] = 1.2/dfFilted['roi360']
    dfFilted['kpi7'] = dfFilted['r']*dfFilted['roi7']
    dfFilted = dfFilted[['geoGroup','cost','roi7','roi360','kpi7']]
    print(dfFilted)

    dfFilted.to_csv('/src/data/20231212_step2.csv',index=False)

import matplotlib.pyplot as plt
import seaborn as sns

def step3():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # 其中拥有完整12个月数据应该是2021-01-01到2022-12-10

    df365 = df.loc[
        # (df['install_date'] >= '202205') &
        (df['install_date'] < '202212')
    ].copy()
    # 按照目前的地理分组
    geoGroupList = getIOSGeoGroup01()
    df365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df365.loc[df365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df365 = df365.groupby(['geoGroup','install_date'],as_index=False).agg({'revenue_d7':'sum','revenue_d30':'sum','revenue_d60':'sum','revenue_d360':'sum'}).reset_index(drop=True)

    df365['install_date'] = pd.to_datetime(df365['install_date'],format='%Y%m')
    for a in ['revenue_d7','revenue_d30','revenue_d60']:
        df365['r'] = df365['revenue_d360']/df365[a]
        # 计算r的标准差
        df365 = df365.sort_values(by='install_date').reset_index(drop=True)

        # 设置风格
        sns.set(style="whitegrid")

        # 创建图形和轴对象
        fig, ax = plt.subplots(figsize=(16, 6))

        # 对于每一个地理分组，画一条线
        for geo_group in df365['geoGroup'].unique():
            df_temp = df365[df365['geoGroup'] == geo_group]
            sns.lineplot(x='install_date', y='r', data=df_temp, label=geo_group, ax=ax)
            rStd = df_temp['r'].std()
            print(a,geo_group,'rStd:',rStd)

        # 设置标题和标签
        ax.set_title('Revenue Ratio Over Time by Geo Group')
        ax.set_xlabel('Install Date')
        ax.set_ylabel('Revenue Ratio (revenue_d360/%s)'%a)

        # 显示图例
        ax.legend()

        # 保存图形
        plt.savefig('/src/data/20231212_step3_%s.png'%a)
        plt.close()
    
def step4():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # 其中拥有完整12个月数据应该是2021-01-01到2022-12-10

    df365 = df
    # 按照目前的地理分组
    geoGroupList = getIOSGeoGroup01()
    geoGroupList = geoGroupList + [{'name':'other','codeList':[]}]
    df365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df365.loc[df365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df365 = df365.groupby(['geoGroup','install_date'],as_index=False).agg({'revenue_d7':'sum','revenue_d30':'sum','revenue_d60':'sum','revenue_d360':'sum'}).reset_index(drop=True)

    installDateList = df365['install_date'].unique().tolist()
    installDateList.sort()
    installDateList = installDateList[12:]
    # print(installDateList)

    for geoGroup in geoGroupList:
        geoDf = df365.loc[df365['geoGroup'] == geoGroup['name']].copy()
        for installDate in installDateList:
            N = 6
            installDate = datetime.datetime.strptime(installDate,'%Y%m')
            # 从installDate往前推12+N~12个月，记作startInstallDate，endInstallDate
            startInstallDate = installDate - datetime.timedelta(days=(12+N)*30)
            endInstallDate = installDate - datetime.timedelta(days=12*30)
            # print(geoGroup['name'],installDate,startInstallDate,endInstallDate)
            retainDf = geoDf.loc[
                (geoDf['install_date'] >= startInstallDate.strftime('%Y%m')) &
                (geoDf['install_date'] <= endInstallDate.strftime('%Y%m'))
            ].copy()
            if len(retainDf) < 3:
                print(geoGroup['name'],installDate,'数据不足，跳过')
                continue
            # 用retainDf 求出revenue_d360/ revenue_d30，revenue_d360/ revenue_d60的平均值
            r30 = retainDf['revenue_d360'].sum()/retainDf['revenue_d30'].sum()
            r60 = retainDf['revenue_d360'].sum()/retainDf['revenue_d60'].sum()
            geoDf.loc[geoDf['install_date'] == installDate.strftime('%Y%m'),'r30'] = r30
            geoDf.loc[geoDf['install_date'] == installDate.strftime('%Y%m'),'r60'] = r60
        
        geoDf['revenue_d360(r30)'] = geoDf['revenue_d30']*geoDf['r30']
        geoDf['revenue_d360(r60)'] = geoDf['revenue_d60']*geoDf['r60']

        # 画图
        geoPicDf = geoDf.loc[
            (geoDf['install_date'] >= '202203') &
            (geoDf['install_date'] <= '202211')
        ].copy()
        sns.set(style="whitegrid")

        # 创建图形和轴对象
        fig, ax = plt.subplots(figsize=(16, 6))

         # 对于每一个收入数据类型，画一条线
        data_types = [('revenue_d360', 'Actual Revenue'),
                      ('revenue_d360(r30)', 'Estimated Revenue (r30)'),
                      ('revenue_d360(r60)', 'Estimated Revenue (r60)')]
        
        for data_type, label in data_types:
            sns.lineplot(x='install_date', y=data_type, data=geoPicDf, label=label, ax=ax)

        # 设置标题和标签
        ax.set_title(f'Revenue Comparison for {geoGroup["name"]}')
        ax.set_xlabel('Install Date')
        ax.set_ylabel('Revenue')

        # 显示图例
        ax.legend()

        # 保存图形
        plt.savefig(f'/src/data/20231212_step4_{geoGroup["name"]}.png')
        plt.close()

        # 计算MAPE
        # 目前数据只能计算2022-03到2022-11的MAPE
        geoMapeDf = geoDf.loc[
            (geoDf['install_date'] >= '202203') &
            (geoDf['install_date'] <= '202211')
        ].copy()
        geoMapeDf['MAPE(r30)'] = abs(geoMapeDf['revenue_d360']-geoMapeDf['revenue_d360(r30)'])/geoMapeDf['revenue_d360']
        geoMapeDf['MAPE(r60)'] = abs(geoMapeDf['revenue_d360']-geoMapeDf['revenue_d360(r60)'])/geoMapeDf['revenue_d360']
        print(geoGroup['name'],'MAPE(r30):',geoMapeDf['MAPE(r30)'].mean())
        print(geoGroup['name'],'MAPE(r60):',geoMapeDf['MAPE(r60)'].mean())


def getPredictDataFromMC():
    filename = '/src/data/20231212_predictDataFromMC.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    
    sql = '''
        select
            country as geo_group,
            predict_revenue as predict_revenue_d360,
            month as install_date
        from rg_ai_bj.ads_predict_base_roi_month_notoragnic
        where
            app_package = 'id1479198816'
            and app = '102'
            and country in ('US','KR','JP')
            and predict_day == 360
        ;
    '''
    print(sql)
    df = execSqlBj(sql)
    df['install_date'] = df['install_date'].astype(str)
    df.rename(columns={
        'geo_group':'geoGroup',
        'predict_revenue_d360':'predictRevenue_d360'
    },inplace=True)
    df.to_csv(filename,index=False)

def step5():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # 其中拥有完整12个月数据应该是2021-01-01到2022-12-10

    predictDf = getPredictDataFromMC()

    df365 = df
    # 按照目前的地理分组
    geoGroupList = getIOSGeoGroup01()
    geoGroupList = geoGroupList + [{'name':'other','codeList':[]}]
    df365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df365.loc[df365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df365 = df365.groupby(['geoGroup','install_date'],as_index=False).agg({'revenue_d7':'sum','revenue_d30':'sum','revenue_d60':'sum','revenue_d360':'sum'}).reset_index(drop=True)

    adDf = pd.read_csv('/src/data/adData_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    # adDf['install_date'] 从 类似20210101的字符串 转换成 202101 的 str
    adDf['install_date'] = adDf['install_date'].apply(lambda x:x[:6])
    adDf365 = adDf.copy()
    adDf365['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adDf365.loc[adDf365.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    adDf365 = adDf365.groupby(['geoGroup','install_date'],as_index=False).agg({'cost':'sum'}).reset_index(drop=True)

    df = pd.merge(df365,adDf365,on=['geoGroup','install_date'],how='left')

    installDateList = df365.loc[
        (df365['install_date']>='202301') &
        (df365['install_date']<'202310')
    ]['install_date'].unique().tolist()
    installDateList.sort()

    retDf = pd.DataFrame()
    for geoGroup in geoGroupList:
        geoDf = df.loc[df['geoGroup'] == geoGroup['name']].copy()
        for installDate in installDateList:
            N = 6
            installDate = datetime.datetime.strptime(installDate,'%Y%m')
            # 从installDate往前推12+N~12个月，记作startInstallDate，endInstallDate
            startInstallDate = installDate - datetime.timedelta(days=(12+N)*30)
            endInstallDate = installDate - datetime.timedelta(days=12*30)
            # print(geoGroup['name'],installDate,startInstallDate,endInstallDate)
            retainDf = geoDf.loc[
                (geoDf['install_date'] >= startInstallDate.strftime('%Y%m')) &
                (geoDf['install_date'] <= endInstallDate.strftime('%Y%m'))
            ].copy()
            if len(retainDf) < 3:
                print(geoGroup['name'],installDate,'数据不足，跳过')
                continue
            r60 = retainDf['revenue_d360'].sum()/retainDf['revenue_d60'].sum()
            geoDf.loc[geoDf['install_date'] == installDate.strftime('%Y%m'),'r60'] = r60
        
        geoDf['revenue_d360(r60)'] = geoDf['revenue_d60']*geoDf['r60']
        geoDf = geoDf.loc[
            (geoDf['install_date']>='202301') &
            (geoDf['install_date']<'202310')
        ].copy()
        # print(geoDf)
        # print(predictDf)
        geoDf = pd.merge(geoDf,predictDf,on=['geoGroup','install_date'],how='left')
        print(geoDf)

        groupDf = geoDf.groupby(['geoGroup']).sum().reset_index()
        groupDf['roi360(r60)'] = groupDf['revenue_d360(r60)']/groupDf['cost']
        groupDf['roi360(predict)'] = groupDf['predictRevenue_d360']/groupDf['cost']

        retDf = retDf.append(groupDf[['geoGroup','cost','revenue_d60','revenue_d360(r60)','roi360(r60)','predictRevenue_d360','roi360(predict)']])
    
    # print(retDf)
    retDf.to_csv('/src/data/20231212_step5.csv',index=False)




def main():
    startDayStr = '20210101'
    endDayStr = '20231031'
    getRevenueDataIOSGroupByGeo(startDayStr,endDayStr)
    getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr)

    # step1()
    # step2()
    # step3()
    # step4()
    step5()

def debug():
    df = pd.read_csv('/src/data/revenue365_20210101_20231031_GroupByGeo.csv',dtype={'install_date':str})
    df365 = df.loc[
        (df['install_date'] < '202212')
    ].copy()

    print(df365.loc[
        (df365['country_code']=='KR') &
        (df365['install_date']=='202211')
    ])

if __name__ == '__main__':
    main()

    # debug()
    