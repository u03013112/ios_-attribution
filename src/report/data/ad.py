import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.report.data.tools import getFilename1
from src.report.geo import getIOSGeoGroup01
from src.report.media import getIOSMediaGroup01

# 海外iOS广告成本
# 分国家，分媒体
# 有必要自行再groupby
def getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory):
    
    filename = getFilename1('adData',startDayStr,endDayStr,directory,'GroupByCampaignAndGeoAndMedia')

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')
    sql = f'''
        SELECT
            install_day as install_date,
            ct.campaign_id,
            cmap.campaign_name as campaign_name,
            ct.mediasource,
            ct.country  as country_code,
            sum(ct.impression) as impression,
            sum(ct.click) as click,
            sum(ct.ad_install) as install,
            sum(ct.cost_value_usd) as cost
        FROM
            (
                SELECT
                    install_day,
                    campaign_id,
                    mediasource,
                    country,
                    impression,
                    click,
                    ad_install,
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
                UNION
                ALL
                SELECT
                    install_day,
                    campaign_id,
                    mediasource,
                    country,
                    impression,
                    click,
                    ad_install,
                    cost_value_usd
                FROM
                    rg_bi.dwd_overseas_cost_history
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
                    AND facebook_segment in ('country', 'N/A')
            ) AS ct
            LEFT JOIN (
                SELECT 
                    cmap.campaign_id,
                    MAX(cmap.mediasource) AS mediasource,
                    MAX(cmap.campaign_name) AS campaign_name
                FROM 
                    rg_bi.dwb_overseas_mediasource_campaign_map AS cmap
                GROUP BY 
                    cmap.campaign_id
            ) AS cmap ON ct.campaign_id = cmap.campaign_id
        WHERE
            ct.install_day BETWEEN '{startDayStr}'
            AND '{endDayStr}'
        GROUP BY
            install_day,
            ct.campaign_id,
            ct.mediasource,
            ct.country,
            cmap.campaign_name;
    '''
    print(sql)
    adCostDf = execSql(sql)
    print('已获得%d条数据'%len(adCostDf))
    
    # 这是为了去掉tiktokglobal_int，不知道为啥，用新表之后应该不需要了
    adCostDf = adCostDf.loc[adCostDf.mediasource != 'tiktokglobal_int']

    geoGroupList = getIOSGeoGroup01()
    adCostDf['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adCostDf.loc[adCostDf.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']

    mediaGroupList = getIOSMediaGroup01()
    adCostDf['media'] = 'other'
    for mediaGroup in mediaGroupList:
        adCostDf.loc[adCostDf.mediasource.isin(mediaGroup['codeList']),'media'] = mediaGroup['name']
    
    adCostDf = adCostDf.groupby(
        ['install_date','campaign_id','campaign_name','geoGroup','media']
        ,as_index=False
    ).agg(
        {    
            'impression':'sum',
            'click':'sum',
            'install':'sum',
            'cost':'sum'
        }
    ).reset_index(drop=True)
    
    adCostDf = adCostDf.groupby(['install_date','campaign_id','campaign_name','geoGroup','media'],as_index=False).sum().reset_index(drop=True)

    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    adCostDf['campaign_id'] = adCostDf['campaign_id'].astype(str)
    return adCostDf

# 不再按照GeoGroup分组，按照自然国家分组
def getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr,directory):
    filename = getFilename1('adData2_',startDayStr,endDayStr,directory,'GroupByCampaignAndGeoAndMedia')

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')
    sql = f'''
        SELECT
            install_day as install_date,
            ct.campaign_id,
            cmap.campaign_name as campaign_name,
            ct.mediasource,
            ct.country  as country_code,
            sum(ct.impression) as impression,
            sum(ct.click) as click,
            sum(ct.ad_install) as install,
            sum(ct.cost_value_usd) as cost
        FROM
            (
                SELECT
                    install_day,
                    campaign_id,
                    mediasource,
                    country,
                    impression,
                    click,
                    ad_install,
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
                UNION
                ALL
                SELECT
                    install_day,
                    campaign_id,
                    mediasource,
                    country,
                    impression,
                    click,
                    ad_install,
                    cost_value_usd
                FROM
                    rg_bi.dwd_overseas_cost_history
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
                    AND facebook_segment in ('country', 'N/A')
            ) AS ct
            LEFT JOIN (
                SELECT 
                    cmap.campaign_id,
                    MAX(cmap.mediasource) AS mediasource,
                    MAX(cmap.campaign_name) AS campaign_name
                FROM 
                    rg_bi.dwb_overseas_mediasource_campaign_map AS cmap
                GROUP BY 
                    cmap.campaign_id
            ) AS cmap ON ct.campaign_id = cmap.campaign_id
        WHERE
            ct.install_day BETWEEN '{startDayStr}'
            AND '{endDayStr}'
        GROUP BY
            install_day,
            ct.campaign_id,
            ct.mediasource,
            ct.country,
            cmap.campaign_name;
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
        ['install_date','campaign_id','campaign_name','country_code','media']
        ,as_index=False
    ).agg(
        {    
            'impression':'sum',
            'click':'sum',
            'install':'sum',
            'cost':'sum'
        }
    ).reset_index(drop=True)

    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    adCostDf['campaign_id'] = adCostDf['campaign_id'].astype(str)
    return adCostDf

# 简化一下，只按照geo分组
def getAdCostDataIOSGroupByGeo(startDayStr,endDayStr,directory):
    filename = getFilename1('adData',startDayStr,endDayStr,directory,'GroupByGeo')

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        print('从MC获得数据')

    sql = f'''
    SELECT
        CASE
            WHEN country IN ('SA', 'AE', 'KW', 'QA', 'OM', 'BH') THEN 'GCC'
            WHEN country = 'KR' THEN 'KR'
            WHEN country = 'US' THEN 'US'
            WHEN country = 'JP' THEN 'JP'
            ELSE 'other'
        END as country_group,
        sum(cost_value_usd) as cost
    FROM
        (
            SELECT
                install_day,
                campaign_id,
                mediasource,
                country,
                impression,
                click,
                ad_install,
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
            UNION
            ALL
            SELECT
                install_day,
                campaign_id,
                mediasource,
                country,
                impression,
                click,
                ad_install,
                cost_value_usd
            FROM
                rg_bi.dwd_overseas_cost_history
            WHERE
                app = '102'
                AND zone = '0'
                AND app_package = 'id1479198816'
                AND cost_value_usd > 0
                AND facebook_segment in ('country', 'N/A')
        )
    WHERE
        install_day BETWEEN '{startDayStr}'
        AND '{endDayStr}'
    GROUP BY
        country_group;
    '''
    print(sql)
    adCostDf = execSql(sql)
    print('已获得%d条数据'%len(adCostDf))
    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)
    return adCostDf
    

if __name__ == '__main__':
    # startDayStr = '20230826'
    # endDayStr = '20231025'

    # directory = '/src/data/report/iOSWeekly20231018_20231025'
    # getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory)

    df = pd.read_csv('/src/data/report/iOSWeekly20231026_20231102/adData20231026_20231102_GroupByCampaignAndGeoAndMedia.csv')
    df = df.groupby(['install_date','media'],as_index=False).agg({'cost':'sum'}).reset_index(drop=True)
    print(df)
