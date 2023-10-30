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
def getAdCostDataIOSGroupByGeoAndMedia(startDayStr,endDayStr,directory):
    
    filename = getFilename1('adCost',startDayStr,endDayStr,directory,'GroupByGeoAndMedia')

    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    else:
        print('从MC获得数据')
    sql = f'''
        SELECT
            install_day as install_date,
            mediasource,
            country as country_code,
            sum(cost_value_usd) as cost
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
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
            ) AS combined_table
        WHERE
            install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
        group by
            install_day,
            mediasource,
            country;
    '''
    print(sql)
    adCostDf = execSql(sql)
    print('已获得%d条数据'%len(adCostDf))
    
    # 这是为了去掉tiktokglobal_int，不知道为啥，用新表之后应该不需要了
    adCostDf = adCostDf.loc[adCostDf.mediasource != 'tiktokglobal_int']

    groupByIndexList = ['install_date']

    
    groupByIndexList.append('geoGroup')

    geoGroupList = getIOSGeoGroup01()
    adCostDf['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adCostDf.loc[adCostDf.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']

    
    groupByIndexList.append('media')

    mediaGroupList = getIOSMediaGroup01()
    adCostDf['media'] = 'other'
    for mediaGroup in mediaGroupList:
        adCostDf.loc[adCostDf.mediasource.isin(mediaGroup['codeList']),'media'] = mediaGroup['name']
    
    adCostDf = adCostDf.groupby(groupByIndexList,as_index=False).agg({'cost':'sum'}).reset_index(drop=True)
    
    adCostDf.to_csv(filename,index=False)
    print('存储在%s'%filename)

    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    return adCostDf

