# 与af 的 am 交流
# 发一份数据给他们，让他们看看

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getPostbackData():
    sql = '''
        select *
        from 
            ods_platform_appsflyer_skad_postbacks_copy
        where
            day = 20230901
            and app_id = 'id1479198816'
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/apple_postback_copy_20230901.csv',index=False)

# getPostbackData()

def getAfPostbackData():
    sql = '''
        select *
        from 
            ods_platform_appsflyer_skad_details
        where
            day = 20230901
            and app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/af_postback_20230901.csv',index=False)

# getAfPostbackData()   


appleDf = pd.read_csv('/src/data/zk2/apple_postback_copy_20230901.csv')
afDf = pd.read_csv('/src/data/zk2/af_postback_20230901.csv')

appleDf['count'] = 1
afDf['count'] = 1

appleFacebookDf = appleDf[appleDf['skad_ad_network_id'].isin(['v9wttpbfk9.skadnetwork','n38lu8286q.skadnetwork'])]
afFacebookDf = afDf[afDf['media_source'] == 'Facebook Ads']

print('appleFacebook cv count:')
print(appleFacebookDf.groupby('skad_conversion_value').agg({'count':'sum'}))

print('afFacebook cv count:')
print(afFacebookDf.groupby('skad_conversion_value').agg({'count':'sum'}))