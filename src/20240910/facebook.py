# 尝试用facebook api获得
# skan_campaign_id 与 campaign_id 的对应关系

# 得到数据后
# 难点：对应关系如果是变化的，如何处理

# 但愿在campaign没有新建、删除的情况下，对应关系是不变的

import io
import os
import requests
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.config import facebook_access_token

import os
import requests
import pandas as pd
import sys
from datetime import datetime, timedelta

sys.path.append('/src')
from src.maxCompute import execSql
from src.config import facebook_access_token

# def getSkanCampaignId(since='2024-08-01', until='2024-08-31', next=''):
#     filename = f'/src/data/facebook_skan_campaign_id_{since}_{until}.csv'
#     if os.path.exists(filename):
#         df = pd.read_csv(filename)
#         df = df.drop_duplicates()
#         return df
#     else:
#         acts = ['act_2307953922735195', 'act_324526830346411', 'act_888398116305812', 'act_1440346639910845', 'act_8319795938035732']
#         all_data = []

#         for act in acts:
#             if next:
#                 url = next
#                 response = requests.get(url)
#             else:
#                 url = f'https://graph.facebook.com/v20.0/{act}/insights'
#                 params = {
#                     'access_token': facebook_access_token,
#                     'fields': 'account_id,campaign_name,campaign_id,total_postbacks_detailed',
#                     'action_attribution_windows': "['skan_view','skan_click']",
#                     'time_increment': 1,
#                     'time_range': f"{{'since':'{since}','until':'{until}'}}",
#                     'breakdowns': '["skan_campaign_id"]',
#                     'level': 'campaign'
#                 }
#                 response = requests.get(url, params=params)

#             campaignIds = []
#             campaignNames = []
#             skanCampaignIds = []
#             days = []

#             jsonData = response.json()
#             data = jsonData.get('data', [])

#             for item in data:
#                 campaignIds.append(item['campaign_id'])
#                 campaignNames.append(item['campaign_name'])
#                 skanCampaignIds.append(item['skan_campaign_id'])
#                 days.append(item['date_start'])

#             df = pd.DataFrame({
#                 'day': days,
#                 'campaign_id': campaignIds,
#                 'campaign_name': campaignNames,
#                 'skan_campaign_id': skanCampaignIds
#             })

#             all_data.append(df)

#             if 'paging' in jsonData:
#                 if 'next' in jsonData['paging']:
#                     next = jsonData['paging']['next']
#                     nextData = getSkanCampaignId(next=next)
#                     all_data.append(nextData)

#         final_df = pd.concat(all_data, ignore_index=True).drop_duplicates()
#         final_df.to_csv(filename, index=False)

#     return final_df

def getSkanCampaignIdByAct(act, since='2024-08-01', until='2024-08-31', next=''):
    filename = f'/src/data/facebook_skan_campaign_id_{act}_{since}_{until}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename, dtype={'campaign_id': 'str'})
        df = df.drop_duplicates()
        return df
    else:
        all_data = []

        while True:
            if next:
                url = next
                response = requests.get(url)
            else:
                url = f'https://graph.facebook.com/v20.0/{act}/insights'
                params = {
                    'access_token': facebook_access_token,
                    'fields': 'account_id,campaign_name,campaign_id,total_postbacks_detailed',
                    'action_attribution_windows': "['skan_view','skan_click']",
                    'time_increment': 1,
                    'time_range': f"{{'since':'{since}','until':'{until}'}}",
                    'breakdowns': '["skan_campaign_id"]',
                    'level': 'campaign'
                }
                response = requests.get(url, params=params)

            if response.status_code != 200:
                print(f"Failed to fetch data for act {act}. Status code: {response.status_code}")
                break

            jsonData = response.json()
            data = jsonData.get('data', [])

            if not data:
                break

            campaignIds = []
            campaignNames = []
            skanCampaignIds = []
            days = []

            for item in data:
                campaignIds.append(item['campaign_id'])
                campaignNames.append(item['campaign_name'])
                skanCampaignIds.append(item['skan_campaign_id'])
                days.append(item['date_start'])

            df = pd.DataFrame({
                'day': days,
                'campaign_id': campaignIds,
                'campaign_name': campaignNames,
                'skan_campaign_id': skanCampaignIds
            })

            all_data.append(df)

            if 'paging' in jsonData and 'next' in jsonData['paging']:
                next = jsonData['paging']['next']
            else:
                break

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True).drop_duplicates()
            final_df.to_csv(filename, index=False)
            return final_df
        else:
            return pd.DataFrame(columns=['day', 'campaign_id', 'campaign_name', 'skan_campaign_id'])

def getSkanCampaignId(since='2024-08-01', until='2024-08-31'):
    acts = ['act_2307953922735195', 'act_324526830346411', 'act_888398116305812', 'act_1440346639910845', 'act_8319795938035732']
    all_data = []

    for act in acts:
        df = getSkanCampaignIdByAct(act, since, until)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True).drop_duplicates()
    # filename = f'/src/data/facebook_skan_campaign_id_{since}_{until}.csv'
    # final_df.to_csv(filename, index=False)

    return final_df

def check1():
    data = getSkanCampaignId()
    
    # 找到同一天，是否存在相同的campaign_id对应不同的skan_campaign_id
    g1Df = data.groupby(['day', 'campaign_id']).agg({'skan_campaign_id': 'nunique'}).reset_index()
    g1Df = g1Df[g1Df['skan_campaign_id'] > 1]
    if g1Df.shape[0] > 0:
        print('>>same campaign_id, different skan_campaign_id:')
        merged_g1 = data.merge(g1Df[['day', 'campaign_id']], on=['day', 'campaign_id'], how='inner')
        print(merged_g1)


    # 找到同一天，是否存在相同的skan_campaign_id对应不同的campaign_id
    g2Df = data.groupby(['day', 'skan_campaign_id']).agg({'campaign_id': 'nunique'}).reset_index()
    g2Df = g2Df[g2Df['campaign_id'] > 1]
    if g2Df.shape[0] > 0:
        print('>>same skan_campaign_id, different campaign_id:')
        merged_g2 = data.merge(g2Df[['day', 'skan_campaign_id']], on=['day', 'skan_campaign_id'], how='inner')
        print(merged_g2)

def getSkadRawReport(day='20240815'):
    filename = f'/src/data/facebook_skad_raw_report_{day}.csv'

    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql = f'''
select
    skad_app_id,
    skad_source_app_id,
    skad_ad_network_id,
    skad_conversion_value,
    skad_campaign_id,
    timestamp
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day = { day }
    and app_id = '6448786147'
    and skad_ad_network_id in (
        'v9wttpbfk9.skadnetwork',
        'n38lu8286q.skadnetwork'
    );
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

# 尝试对Facebook的原始报告进行campaign_id的映射
def check2():
    skanRawDf = getSkadRawReport()
    skanRawDf['timestamp'] = pd.to_datetime(skanRawDf['timestamp'])
    
    skanRawDf['min_install_time'] = skanRawDf.apply(
        lambda row: row['timestamp'] - pd.Timedelta(hours=48) if row['skad_conversion_value'] in [0, 32] else row['timestamp'] - pd.Timedelta(hours=72),
        axis=1
    )
    skanRawDf['max_install_time'] = skanRawDf['timestamp'] - pd.Timedelta(hours=24)
    
    skanRawDf = skanRawDf.sort_values(by=['timestamp'])
    skanRawDf['index'] = range(skanRawDf.shape[0])
    print('所有的skan行数：', skanRawDf.shape[0])

    fbDf = getSkanCampaignId()
    print('所有的fb行数：', fbDf.shape[0])

    result_list = []

    for i, row in skanRawDf.iterrows():
        matched = fbDf[
            (fbDf['skan_campaign_id'] == row['skad_campaign_id']) &
            (fbDf['day'] >= row['min_install_time'].strftime('%Y-%m-%d')) &
            (fbDf['day'] <= row['max_install_time'].strftime('%Y-%m-%d'))
        ]
        if not matched.empty:
            # 由于实在多天内进行匹配，可能会匹配到同一个campaign多次
            # 所以要排重
            matched = matched.drop_duplicates(subset=['campaign_id'])

            for _, match_row in matched.iterrows():
                new_row = row.copy()
                new_row['campaign_id'] = match_row['campaign_id']
                new_row['campaign_name'] = match_row['campaign_name']
                result_list.append(new_row)
        else:
            result_list.append(row)

    result_df = pd.DataFrame(result_list)
    
    print(result_df.head())
    result_df.to_csv('/src/data/facebook_skad_raw_report_20240815_with_campaign_id1.csv', index=False)

    print('没有匹配到的行数：',result_df[result_df['campaign_id'].isnull()].shape[0])
    print('匹配到多个campaign的行数：', result_df[result_df.duplicated(subset=['index'], keep=False)].shape[0])
    
    return result_df


def check2(day):
    skanRawDf = getSkadRawReport(day)
    skanRawDf['timestamp'] = pd.to_datetime(skanRawDf['timestamp'])
    
    skanRawDf['min_install_time'] = skanRawDf.apply(
        lambda row: row['timestamp'] - pd.Timedelta(hours=48) if row['skad_conversion_value'] in [0, 32] else row['timestamp'] - pd.Timedelta(hours=72),
        axis=1
    )
    skanRawDf['max_install_time'] = skanRawDf['timestamp'] - pd.Timedelta(hours=24)
    
    skanRawDf = skanRawDf.sort_values(by=['timestamp'])
    skanRawDf['index'] = range(skanRawDf.shape[0])
    print('所有的skan行数：', skanRawDf.shape[0])

    fbDf = getSkanCampaignId()
    print('所有的fb行数：', fbDf.shape[0])

    result_list = []

    for i, row in skanRawDf.iterrows():
        matched = fbDf[
            (fbDf['skan_campaign_id'] == row['skad_campaign_id']) &
            (fbDf['day'] >= row['min_install_time'].strftime('%Y-%m-%d')) &
            (fbDf['day'] <= row['max_install_time'].strftime('%Y-%m-%d'))
        ]
        if not matched.empty:
            # 由于实在多天内进行匹配，可能会匹配到同一个campaign多次
            # 所以要排重
            matched = matched.drop_duplicates(subset=['campaign_id'])

            for _, match_row in matched.iterrows():
                new_row = row.copy()
                new_row['campaign_id'] = match_row['campaign_id']
                new_row['campaign_name'] = match_row['campaign_name']
                result_list.append(new_row)
        else:
            result_list.append(row)

    result_df = pd.DataFrame(result_list)
    
    print(result_df.head())
    result_df.to_csv(f'/src/data/facebook_skad_raw_report_20240815_with_campaign_id3_{day}.csv', index=False)

    print('没有匹配到的行数：',result_df[result_df['campaign_id'].isnull()].shape[0])
    print('匹配到多个campaign的行数：', result_df[result_df.duplicated(subset=['index'], keep=False)].shape[0])
    
    return result_df



csvStr20240706 = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2024-07-05 09:48:05,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,0.97,0,24,2024-07-05 09:48:05,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,0.97,0.99,0,24,2024-07-05 09:48:05,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,0.99,1.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,1.92,2.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,2.91,3.28,0,24,2024-07-05 09:48:05,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,3.28,5.85,0,24,2024-07-05 09:48:05,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,5.85,7.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,7.67,9.24,0,24,2024-07-05 09:48:05,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,9.24,12.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,12.4,14.95,0,24,2024-07-05 09:48:05,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,14.95,17.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,17.96,22.37,0,24,2024-07-05 09:48:05,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,22.37,26.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,26.96,31.81,0,24,2024-07-05 09:48:05,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,31.81,36.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,36.25,42.53,0,24,2024-07-05 09:48:05,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,42.53,49.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,49.91,57.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,57.92,67.93,0,24,2024-07-05 09:48:05,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,67.93,81.27,0,24,2024-07-05 09:48:05,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,81.27,98.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,98.25,117.86,0,24,2024-07-05 09:48:05,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,117.86,142.29,0,24,2024-07-05 09:48:05,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,142.29,180.76,0,24,2024-07-05 09:48:05,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,180.76,225.43,0,24,2024-07-05 09:48:05,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,225.43,276.72,0,24,2024-07-05 09:48:05,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,276.72,347.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,347.4,472.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,472.67,620.8,0,24,2024-07-05 09:48:05,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,620.8,972.22,0,24,2024-07-05 09:48:05,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,972.22,2038.09,0,24,2024-07-05 09:48:05,0,,,
'''

def debug2(df):
    # df = pd.read_csv('/src/data/facebook_skad_raw_report_20240815_with_campaign_id1.csv', dtype={'campaign_id': 'str','skad_conversion_value': 'float64'})
    # campaign_name 类似：Lastwar_JP_IOS_VO_FT_AAA_20240706
    # 截取出country
    df['country'] = df['campaign_name'].str.split('_').str[1]

    nullDf = df[df['campaign_id'].isnull()]
    print('没有匹配到的行数：',nullDf.shape[0])
    # 出现了skad_campaign_id = 26，在af翻译后的版本中没有26，有待进一步调查


    csv_file_like_object = io.StringIO(csvStr20240706)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    cvMapDf['usd'] = (cvMapDf['max_event_revenue'] + cvMapDf['min_event_revenue'])/2
    cvMapDf['usd'] = cvMapDf['usd'].fillna(0)

    df['skad_conversion_value'] = df['skad_conversion_value'].fillna(0)

    df.loc[df['skad_conversion_value']>= 32.0,'skad_conversion_value'] -= 32.0
    df = df.merge(cvMapDf[['conversion_value','usd']], left_on='skad_conversion_value', right_on='conversion_value', how='left')
    
    # 找到多重匹配的行
    dupDf = df.groupby(['index']).agg({
        'campaign_id': 'nunique',
        'country': 'nunique',
        'conversion_value': 'max',
        'usd': 'max'
    }).reset_index()
    print('总行数：', dupDf.shape[0])
    print('没有匹配到的行数：',dupDf[dupDf['campaign_id'].isnull()].shape[0])
    print('重复匹配行数：',dupDf[dupDf['campaign_id'] > 1].shape[0])
    print('总金额：',dupDf['usd'].sum())
    print('重复匹配金额：',dupDf[dupDf['campaign_id'] > 1]['usd'].sum())

    print('重复匹配并且国家不同的行数：',dupDf[(dupDf['campaign_id'] > 1) & (dupDf['country'] > 1)].shape[0])
    print('重复匹配并且国家不同的金额：',dupDf[(dupDf['campaign_id'] > 1) & (dupDf['country'] > 1)]['usd'].sum())

    # df1 = dupDf[(dupDf['campaign_id'] > 1) & (dupDf['country'] > 1) & (dupDf['usd'] > 0)]
    # df1Index = df1['index'].tolist()
    # df1 = df[df['index'].isin(df1Index)]
    # print(df1)

# check2 的扩展，更多的数据，结果更加稳定
def check3():
    skanRawDf = pd.DataFrame()
    dayList = [
        '20240815','20240816','20240817','20240818','20240819','20240820','20240821','20240822','20240823','20240824','20240825','20240826','20240827','20240828','20240829','20240830','20240831'
    ]
    for day in dayList:
        skanRawDf0 = getSkadRawReport(day)
        skanRawDf = pd.concat([skanRawDf,skanRawDf0],ignore_index=True)
        
    skanRawDf['timestamp'] = pd.to_datetime(skanRawDf['timestamp'])
    
    skanRawDf['min_install_time'] = skanRawDf.apply(
        lambda row: row['timestamp'] - pd.Timedelta(hours=48) if row['skad_conversion_value'] in [0, 32] else row['timestamp'] - pd.Timedelta(hours=72),
        axis=1
    )
    skanRawDf['max_install_time'] = skanRawDf['timestamp'] - pd.Timedelta(hours=24)
    
    skanRawDf = skanRawDf.sort_values(by=['timestamp'])
    skanRawDf['index'] = range(skanRawDf.shape[0])
    print('所有的skan行数：', skanRawDf.shape[0])

    fbDf = getSkanCampaignId()
    print('所有的fb行数：', fbDf.shape[0])

    result_list = []

    for i, row in skanRawDf.iterrows():
        matched = fbDf[
            (fbDf['skan_campaign_id'] == row['skad_campaign_id']) &
            (fbDf['day'] >= row['min_install_time'].strftime('%Y-%m-%d')) &
            (fbDf['day'] <= row['max_install_time'].strftime('%Y-%m-%d'))
        ]
        if not matched.empty:
            # 由于实在多天内进行匹配，可能会匹配到同一个campaign多次
            # 所以要排重
            matched = matched.drop_duplicates(subset=['campaign_id'])

            for _, match_row in matched.iterrows():
                new_row = row.copy()
                new_row['campaign_id'] = match_row['campaign_id']
                new_row['campaign_name'] = match_row['campaign_name']
                result_list.append(new_row)
        else:
            result_list.append(row)

    result_df = pd.DataFrame(result_list)
    
    # print(result_df.head())
    result_df.to_csv('/src/data/facebook_skad_raw_report_20240815_with_campaign_id3.csv', index=False)

    print('没有匹配到的行数：',result_df[result_df['campaign_id'].isnull()].shape[0])
    print('匹配到多个campaign的行数：', result_df[result_df.duplicated(subset=['index'], keep=False)].shape[0])
    
    return result_df

def debug3():
    fbDf = getSkanCampaignId()
    fbDf = fbDf.sort_values(by=['skan_campaign_id','day','campaign_id','campaign_name'])
    fbDf.to_csv('/src/data/facebook_skan_campaign_id.csv', index=False)

if __name__ == '__main__':
    dayList = [
        '20240815','20240816','20240817','20240818','20240819','20240820','20240821','20240822','20240823','20240824','20240825','20240826','20240827','20240828','20240829','20240830','20240831'
    ]

    df = pd.DataFrame()
    for i in range(len(dayList)):
        day = dayList[i]
        df0 = pd.read_csv(f'/src/data/facebook_skad_raw_report_20240815_with_campaign_id3_{day}.csv')
        df0['index'] += i*100000
    #     df0 = check2(day)
        df = pd.concat([df,df0],ignore_index=True)

    debug2(df)

    # debug3()
    

    