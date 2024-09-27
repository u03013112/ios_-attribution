# 2024-06-15~2024-06-30
# Facebook SKAN原始报告中，涉及到KR的回收金额计算

import io
import os
import requests
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.config import facebook_access_token

# 获取SKAN原始报告
def getFacebookSKANRawReport(startDate = '20240615', endDate = '20240630'):
    filename = f'/src/data/facebook_skan_raw_report_{startDate}_{endDate}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql = f'''
select
    skad_ad_network_id,
    skad_conversion_value,
    skad_campaign_id,
    count(*) as count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between {startDate} and {endDate}
    and app_id = '6448786147'
    and skad_ad_network_id in (
        'v9wttpbfk9.skadnetwork',
        'n38lu8286q.skadnetwork'
    )
    group by
        skad_ad_network_id,
        skad_conversion_value,
        skad_campaign_id
;
        '''
        result = execSql(sql)
        result.to_csv(filename, index=False)
    
    return result

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

def getSkanCampaignId(since='2024-06-15', until='2024-06-30'):
    acts = ['act_2307953922735195', 'act_324526830346411', 'act_888398116305812', 'act_1440346639910845', 'act_8319795938035732']
    all_data = []

    for act in acts:
        df = getSkanCampaignIdByAct(act, since, until)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True).drop_duplicates()
    return final_df

csvStr = '''
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



def main():
    skanCampaignId = getSkanCampaignId()
    # print(skanCampaignId.head())
    campaignNameList = skanCampaignId['campaign_name'].unique().tolist()
    KRCampaignNameList = [name for name in campaignNameList if 'KR' in name]
    KRSkanCampaignId = skanCampaignId[skanCampaignId['campaign_name'].isin(KRCampaignNameList)]
    KRSkanCampaignId = KRSkanCampaignId.sort_values(by=['campaign_name', 'day'])
    KRSkanCampaignId.to_csv('/src/data/facebook_skan_campaign_id_KR.csv', index=False)
    KRSkanCampaignId2 = skanCampaignId[skanCampaignId['skan_campaign_id'].isin(KRSkanCampaignId['skan_campaign_id'])]
    KRSkanCampaignId2 = KRSkanCampaignId2.sort_values(by=['skan_campaign_id', 'day'])
    KRSkanCampaignId2.to_csv('/src/data/facebook_skan_campaign_id_KR2.csv', index=False)


    campaignIdList = KRSkanCampaignId2['skan_campaign_id'].unique().tolist()
    print(campaignIdList)

    skanRawReport = getFacebookSKANRawReport()
    skanRawReport = skanRawReport[skanRawReport['skad_campaign_id'].isin(campaignIdList)]
    print('skanRawReport count sum:', skanRawReport['count'].sum())

    # 计算skad_conversion_value为空的count比例
    nullDf = skanRawReport[skanRawReport['skad_conversion_value'].isna()]
    nullCount = nullDf['count'].sum()

    totalDf = skanRawReport.groupby('skad_campaign_id').sum().reset_index()
    totalCount = totalDf['count'].sum()

    print(f'nullCount: {nullCount}, totalCount: {totalCount}, nullCount/totalCount: {nullCount/totalCount}')

    skanRawReportGrouped = skanRawReport.groupby(['skad_conversion_value', 'skad_campaign_id']).sum().reset_index()
    skanRawReportGrouped.loc[skanRawReportGrouped['skad_conversion_value']>31, 'skad_conversion_value'] -= 32
    skanRawReportGrouped.rename(columns={'skad_conversion_value': 'conversion_value'}, inplace=True)
    # print('skanRawReportGrouped:')
    # print(skanRawReportGrouped.head())

    csv_file_like_object = io.StringIO(csvStr)
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvMapDf['avg_event_revenue'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue']) / 2
    cvMapDf['avg_event_revenue'] = cvMapDf['avg_event_revenue'].fillna(0)

    df = pd.merge(skanRawReportGrouped, cvMapDf[['conversion_value','avg_event_revenue']], on='conversion_value', how='left')
    df['revenue'] = df['count'] * df['avg_event_revenue']
    df = df.groupby('skad_campaign_id').agg({
        'count': 'sum',
        'avg_event_revenue': 'sum',
    }).reset_index()

    print(df)
    print('sum count:',df['count'].sum())
    print('sum revenue:',df['avg_event_revenue'].sum())

if __name__ == '__main__':
    main()
    
