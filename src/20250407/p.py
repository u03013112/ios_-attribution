import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import re
import io
import sys
sys.path.append('/src')
from src.maxCompute import execSql


def main():
    df03 = pd.read_csv('lastwar_inapps_20250403.csv')
    df04 = pd.read_csv('lastwar_inapps_20250404.csv')
    df05 = pd.read_csv('lastwar_inapps_20250405.csv')

    print('列名:')
    print(df03.columns)
    print(df04.columns)

    df03 = df03.sort_values(by=['app_id','skad_conversion_value'])
    print(df03[(df03['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())
    df04 = df04.sort_values(by=['app_id','skad_conversion_value'])
    print(df04[(df04['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())
    df05 = df05.sort_values(by=['app_id','skad_conversion_value'])
    print(df05[(df05['app_id'] == 'id6448786147')]['skad_conversion_value'].unique())


    df03 = df03[(df03['app_id'] == 'id6448786147') & (df03['skad_conversion_value'] == 32)]
    df04 = df04[(df04['app_id'] == 'id6448786147') & (df04['skad_conversion_value'] == 32)]


    print('len df03:',len(df03))
    print('len df04:',len(df04))
    return

    # print('event_name count:')
    # df03G = df03.groupby(['event_name']).size().reset_index(name='count')
    # df04G = df04.groupby(['event_name']).size().reset_index(name='count')
    # print(df03G)
    # print(df04G)

    for col in df03.columns:
        if col not in df04.columns:
            print(f"列 {col} 在 df03 中存在，但在 df04 中不存在。")
            continue
        
        if col == 'app_id':
            continue

        print(f"==================\n列 {col} 的 count:")
        df03G = df03.groupby([col]).size().reset_index(name='count')
        df04G = df04.groupby([col]).size().reset_index(name='count')
        print('20250403:')
        print(df03G)
        print('')
        print('20250404:')
        print(df04G)

    

def getSKANDataFromMC(dayStr, days):
    dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')

    sql = f'''
        SELECT
            skad_conversion_value as cv,
            media_source,
            count(*) as cnt,
            day
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day between '{dayBeforeStr}' and '{dayStr}'
            AND app_id = 'id6448786147'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
        GROUP BY
            skad_conversion_value,
            media_source,
            day
        ;
    '''

    df = execSql(sql)
    return df


def forHaitao():
    sql = '''
select
*
from 
ods_platform_appsflyer_skad_postbacks_copy
where
    day = 20250401
    and app_id in ('6448786147')
;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/haitao.csv', index=False)


def forHaitao2():
    sql = '''
select *
from
ods_platform_appsflyer_skad_details
where
    day = 20250401
    and app_id in ('id6448786147')
;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/haitao2.csv', index=False)


def p1():
    df = getSKANDataFromMC('20250612', 60)

    df = df.groupby(['day', 'cv']).agg({'cnt': 'sum'}).reset_index()

    # 按照day，cv排序
    # 画图，day为x轴，cnt为y轴，每个cv一张图
    # 保存到文件 /src/data/20250407_cv{cv}.png
    cvList = df['cv'].unique()
    for cv in cvList:
        df2 = df[df['cv'] == cv]
        df2 = df2.sort_values(by=['day'])
        df2['day'] = pd.to_datetime(df2['day'], format='%Y%m%d')
        df2.to_csv(f'/src/data/20250421_cv{cv}.csv', index=False)
        df2.plot(x='day', y='cnt', title=f'cv={cv}', kind='line')
        plt.legend(loc='best')
        plt.savefig(f'/src/data/20250421_cv{cv}.png')
        plt.close()


def af20250415():
    df = getSKANDataFromMC('20250512', 20)

    df = df.sort_values(by=['day', 'media_source','cv'])

    df.to_csv('/src/data/20250407_af20250415.csv', index=False)


import os
def debug():
    filename = '/src/data/20250512_debug.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
SELECT
    skad_conversion_value as cv,
    count(*) as cnt,
    media_source,
    ad_network_campaign_name,
    day
FROM
    ods_platform_appsflyer_skad_details
WHERE
    day between '20250421'
    and '20250512'
    AND app_id = 'id6448786147'
    AND event_name in ('af_purchase_update_skan_on')
    and skad_conversion_value = 32
GROUP BY
    skad_conversion_value,
    media_source,
    ad_network_campaign_name,
    day;
        '''

        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

def debug2():

# 30~35 200~300
# 27~29 180~350

    sql1 = '''
SELECT
count(*) as cnt,
media_source,
day
FROM
ods_platform_appsflyer_skad_details
WHERE
day between '20250501'
and '20250508'
AND app_id = 'id6448786147'
AND event_name in ('af_purchase_update_skan_on')
and skad_conversion_value between 30 and 35
GROUP BY
media_source,
day;
    '''
    df = execSql(sql1)
    df.to_csv('/src/data/20250512_debug1.csv', index=False)

    sql2 = '''
SELECT
count(*) as cnt,
media_source,
day
FROM
ods_platform_appsflyer_skad_details
WHERE
day between '20250309'
and '20250315'
AND app_id = 'id6448786147'
AND event_name in ('af_purchase_update_skan_on')
and skad_conversion_value between 27 and 29
GROUP BY
media_source,
day;
    '''

# 尝试将20250404之前建立的campaign的CV == 32 的数据进行削弱，然后再和大盘收入金额做比较
def func20250512():
    filename = '/src/data/20250612_func20250512.csv'

    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        # 暂时从20250501开始，到20250508
        sql = '''
SELECT
    skad_conversion_value as cv,
    count(*) as cnt,
    media_source,
    ad_network_campaign_name,
    install_date
FROM
    ods_platform_appsflyer_skad_details
WHERE
    day between '20250428'
    and '20250612'
    AND app_id = 'id6448786147'
    AND event_name in ('af_purchase_update_skan_on')
GROUP BY
    skad_conversion_value,
    media_source,
    ad_network_campaign_name,
    install_date
;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)

    df = df[df['install_date'] >= '2025-06-01']

    # 获得campaign的创建时间
    campaignNameList = df['ad_network_campaign_name'].unique()
    # print('campaignNameList:', campaignNameList)
    
    # 正则表达式匹配8位数字
    def extract_campaign_create_time(campaignNameList):
        campaignCreateTime = {}
        date_pattern = re.compile(r'(\d{8})')
        unmatched_campaigns = []
        for campaignName in campaignNameList:
            if not isinstance(campaignName, str):
                continue
            # 使用正则表达式搜索匹配
            match = date_pattern.search(campaignName)
            if match:
                campaignCreateTime[campaignName] = match.group(1)
            else:
                unmatched_campaigns.append(campaignName)
        return campaignCreateTime, unmatched_campaigns
    

    campaignCreateTime, unmatched_campaigns = extract_campaign_create_time(campaignNameList)
    
    # 没有匹配的campaign
    def extract_campaign_create_time4(unmatched_campaigns_in):
        campaignCreateTime = {}
        # 定义一个正则表达式模式来匹配4位数字
        date_pattern = re.compile(r'(\d{4})')
        unmatched_campaigns = []
        for campaignName in unmatched_campaigns_in:
            # 使用正则表达式搜索匹配
            match = date_pattern.search(campaignName)
            # if campaignName == 'LW_WW_IOS_D28ROAS_JY_0826':
            #     print('LW_WW_IOS_D28ROAS_JY_0826:', match)
            if match:
                day_month = match.group(1)
                day = day_month[:2]
                month = int(day_month[2:])  # 获取月份并转换为整数
                # 根据月份决定年份
                if month < 6:
                    year = '25'
                else:
                    year = '24'
                # 构造完整的日期字符串
                full_date = f'20{year}{day}{month:02}'
                campaignCreateTime[campaignName] = full_date
            else:
                # campaignCreateTime[campaignName] = None  # 如果没有找到4位数字，保持为 None
                unmatched_campaigns.append(campaignName)
        return campaignCreateTime, unmatched_campaigns
    
    campaignCreateTime4, unmatched_campaigns = extract_campaign_create_time4(unmatched_campaigns)

    # 合并两个字典
    campaignCreateTime.update(campaignCreateTime4)
    # 将字典的k，v转成Df
    campaignCreateTimeDf = pd.DataFrame(list(campaignCreateTime.items()), columns=['campaignName', 'createTime'])

    campaignCreateTimeDf['isNew'] = 0
    campaignCreateTimeDf['isNew'] = campaignCreateTimeDf['createTime'].apply(lambda x: 1 if x >= '20250404' else 0)
    campaignCreateTimeDf.rename(columns={
        'campaignName': 'ad_network_campaign_name',
        'createTime': 'campaignCreateTime'
    }, inplace=True)

    # 将campaignCreateTimeDf和df进行左连接
    df = pd.merge(df, campaignCreateTimeDf, on='ad_network_campaign_name', how='left')
    df['isNew'] = df['isNew'].fillna(0)
    df.to_csv('/src/data/20250512_func20250512_isNew.csv', index=False)


    csvStr20250403 = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2025-04-03 05:19:54,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,1.06,0,24,2025-04-03 05:19:54,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,1.06,2.13,0,24,2025-04-03 05:19:54,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,2.13,3.2,0,24,2025-04-03 05:19:54,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,3.2,5.97,0,24,2025-04-03 05:19:54,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,5.97,8.08,0,24,2025-04-03 05:19:54,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,8.08,10.91,0,24,2025-04-03 05:19:54,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,10.91,13.31,0,24,2025-04-03 05:19:54,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,13.31,17.17,0,24,2025-04-03 05:19:54,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,17.17,22.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,22.15,27.18,0,24,2025-04-03 05:19:54,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,27.18,32.25,0,24,2025-04-03 05:19:54,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,32.25,37.27,0,24,2025-04-03 05:19:54,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,37.27,42.96,0,24,2025-04-03 05:19:54,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,42.96,49.94,0,24,2025-04-03 05:19:54,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,49.94,57.19,0,24,2025-04-03 05:19:54,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,57.19,63.4,0,24,2025-04-03 05:19:54,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,63.4,70.59,0,24,2025-04-03 05:19:54,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,70.59,79.52,0,24,2025-04-03 05:19:54,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,79.52,89.43,0,24,2025-04-03 05:19:54,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,89.43,98.61,0,24,2025-04-03 05:19:54,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,98.61,105.7,0,24,2025-04-03 05:19:54,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,105.7,114.61,0,24,2025-04-03 05:19:54,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,114.61,124.59,0,24,2025-04-03 05:19:54,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,124.59,135.71,0,24,2025-04-03 05:19:54,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,135.71,148.07,0,24,2025-04-03 05:19:54,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,148.07,160.79,0,24,2025-04-03 05:19:54,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,160.79,173.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,173.8,185.74,0,24,2025-04-03 05:19:54,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,185.74,199.42,0,24,2025-04-03 05:19:54,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,199.42,215.69,0,24,2025-04-03 05:19:54,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,215.69,232.62,0,24,2025-04-03 05:19:54,0,,,
id6448786147,32,af_purchase_update_skan_on,0,1,232.62,247.52,0,24,2025-04-03 05:19:54,0,,,
id6448786147,33,af_purchase_update_skan_on,0,1,247.52,264.32,0,24,2025-04-03 05:19:54,0,,,
id6448786147,34,af_purchase_update_skan_on,0,1,264.32,283.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,35,af_purchase_update_skan_on,0,1,283.8,299.49,0,24,2025-04-03 05:19:54,0,,,
id6448786147,36,af_purchase_update_skan_on,0,1,299.49,325.72,0,24,2025-04-03 05:19:54,0,,,
id6448786147,37,af_purchase_update_skan_on,0,1,325.72,347.38,0,24,2025-04-03 05:19:54,0,,,
id6448786147,38,af_purchase_update_skan_on,0,1,347.38,374.22,0,24,2025-04-03 05:19:54,0,,,
id6448786147,39,af_purchase_update_skan_on,0,1,374.22,401.19,0,24,2025-04-03 05:19:54,0,,,
id6448786147,40,af_purchase_update_skan_on,0,1,401.19,441.07,0,24,2025-04-03 05:19:54,0,,,
id6448786147,41,af_purchase_update_skan_on,0,1,441.07,492.51,0,24,2025-04-03 05:19:54,0,,,
id6448786147,42,af_purchase_update_skan_on,0,1,492.51,519.21,0,24,2025-04-03 05:19:54,0,,,
id6448786147,43,af_purchase_update_skan_on,0,1,519.21,549.44,0,24,2025-04-03 05:19:54,0,,,
id6448786147,44,af_purchase_update_skan_on,0,1,549.44,580.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,45,af_purchase_update_skan_on,0,1,580.15,617.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,46,af_purchase_update_skan_on,0,1,617.15,668.99,0,24,2025-04-03 05:19:54,0,,,
id6448786147,47,af_purchase_update_skan_on,0,1,668.99,711.78,0,24,2025-04-03 05:19:54,0,,,
id6448786147,48,af_purchase_update_skan_on,0,1,711.78,762.73,0,24,2025-04-03 05:19:54,0,,,
id6448786147,49,af_purchase_update_skan_on,0,1,762.73,815.06,0,24,2025-04-03 05:19:54,0,,,
id6448786147,50,af_purchase_update_skan_on,0,1,815.06,915.85,0,24,2025-04-03 05:19:54,0,,,
id6448786147,51,af_purchase_update_skan_on,0,1,915.85,975.68,0,24,2025-04-03 05:19:54,0,,,
id6448786147,52,af_purchase_update_skan_on,0,1,975.68,1057.86,0,24,2025-04-03 05:19:54,0,,,
id6448786147,53,af_purchase_update_skan_on,0,1,1057.86,1239.54,0,24,2025-04-03 05:19:54,0,,,
id6448786147,54,af_purchase_update_skan_on,0,1,1239.54,1282.83,0,24,2025-04-03 05:19:54,0,,,
id6448786147,55,af_purchase_update_skan_on,0,1,1282.83,1416.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,56,af_purchase_update_skan_on,0,1,1416.8,1570,0,24,2025-04-03 05:19:54,0,,,
id6448786147,57,af_purchase_update_skan_on,0,1,1570,1628.32,0,24,2025-04-03 05:19:54,0,,,
id6448786147,58,af_purchase_update_skan_on,0,1,1628.32,1745.34,0,24,2025-04-03 05:19:54,0,,,
id6448786147,59,af_purchase_update_skan_on,0,1,1745.34,1792.65,0,24,2025-04-03 05:19:54,0,,,
id6448786147,60,af_purchase_update_skan_on,0,1,1792.65,1935.41,0,24,2025-04-03 05:19:54,0,,,
id6448786147,61,af_purchase_update_skan_on,0,1,1935.41,2304.95,0,24,2025-04-03 05:19:54,0,,,
id6448786147,62,af_purchase_update_skan_on,0,1,2304.95,3006.72,0,24,2025-04-03 05:19:54,0,,,
id6448786147,63,af_purchase_update_skan_on,0,1,3006.72,3814.71,0,24,2025-04-03 05:19:54,0,,,
'''

    csv_file_like_object = io.StringIO(csvStr20250403)    
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']].copy()
    
    cvMapDf.rename(columns={
        'conversion_value': 'cv',
        'min_event_revenue': 'min_revenue',
        'max_event_revenue': 'max_revenue'
    }, inplace=True)
    cvMapDf['avg_revenue'] = (cvMapDf['min_revenue'] + cvMapDf['max_revenue']) / 2
    cvMapDf.fillna(0, inplace=True)
    cvMapDf = cvMapDf[['cv','avg_revenue']]
    # print(cvMapDf)

    # 将df和cvMapDf进行左连接
    df = pd.merge(df, cvMapDf, on='cv', how='left')
    

    nerfRateList = [0,0.5,0.8,1]

    final_df = pd.DataFrame()
    for nerfRate in nerfRateList:
        df0 = df.copy()
        df0['cnt'] = df0.apply(lambda x: int(x['cnt'] * (1-nerfRate)) if x['isNew'] == 0 and x['cv'] == 32 else x['cnt'], axis=1)
        df0['revenue'] = df0['cnt'] * df0['avg_revenue']
        
        # 按照 install_date 分组，计算每日收入和
        df0_grouped = df0.groupby(['install_date']).agg({'revenue': 'sum'}).reset_index()
        
        # 重命名列以添加后缀
        df0_grouped.rename(columns={'revenue': f'revenue_{nerfRate}'}, inplace=True)
        
        # 合并到最终的 DataFrame
        if final_df.empty:
            final_df = df0_grouped
        else:
            final_df = final_df.merge(df0_grouped, on='install_date', how='outer')
    # 保存最终的合并结果到 CSV
    final_df.to_csv('/src/data/20250512_final_nerfRate.csv', index=False)


if __name__ == "__main__":
    # main()
    # forHaitao()
    # forHaitao2()
    # p1()
    # af20250415()

    # debugDf = debug()

    # debug2()

    func20250512()