# 用于验证FunPlus02AdvUidMutiDaysCampaign2Lastwar.py的正确性
# 验证方式是获取一段较长的时间（比如7天或者14天）
# 然后对比这段时间4个媒体的SKAN收入和融合归因收入
# 应该总体差异不大，应该差异在5%以内

import io
import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

mediaList = [
    'Facebook Ads',
    'googleadwords_int',
    'bytedanceglobal_int',
    'other'
]

def getSKANDataFromMC(sinceDayStr, untilDayStr):
    filename = '/src/data/FunPlus02AdvUidMutiDaysCampaign2Lastwar_debug_skan.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename, dtype={'campaign_id':str})
        return df
    
    sql = f'''
        SELECT
            ad_network_campaign_id as campaign_id,
            skad_conversion_value as cv,
            count(*) as count
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day between '{sinceDayStr}' and '{untilDayStr}'
            AND app_id = 'id6448786147'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
        GROUP BY
            ad_network_campaign_id,
            skad_conversion_value
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(filename, index=False)
    return df

def getCvMap():
    csv_str = '''
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
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf


def getZkDataFromMC(sinceDayStr, untilDayStr):
    filename = '/src/data/FunPlus02AdvUidMutiDaysCampaign2Lastwar_debug_zk.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename, dtype={'campaign_id':str})
        return df
    

    sql = f'''
        SELECT
            sql1.campaign_id,
            SUM(sql2.revenue_value_usd * sql1.rate) AS revenue,
            SUM(sql1.rate) AS count
        FROM
            (
                SELECT
                    customer_user_id,
                    campaign_id,
                    sum(rate) as rate
                FROM
                    lastwar_ios_funplus02_adv_uid_mutidays_campaign2
                WHERE
                    day BETWEEN '{sinceDayStr}'
                    AND '{untilDayStr}'
                group by
                    customer_user_id,
                    campaign_id
            ) AS sql1
            LEFT JOIN (
                SELECT
                    game_uid AS customer_user_id,
                    sum(revenue_value_usd) as revenue_value_usd
                FROM
                    rg_bi.ads_lastwar_ios_purchase_adv
                WHERE
                    event_timestamp - install_timestamp BETWEEN 0 AND 24 * 3600
                GROUP BY
                    game_uid
            ) AS sql2 ON sql1.customer_user_id = sql2.customer_user_id
        GROUP BY
            sql1.campaign_id;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(filename, index=False)
    return df

def debug(sinceDayStr, untilDayStr):
    # 逐天遍历,sinceDayStr和untilDayStr都是类似20231201的字符串
    sinceDay = pd.to_datetime(sinceDayStr, format='%Y%m%d')
    untilDay = pd.to_datetime(untilDayStr, format='%Y%m%d')

    for day in pd.date_range(sinceDay, untilDay):
        dayStr = day.strftime('%Y%m%d')
        print(dayStr)
        

        sql = f'''
            SELECT
                customer_user_id,
                sum(rate) as rate
            FROM
                topwar_ios_funplus02_adv_uid_mutidays_campaign2
            WHERE
                day = '{dayStr}'
                and campaign_id <> 'total media'
            GROUP BY
                customer_user_id
            HAVING
                rate > 1
            ;
        '''
        # print(sql)
        df = execSql(sql)
        print(df)
    

def debug2():
    sql = f'''
        SELECT
            customer_user_id,
            sum(rate) as rate
        FROM
            topwar_ios_funplus02_adv_uid_mutidays_campaign2
        WHERE
            day between '20231201' and '20231207'
            and campaign_id <> 'total media'
        GROUP BY
            customer_user_id
        HAVING
            rate > 1
        ;
    '''
    # print(sql)
    df = execSql(sql)
    print(df)

def debug3(sinceDayStr, untilDayStr):

    sql = f'''
        SELECT
            SUM(CASE WHEN sql1.rate >= 0.99 THEN sql2.revenue_value_usd ELSE 0 END) / SUM(sql2.revenue_value_usd) AS rate_over_99,
            SUM(CASE WHEN sql1.rate >= 0.8 THEN sql2.revenue_value_usd ELSE 0 END) / SUM(sql2.revenue_value_usd) AS rate_over_80,
            SUM(CASE WHEN sql1.rate >= 0.5 THEN sql2.revenue_value_usd ELSE 0 END) / SUM(sql2.revenue_value_usd) AS rate_over_50,
            SUM(CASE WHEN sql1.rate >= 0 THEN sql2.revenue_value_usd ELSE 0 END) / SUM(sql2.revenue_value_usd) AS rate_over_0
        FROM
            (
                SELECT
                    customer_user_id,
                    max(rate) as rate
                FROM
                    lastwar_ios_funplus02_adv_uid_mutidays_campaign2
                WHERE
                    day BETWEEN '{sinceDayStr}'
                    AND '{untilDayStr}'
                group by
                    customer_user_id
            ) AS sql1
            LEFT JOIN (
                SELECT
                    game_uid AS customer_user_id,
                    sum(revenue_value_usd) as revenue_value_usd
                FROM
                    rg_bi.ads_lastwar_ios_purchase_adv
                WHERE
                    event_timestamp - install_timestamp BETWEEN 0
                    AND 24 * 3600
                GROUP BY
                    game_uid
            ) AS sql2 ON sql1.customer_user_id = sql2.customer_user_id
        ;
    '''
    print(sql)
    df = execSql(sql)
    print(df)


def debug4(sinceDayStr, untilDayStr):
    sql = f'''
        select
            sum(case when revenue_value_usd > 50 then 1 else 0 end)/sum(case when revenue_value_usd > 0 then 1 else 0 end) as 50_rate,
            sum(case when revenue_value_usd > 30 then 1 else 0 end)/sum(case when revenue_value_usd > 0 then 1 else 0 end) as 30_rate,
            sum(case when revenue_value_usd > 20 then 1 else 0 end)/sum(case when revenue_value_usd > 0 then 1 else 0 end) as 20_rate,
            sum(case when revenue_value_usd > 10 then 1 else 0 end)/sum(case when revenue_value_usd > 0 then 1 else 0 end) as 10_rate
        from
            (
                select
                    game_uid as customer_user_id,
                    sum(revenue_value_usd) as revenue_value_usd
                from
                    ads_topwar_ios_purchase_adv
                where
                    install_day between '{sinceDayStr}' and '{untilDayStr}'
                    and event_timestamp - install_timestamp between 0 and 24 * 3600
                group by
                    game_uid
            )
        ;    
    '''
    print(sql)
    df = execSql(sql)
    print(df)

def main(sinceDayStr, untilDayStr):
    # 先获取这段时间的SKAN收入
    skanDf = getSKANDataFromMC(sinceDayStr, untilDayStr)
    skanDf = skanDf.groupby(['campaign_id','cv']).sum().reset_index()
    skanDf['cv'].fillna(0)
    skanDf['cv'] = skanDf['cv'].astype(int)

    cvMapDf = getCvMap()
    cvMapDf['avg'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue']) / 2
    cvMapDf = cvMapDf.fillna(0)
    cvMapDf = cvMapDf[['conversion_value','avg']]
    cvMapDf.rename(columns={'conversion_value':'cv'}, inplace=True)
    skanDf.loc[skanDf['cv'] >= 32, 'cv'] -= 32
    skanDf = pd.merge(skanDf, cvMapDf, on='cv', how='left')
    skanDf['revenue'] = skanDf['count'] * skanDf['avg']
    # print(skanDf[skanDf['campaign_id'] == '23862335430260113'])

    skanDf = skanDf.groupby('campaign_id').agg({
        'count':'sum',
        'revenue':'sum'
    }).reset_index()
    # print(skanDf.sort_values(by=['revenue'], ascending=False))

    # 再获取这段时间的融合归因收入
    zkDf = getZkDataFromMC(sinceDayStr, untilDayStr)
    # print(zkDf.sort_values(by=['revenue'], ascending=False))

    df = pd.merge(skanDf, zkDf, on='campaign_id', how='left', suffixes=('_skan', '_zk'))
    df['(skan-zk)/zk'] = (df['revenue_skan'] - df['revenue_zk']) / df['revenue_zk']

    print(df.sort_values(by=['revenue_zk'], ascending=False))

    skanSum = df['revenue_skan'].sum()
    zkSum = df['revenue_zk'].sum()

    print(f'skanSum={skanSum}, zkSum={zkSum}, (skanSum-zkSum)/zkSum={(skanSum-zkSum)/zkSum}')

    skanCountSum = df['count_skan'].sum()
    zkCountSum = df['count_zk'].sum()
    print(f'skanCountSum={skanCountSum}, zkCountSum={zkCountSum}, (skanCountSum-zkCountSum)/zkCountSum={(skanCountSum-zkCountSum)/zkCountSum}')

if __name__ == '__main__':
    # main('20231201', '20231220')
    # debug('20231201', '20231214')
    # debug3('20231201', '20231220')
    debug4('20231201', '20231220')
