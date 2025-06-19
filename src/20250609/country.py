import os
import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO


def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/lw_cost_revenue_country_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name,
    sum(cost_value_usd) as cost,
    sum(revenue_h24) as revenue_h24,
    sum(revenue_d1) as revenue_d1,
    sum(revenue_d3) as revenue_d3,
    sum(revenue_d7) as revenue_d7,
    sum(revenue_d14) as revenue_d14,
    sum(revenue_d30) as revenue_d30,
    sum(revenue_d60) as revenue_d60,
    sum(revenue_d90) as revenue_d90,
    sum(revenue_d120) as revenue_d120,
    sum(revenue_d150) as revenue_d150
from
    dws_overseas_public_roi
where
    app = '502'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

def step1():
    df = getRevenueData('20240101', '20250501')
    print(df.head())

    # 将不满日的数据置为空
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    # 添加广告类型
    # 只针对 'Facebook Ads' 和 'googleadwords_int'
    # 其他媒体广告类型都置'N/A'
    df['ad_type'] = 'N/A'
    # Facebook Ads 中 campaign_name 中包含 'BAU' 的置为 'BAU' , 包含 'AAA' 的置为 'AAA', 包含 '3A' 的置为 'AAA',
    # 其他置为 'N/A'
    df.loc[df['mediasource'] == 'Facebook Ads', 'ad_type'] = df['campaign_name'].apply(
        lambda x: 'BAU' if 'BAU' in x else ('AAA' if 'AAA' in x else ('AAA' if '3A' in x else 'N/A'))
    )
    print('facebook 广告类型未成功匹配的共有：', df[(df['mediasource'] == 'Facebook Ads') & (df['ad_type'] == 'N/A')].shape[0])
    print(df[(df['mediasource'] == 'Facebook Ads') & (df['ad_type'] == 'N/A')]['campaign_name'].unique())
    # googleadwords_int 中 campaign_name 中包含 '3.0' 的置为 '3.0' , 包含 '2.5' 的置为 '2.5', 包含 '1.0' 的置为 '1.0', 包含 'smart' 的置为 'smart'
    # 其他置为 'N/A'
    df.loc[df['mediasource'] == 'googleadwords_int', 'ad_type'] = df['campaign_name'].apply(
        lambda x: '3.0' if '3.0' in x else ('2.5' if '2.5' in x else ('1.0' if '1.0' in x else ('smart' if 'smart' in x else 'N/A')))
    )
    print('google 广告类型未成功匹配的共有：', df[(df['mediasource'] == 'googleadwords_int') & (df['ad_type'] == 'N/A')].shape[0])
    print(df[(df['mediasource'] == 'googleadwords_int') & (df['ad_type'] == 'N/A')]['campaign_name'].unique())

    # 按照mediasource, country, ad_type, install_day 分组
    group_cols = ['app_package','mediasource', 'country', 'ad_type', 'install_day']
    df = df.groupby(group_cols).sum().reset_index()
    print('按广告类型分组后数据预览：')
    print(df.head())

    # 国家分组
    countryGroupMap = {
        'T1': ['AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL'],
        'T2': ['BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA'],
        'T3': ['AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK'],
        'US':['US'],
        'JP':['JP'],
        'KR':['KR'],
        'TW':['TW'],
        'GCC':['SA','AE','QA','KW','BH','OM']
    }
    df['country_group'] = 'T3'  # 默认分组为 T3
    for group, countries in countryGroupMap.items():
        df.loc[df['country'].isin(countries), 'country_group'] = group
    group_cols = ['app_package','mediasource', 'country_group', 'ad_type', 'install_day']
    df = df.groupby(group_cols).sum().reset_index()
    print('国家分组后数据预览：')
    print(df.head())

    for days in [1, 3, 7, 14, 30, 60, 90, 120, 150]:
        df[f'revenue_d{days}'] = df.apply(
            lambda row: row[f'revenue_d{days}'] if (yesterday - row['install_day']).days >= days else np.nan,
            axis=1
        )
    df = df.sort_values(by='install_day')
    print('将不满日的数据置为空 ok')

    df.to_csv('/src/data/lw_20250619_step1.csv', index=False)
    
def step2():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # print(df.head())  
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()

    # 计算收入比率
    df = df[[
        'app_package', 'mediasource', 'country_group', 'ad_type', 'install_day',
        'revenue_d1', 'revenue_d3', 'revenue_d7', 'revenue_d14', 'revenue_d30', 
        'revenue_d60', 'revenue_d90', 'revenue_d120', 'revenue_d150'
    ]].copy()

    df['r3/r1'] = df['revenue_d3'] / df['revenue_d1']
    df['r7/r3'] = df['revenue_d7'] / df['revenue_d3']
    df['r14/r7'] = df['revenue_d14'] / df['revenue_d7']
    df['r30/r14'] = df['revenue_d30'] / df['revenue_d14']
    df['r60/r30'] = df['revenue_d60'] / df['revenue_d30']
    df['r90/r60'] = df['revenue_d90'] / df['revenue_d60']
    df['r120/r90'] = df['revenue_d120'] / df['revenue_d90']
    df['r150/r120'] = df['revenue_d150'] / df['revenue_d120']
    
    df.replace([np.inf, -np.inf], np.nan, inplace=True)


    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r3/r1', 'std_r7/r3', 'std_r14/r7', 'std_r30/r14',
        'std_r60/r30', 'std_r90/r60', 'std_r120/r90', 'std_r150/r120',
        'mean_r3/r1', 'mean_r7/r3', 'mean_r14/r7', 'mean_r30/r14',
        'mean_r60/r30', 'mean_r90/r60', 'mean_r120/r90', 'mean_r150/r120',
        'cv_r3/r1', 'cv_r7/r3', 'cv_r14/r7', 'cv_r30/r14',
        'cv_r60/r30', 'cv_r90/r60', 'cv_r120/r90', 'cv_r150/r120',
        'iqr_r3/r1', 'iqr_r7/r3', 'iqr_r14/r7', 'iqr_r30/r14',
        'iqr_r60/r30', 'iqr_r90/r60', 'iqr_r120/r90', 'iqr_r150/r120'
    ])
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing group: {name}")
        # # for debug
        # if name[0] == 'com.fun.lastwar.gp' and name[1] == 'Facebook Ads' and name[2] == 'GCC' and name[3] == 'AAA':
        #     group.to_csv('/src/data/lw_debug_group.csv', index=False)
        #     break
            

        df0 = group[['r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']].copy()

        std_values = df0.std(skipna=True).tolist()
        mean_values = df0.mean(skipna=True).tolist()
        cv_values = (df0.std(skipna=True) / df0.mean(skipna=True)).tolist()
        iqr_values = df0.quantile(0.75) - df0.quantile(0.25)
        iqr_values = iqr_values.tolist()

        # 将结果添加到resaultDf
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r3/r1': std_values[0],
            'std_r7/r3': std_values[1],
            'std_r14/r7': std_values[2],
            'std_r30/r14': std_values[3],
            'std_r60/r30': std_values[4],
            'std_r90/r60': std_values[5],
            'std_r120/r90': std_values[6],
            'std_r150/r120': std_values[7],
            'mean_r3/r1': mean_values[0],
            'mean_r7/r3': mean_values[1],
            'mean_r14/r7': mean_values[2],
            'mean_r30/r14': mean_values[3],
            'mean_r60/r30': mean_values[4],
            'mean_r90/r60': mean_values[5],
            'mean_r120/r90': mean_values[6],
            'mean_r150/r120': mean_values[7],
            'cv_r3/r1': cv_values[0],
            'cv_r7/r3': cv_values[1],
            'cv_r14/r7': cv_values[2],
            'cv_r30/r14': cv_values[3],
            'cv_r60/r30': cv_values[4],
            'cv_r90/r60': cv_values[5],
            'cv_r120/r90': cv_values[6],
            'cv_r150/r120': cv_values[7],
            'iqr_r3/r1': iqr_values[0],
            'iqr_r7/r3': iqr_values[1],
            'iqr_r14/r7': iqr_values[2],
            'iqr_r30/r14': iqr_values[3],
            'iqr_r60/r30': iqr_values[4],
            'iqr_r90/r60': iqr_values[5],
            'iqr_r120/r90': iqr_values[6],
            'iqr_r150/r120': iqr_values[7]
        }, ignore_index=True)

    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step2.csv', index=False)
    print('CV计算完成，结果已保存到 /src/data/lw_20250619_step2.csv')
        


if __name__ == "__main__":
    # step1()
    step2()
    print("Script executed successfully.")