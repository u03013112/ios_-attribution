import pandas as pd
import matplotlib.pyplot as plt
import os

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def plot_and_save(df, columns, filename):
    # 将installDate从类似20240707的str，转成日期，并作为x。按照日期升序重排数据。
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    df = df.sort_values(by='installDate')
    
    # 绘制图表
    plt.figure(figsize=(10, 6))
    for column in columns:
        plt.plot(df['installDate'], df[column], label=column)
    
    plt.xlabel('Install Date')
    plt.ylabel('Value')
    plt.title(f'{filename} Over Time')
    plt.legend()
    plt.grid(True)
    
    # 保存图表
    plt.savefig(f'/src/data/tw_{filename}.png')
    plt.close()

def getData():
    skanAfDf = pd.read_csv('Topwar_skanAf_20240101_20240716.csv')
    skanAfDf = skanAfDf[['installDate', 'installs','24hROI']]
    skanAfDf.rename(columns={'installs':'skanInstalls', '24hROI':'skan24hROI'}, inplace=True)

    mergeDf = getMergeData()
    # installs 取整后转为 str
    mergeDf['installs'] = mergeDf['installs'].astype(int).apply(lambda x: '{:,}'.format(x))
    mergeDf = mergeDf[['installDate', 'installs','24hROI','7dROI','r1usd','r7usd']]
    mergeDf.rename(columns={
        'installs':'mergeInstalls', 
        '24hROI':'merge24hROI', 
        '7dROI':'merge7dROI',
        'r1usd':'merge24hRevenue',
        'r7usd':'merge7dRevenue'
    }, inplace=True)


    iOSDf = pd.read_csv('Topwar_iOS_20240101_20240716.csv')
    iOSDf = iOSDf[['installDate', 'cost', 'installs','24hROI','7dROI']]
    iOSDf.rename(columns={'installs':'iOSInstalls', '24hROI':'iOS24hROI', '7dROI':'iOS7dROI'}, inplace=True)

    df = pd.merge(iOSDf, mergeDf, on='installDate', how='inner')
    df = pd.merge(df, skanAfDf, on='installDate', how='inner')
    df = df[[
        'installDate', 'cost',
        'iOSInstalls', 'mergeInstalls', 'skanInstalls',
        'iOS24hROI', 'merge24hROI', 'skan24hROI', 
        'iOS7dROI', 'merge7dROI',
        'merge24hRevenue', 'merge7dRevenue']]
    
    df['installDate'] = df['installDate'].astype(str)
    df = df[df['installDate'] < '20240708']

    # 计算revenue
    df['iOS24hRevenue'] = df['cost'] * df['iOS24hROI'].str.replace('%', '').astype(float) / 100
    # df['merge24hRevenue'] = df['cost'] * df['merge24hROI'].str.replace('%', '').astype(float) / 100
    df['skan24hRevenue'] = df['cost'] * df['skan24hROI'].str.replace('%', '').astype(float) / 100
    df['iOS7dRevenue'] = df['cost'] * df['iOS7dROI'].str.replace('%', '').astype(float) / 100
    # df['merge7dRevenue'] = df['cost'] * df['merge7dROI'].str.replace('%', '').astype(float) / 100

    df.to_csv('/src/data/tw_20240716_skanVsTotal.csv', index=False)

def getData2():
    filename = '/src/data/tw_merge_20240101_20240716.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        sql = f'''
SET
    odps.sql.timezone = Africa / Accra;

set
    odps.sql.hive.compatible = true;

@rhData :=
select
    customer_user_id,
    media,
    rate
from
    topwar_ios_funplus02_adv_uid_mutidays_media
where
    day between '20240101'
    and '20240716'
;

@biData :=
SELECT
    game_uid as customer_user_id,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as r1usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as r7usd,
    install_day as install_date
FROM
    rg_bi.ads_topwar_ios_purchase_adv
WHERE
    game_uid IS NOT NULL
GROUP BY
    game_uid,
    install_day
;

select
    sum(bi.r1usd * rh.rate) as r1usd,
    sum(bi.r7usd * rh.rate) as r7usd,
    bi.install_date,
    sum(rh.rate) as installs
from
    @rhData as rh
    left join @biData as bi on rh.customer_user_id = bi.customer_user_id
group by
    bi.install_date
;
    '''
        df = execSql(sql)
        df = df.loc[(df['install_date'] >= '20240101') & (df['install_date'] < '20240708')]
        df.rename(columns={'install_date':'installDate'}, inplace=True)
        df.to_csv(filename, index=False)
    
    return df

def getMergeData():
    df = getData2()
    df = df[['installDate', 'installs','r1usd','r7usd']]
    df = df.sort_values(by='installDate')
    
    costDf = pd.read_csv('Topwar_iOS_20240101_20240716.csv')
    costDf = costDf[['installDate', 'cost']]

    df = pd.merge(df, costDf, on='installDate', how='inner')
    df['24hROI'] = df['r1usd'] / df['cost']
    df['7dROI'] = df['r7usd'] / df['cost']

    df['24hROI'] = df['24hROI'].apply(lambda x: '%.2f%%'%(x*100))
    df['7dROI'] = df['7dROI'].apply(lambda x: '%.2f%%'%(x*100))

    # print(df)
    # df = df[['installDate', 'installs','cost', '24hROI', '7dROI']]

    return df

# 按天比对
def check1():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = df[['installDate','cost','iOSInstalls','mergeInstalls','skanInstalls']].copy()

    installDf['mergeInstalls'] = installDf['mergeInstalls'].str.replace(',', '').astype(int)
    installDf['skanInstalls'] = installDf['skanInstalls'].str.replace(',', '').astype(int)

    print(installDf[['cost','iOSInstalls', 'mergeInstalls', 'skanInstalls']].corr())
    plot_and_save(installDf, ['iOSInstalls', 'mergeInstalls', 'skanInstalls'], 'skanVsTotal_install')

    # iOS24hROI,merge24hROI,skan24hROI 的相关性
    roi24hDf = df[['installDate','iOS24hROI','merge24hROI','skan24hROI']].copy()
    roi24hDf['iOS24hROI'] = roi24hDf['iOS24hROI'].str.replace('%', '').astype(float)
    roi24hDf['merge24hROI'] = roi24hDf['merge24hROI'].str.replace('%', '').astype(float)
    roi24hDf['skan24hROI'] = roi24hDf['skan24hROI'].str.replace('%', '').astype(float)

    print(roi24hDf[['iOS24hROI', 'merge24hROI', 'skan24hROI']].corr())
    plot_and_save(roi24hDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI'], 'skanVsTotal_roi24hDf')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
    roi7dDf = df[['installDate','iOS24hROI','merge24hROI','skan24hROI','iOS7dROI','merge7dROI']].copy()
    roi7dDf['iOS24hROI'] = roi7dDf['iOS24hROI'].str.replace('%', '').astype(float)
    roi7dDf['merge24hROI'] = roi7dDf['merge24hROI'].str.replace('%', '').astype(float)
    roi7dDf['skan24hROI'] = roi7dDf['skan24hROI'].str.replace('%', '').astype(float)
    roi7dDf['iOS7dROI'] = roi7dDf['iOS7dROI'].str.replace('%', '').astype(float)
    roi7dDf['merge7dROI'] = roi7dDf['merge7dROI'].str.replace('%', '').astype(float)

    print(roi7dDf[['iOS24hROI', 'merge24hROI', 'skan24hROI', 'iOS7dROI', 'merge7dROI']].corr())
    plot_and_save(roi7dDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI', 'iOS7dROI', 'merge7dROI'], 'skanVsTotal_roi7dDf')

def check2():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # 将installDate从类似20240707的str，转成日期
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    
    # 按周进行汇总
    df['week'] = df['installDate'].dt.to_period('W').apply(lambda r: r.start_time)
    
    # 汇总数据
    weekly_df = df.groupby('week').agg({
        'cost': 'sum',
        'iOSInstalls': 'sum',
        'mergeInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hRevenue': 'sum',
        'merge24hRevenue': 'sum',
        'skan24hRevenue': 'sum',
        'iOS7dRevenue': 'sum',
        'merge7dRevenue': 'sum'
    }).reset_index()
    
    # 计算ROI
    weekly_df['iOS24hROI'] = (weekly_df['iOS24hRevenue'] / weekly_df['cost']) * 100
    weekly_df['merge24hROI'] = (weekly_df['merge24hRevenue'] / weekly_df['cost']) * 100
    weekly_df['skan24hROI'] = (weekly_df['skan24hRevenue'] / weekly_df['cost']) * 100
    weekly_df['iOS7dROI'] = (weekly_df['iOS7dRevenue'] / weekly_df['cost']) * 100
    weekly_df['merge7dROI'] = (weekly_df['merge7dRevenue'] / weekly_df['cost']) * 100
    
    # 将 week 列重命名为 installDate
    weekly_df.rename(columns={'week': 'installDate'}, inplace=True)
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = weekly_df[['installDate','cost','iOSInstalls','mergeInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls', 'mergeInstalls', 'skanInstalls'], 'skanVsTotal_install2')

    # # iOS24hROI,merge24hROI,skan24hROI 的相关性
    # roi24hDf = weekly_df[['installDate','iOS24hROI','merge24hROI','skan24hROI']].copy()
    # print(roi24hDf.corr())
    # plot_and_save(roi24hDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI'], 'skanVsTotal_roi24hDf2')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
    roi7dDf = weekly_df[['installDate','iOS24hROI','merge24hROI','skan24hROI','iOS7dROI','merge7dROI']].copy()
    print(roi7dDf.corr())
    plot_and_save(roi7dDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI', 'iOS7dROI', 'merge7dROI'], 'skanVsTotal_roi7dDf2')
    roi7dDf['iOS7dROI/merge24hROI'] = roi7dDf['iOS7dROI'] / roi7dDf['merge24hROI']
    print(roi7dDf)

def check3():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # 将installDate从类似20240707的str，转成日期
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    
    # 按月进行汇总
    df['month'] = df['installDate'].dt.to_period('M').apply(lambda r: r.start_time)
    
    # 汇总数据
    monthly_df = df.groupby('month').agg({
        'cost': 'sum',
        'iOSInstalls': 'sum',
        'mergeInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hRevenue': 'sum',
        'merge24hRevenue': 'sum',
        'skan24hRevenue': 'sum',
        'iOS7dRevenue': 'sum',
        'merge7dRevenue': 'sum'
    }).reset_index()
    
    # 计算ROI
    monthly_df['iOS24hROI'] = (monthly_df['iOS24hRevenue'] / monthly_df['cost']) * 100
    monthly_df['merge24hROI'] = (monthly_df['merge24hRevenue'] / monthly_df['cost']) * 100
    monthly_df['skan24hROI'] = (monthly_df['skan24hRevenue'] / monthly_df['cost']) * 100
    monthly_df['iOS7dROI'] = (monthly_df['iOS7dRevenue'] / monthly_df['cost']) * 100
    monthly_df['merge7dROI'] = (monthly_df['merge7dRevenue'] / monthly_df['cost']) * 100
    
    # 将 month 列重命名为 installDate
    monthly_df.rename(columns={'month': 'installDate'}, inplace=True)
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = monthly_df[['installDate','cost','iOSInstalls','mergeInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls', 'mergeInstalls', 'skanInstalls'], 'skanVsTotal_install3')

    # iOS24hROI,merge24hROI,skan24hROI 的相关性
    roi24hDf = monthly_df[['installDate','iOS24hROI','merge24hROI','skan24hROI']].copy()
    print(roi24hDf.corr())
    plot_and_save(roi24hDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI'], 'skanVsTotal_roi24hDf3')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
    roi7dDf = monthly_df[['installDate','iOS24hROI','merge24hROI','skan24hROI','iOS7dROI','merge7dROI']].copy()
    print(roi7dDf.corr())
    plot_and_save(roi7dDf, ['iOS24hROI', 'merge24hROI', 'skan24hROI', 'iOS7dROI', 'merge7dROI'], 'skanVsTotal_roi7dDf3')

    roi7dDf['iOS7dROI/merge24hROI'] = roi7dDf['iOS7dROI'] / roi7dDf['merge24hROI']
    print(roi7dDf)

if __name__ == '__main__':
    getData()
    
    check1()
    check2()
    check3()
