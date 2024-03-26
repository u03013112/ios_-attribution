# lw 48小时 skan对于 24小时 skan的 优势



# 主要体现在与7日回收的线性相关性的提升
# 进一步比较直接的感受是，直接用一个倍率系数，来计算7日回收，24小时与48小时的 结果，按照天为单位，计算MAPE
# 可以按照3天、7天进行rolling，计算MAPE
# 另外再计算一下相对14日回收的MAPE

import os
import io
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getLwIosPayData(startDayStr,endDayStr):
    filename = f'/src/data/lwIosPayData_48hours_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SELECT
    install_day,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 24h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 48 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 48h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 7 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 168h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 14 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 336h_revenue_usd
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
GROUP BY
    install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 抽样，抽样比例rand<1
def getLwIosPayDataRAND(startDayStr,endDayStr,rand = 0.1):
    filename = f'/src/data/lwIosPayData_48hours_{startDayStr}_{endDayStr}_rand{rand}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SELECT
    install_day,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 24h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 48 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 48h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 7 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 168h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 14 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 336h_revenue_usd
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
    and rand() < {rand}
GROUP BY
    install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df



def corr(startDayStr,endDayStr):
    # df = getLwIosPayData(startDayStr,endDayStr)
    df = getLwIosPayDataRAND(startDayStr,endDayStr,rand=0.1)
    # 计算相关性
    print(df.corr()[['24h_revenue_usd','48h_revenue_usd']])

    # 针对24小时计算
    df24 = df.copy()
    # 针对168小时计算
    # 计算平均倍率系数 a168p24 = 168h_revenue_usd/24h_revenue_usd
    a168p24 = df24['168h_revenue_usd'].sum() / df24['24h_revenue_usd'].sum()
    print('a168p24:',a168p24)
    df24['168h_revenue_usd_pred_by_24h'] = df24['24h_revenue_usd'] * a168p24
    df24['mape24_168'] = (df24['168h_revenue_usd'] - df24['168h_revenue_usd_pred_by_24h']).abs() / df24['168h_revenue_usd']
    print('mape24_168:',df24['mape24_168'].mean())
    
    # # rolling 3 计算MAPE
    # df24['24h_revenue_usd rolling 3'] = df24['24h_revenue_usd'].rolling(3).mean()
    # df24['168h_revenue_usd rolling 3'] = df24['168h_revenue_usd'].rolling(3).mean()
    # a168p24rolling3 = df24['168h_revenue_usd rolling 3'].sum() / df24['24h_revenue_usd rolling 3'].sum()
    # print('a168p24rolling3:',a168p24rolling3)
    # df24['168h_revenue_usd_pred_by_24h rolling 3'] = df24['24h_revenue_usd rolling 3'] * a168p24rolling3
    # df24['mape24_168 rolling 3'] = (df24['168h_revenue_usd rolling 3'] - df24['168h_revenue_usd_pred_by_24h rolling 3']).abs() / df24['168h_revenue_usd rolling 3']
    # print('mape24_168 rolling 3:',df24['mape24_168 rolling 3'].mean())
    
    # # rolling 7 计算MAPE
    # df24['24h_revenue_usd rolling 7'] = df24['24h_revenue_usd'].rolling(7).mean()
    # df24['168h_revenue_usd rolling 7'] = df24['168h_revenue_usd'].rolling(7).mean()
    # a168p24rolling7 = df24['168h_revenue_usd rolling 7'].sum() / df24['24h_revenue_usd rolling 7'].sum()
    # print('a168p24rolling7:',a168p24rolling7)
    # df24['168h_revenue_usd_pred_by_24h rolling 7'] = df24['24h_revenue_usd rolling 7'] * a168p24rolling7
    # df24['mape24_168 rolling 7'] = (df24['168h_revenue_usd rolling 7'] - df24['168h_revenue_usd_pred_by_24h rolling 7']).abs() / df24['168h_revenue_usd rolling 7']
    # print('mape24_168 rolling 7:',df24['mape24_168 rolling 7'].mean())
    
    # 针对336小时计算
    # 计算平均倍率系数 a336p24 = 336h_revenue_usd/24h_revenue_usd
    a336p24 = df24['336h_revenue_usd'].sum() / df24['24h_revenue_usd'].sum()
    print('a336p24:',a336p24)
    df24['336h_revenue_usd_pred_by_24h'] = df24['24h_revenue_usd'] * a336p24
    df24['mape24_336'] = (df24['336h_revenue_usd'] - df24['336h_revenue_usd_pred_by_24h']).abs() / df24['336h_revenue_usd']
    print('mape24_336:',df24['mape24_336'].mean())
    
    # # rolling 3 计算MAPE
    # df24['24h_revenue_usd rolling 3'] = df24['24h_revenue_usd'].rolling(3).mean()
    # df24['336h_revenue_usd rolling 3'] = df24['336h_revenue_usd'].rolling(3).mean()
    # a336p24rolling3 = df24['336h_revenue_usd rolling 3'].sum() / df24['24h_revenue_usd rolling 3'].sum()
    # print('a336p24rolling3:',a336p24rolling3)
    # df24['336h_revenue_usd_pred_by_24h rolling 3'] = df24['24h_revenue_usd rolling 3'] * a336p24rolling3
    # df24['mape24_336 rolling 3'] = (df24['336h_revenue_usd rolling 3'] - df24['336h_revenue_usd_pred_by_24h rolling 3']).abs() / df24['336h_revenue_usd rolling 3']
    # print('mape24_336 rolling 3:',df24['mape24_336 rolling 3'].mean())

    # # rolling 7 计算MAPE
    # df24['24h_revenue_usd rolling 7'] = df24['24h_revenue_usd'].rolling(7).mean()
    # df24['336h_revenue_usd rolling 7'] = df24['336h_revenue_usd'].rolling(7).mean()
    # a336p24rolling7 = df24['336h_revenue_usd rolling 7'].sum() / df24['24h_revenue_usd rolling 7'].sum()
    # print('a336p24rolling7:',a336p24rolling7)
    # df24['336h_revenue_usd_pred_by_24h rolling 7'] = df24['24h_revenue_usd rolling 7'] * a336p24rolling7
    # df24['mape24_336 rolling 7'] = (df24['336h_revenue_usd rolling 7'] - df24['336h_revenue_usd_pred_by_24h rolling 7']).abs() / df24['336h_revenue_usd rolling 7']
    # print('mape24_336 rolling 7:',df24['mape24_336 rolling 7'].mean())

    print('--------------------------------------')
    # 针对48小时计算
    df48 = df.copy()
    # 针对168小时计算
    # 计算平均倍率系数 a168p48 = 168h_revenue_usd/48h_revenue_usd
    a168p48 = df48['168h_revenue_usd'].sum() / df48['48h_revenue_usd'].sum()
    print('a168p48:',a168p48)
    df48['168h_revenue_usd_pred_by_48h'] = df48['48h_revenue_usd'] * a168p48
    df48['mape48_168'] = (df48['168h_revenue_usd'] - df48['168h_revenue_usd_pred_by_48h']).abs() / df48['168h_revenue_usd']
    print('mape48_168:',df48['mape48_168'].mean())
    
    # # rolling 3 计算MAPE
    # df48['48h_revenue_usd rolling 3'] = df48['48h_revenue_usd'].rolling(3).mean()
    # df48['168h_revenue_usd rolling 3'] = df48['168h_revenue_usd'].rolling(3).mean()
    # a168p48rolling3 = df48['168h_revenue_usd rolling 3'].sum() / df48['48h_revenue_usd rolling 3'].sum()
    # print('a168p48rolling3:',a168p48rolling3)
    # df48['168h_revenue_usd_pred_by_48h rolling 3'] = df48['48h_revenue_usd rolling 3'] * a168p48rolling3
    # df48['mape48_168 rolling 3'] = (df48['168h_revenue_usd rolling 3'] - df48['168h_revenue_usd_pred_by_48h rolling 3']).abs() / df48['168h_revenue_usd rolling 3']
    # print('mape48_168 rolling 3:',df48['mape48_168 rolling 3'].mean())

    # # rolling 7 计算MAPE
    # df48['48h_revenue_usd rolling 7'] = df48['48h_revenue_usd'].rolling(7).mean()
    # df48['168h_revenue_usd rolling 7'] = df48['168h_revenue_usd'].rolling(7).mean()
    # a168p48rolling7 = df48['168h_revenue_usd rolling 7'].sum() / df48['48h_revenue_usd rolling 7'].sum()
    # print('a168p48rolling7:',a168p48rolling7)
    # df48['168h_revenue_usd_pred_by_48h rolling 7'] = df48['48h_revenue_usd rolling 7'] * a168p48rolling7
    # df48['mape48_168 rolling 7'] = (df48['168h_revenue_usd rolling 7'] - df48['168h_revenue_usd_pred_by_48h rolling 7']).abs() / df48['168h_revenue_usd rolling 7']
    # print('mape48_168 rolling 7:',df48['mape48_168 rolling 7'].mean())

    # 针对336小时计算
    # 计算平均倍率系数 a336p48 = 336h_revenue_usd/48h_revenue_usd
    a336p48 = df48['336h_revenue_usd'].sum() / df48['48h_revenue_usd'].sum()
    print('a336p48:',a336p48)
    df48['336h_revenue_usd_pred_by_48h'] = df48['48h_revenue_usd'] * a336p48
    df48['mape48_336'] = (df48['336h_revenue_usd'] - df48['336h_revenue_usd_pred_by_48h']).abs() / df48['336h_revenue_usd']
    print('mape48_336:',df48['mape48_336'].mean())

    # # rolling 3 计算MAPE
    # df48['48h_revenue_usd rolling 3'] = df48['48h_revenue_usd'].rolling(3).mean()
    # df48['336h_revenue_usd rolling 3'] = df48['336h_revenue_usd'].rolling(3).mean()
    # a336p48rolling3 = df48['336h_revenue_usd rolling 3'].sum() / df48['48h_revenue_usd rolling 3'].sum()
    # print('a336p48rolling3:',a336p48rolling3)
    # df48['336h_revenue_usd_pred_by_48h rolling 3'] = df48['48h_revenue_usd rolling 3'] * a336p48rolling3
    # df48['mape48_336 rolling 3'] = (df48['336h_revenue_usd rolling 3'] - df48['336h_revenue_usd_pred_by_48h rolling 3']).abs() / df48['336h_revenue_usd rolling 3']
    # print('mape48_336 rolling 3:',df48['mape48_336 rolling 3'].mean())

    # # rolling 7 计算MAPE
    # df48['48h_revenue_usd rolling 7'] = df48['48h_revenue_usd'].rolling(7).mean()
    # df48['336h_revenue_usd rolling 7'] = df48['336h_revenue_usd'].rolling(7).mean()
    # a336p48rolling7 = df48['336h_revenue_usd rolling 7'].sum() / df48['48h_revenue_usd rolling 7'].sum()
    # print('a336p48rolling7:',a336p48rolling7)
    # df48['336h_revenue_usd_pred_by_48h rolling 7'] = df48['48h_revenue_usd rolling 7'] * a336p48rolling7
    # df48['mape48_336 rolling 7'] = (df48['336h_revenue_usd rolling 7'] - df48['336h_revenue_usd_pred_by_48h rolling 7']).abs() / df48['336h_revenue_usd rolling 7']
    # print('mape48_336 rolling 7:',df48['mape48_336 rolling 7'].mean())


    print('--------------------------------------')
    # 画图
    # 针对168小时画图
    df24['install_day'] = pd.to_datetime(df24['install_day'], format='%Y%m%d')
    df48['install_day'] = pd.to_datetime(df48['install_day'], format='%Y%m%d')
    fig, ax1 = plt.subplots(figsize=(16, 6))

    ax1.set_xlabel('install_day')
    ax1.set_ylabel('revenue')
    ax1.plot(df24['install_day'], df24['24h_revenue_usd'],label = '24h revenue usd')
    ax1.plot(df24['install_day'], df24['48h_revenue_usd'],label = '48h revenue usd')
    ax1.plot(df24['install_day'], df24['168h_revenue_usd'],label = '168h revenue usd')
    ax1.plot(df24['install_day'], df24['168h_revenue_usd_pred_by_24h'], linestyle='-.',label = '168h revenue usd pred by 24h',alpha=0.9)
    ax1.plot(df48['install_day'], df48['168h_revenue_usd_pred_by_48h'], linestyle='-.',label = '168h revenue usd pred by 48h',alpha=0.9)
    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))

    ax2 = ax1.twinx()  
    ax2.set_ylabel('MAPE', )  
    ax2.plot(df24['install_day'], df24['mape24_168'], linestyle='--',label = 'MAPE 24h',alpha=0.5)
    ax2.plot(df48['install_day'], df48['mape48_168'], linestyle='--',label = 'MAPE 48h',alpha=0.5)
    ax2.legend(loc='upper right', bbox_to_anchor=(1, 1))

    fig.tight_layout()  
    plt.savefig('/src/data/lwIosPayData_48hours_20240101_20240310_168h.png')
    plt.clf()

    # 针对336小时画图
    fig, ax1 = plt.subplots(figsize=(16, 6))

    ax1.set_xlabel('install_day')
    ax1.set_ylabel('revenue')
    ax1.plot(df24['install_day'], df24['24h_revenue_usd'],label = '24h revenue usd')
    ax1.plot(df24['install_day'], df24['48h_revenue_usd'],label = '48h revenue usd')
    ax1.plot(df24['install_day'], df24['336h_revenue_usd'],label = '336h revenue usd')
    ax1.plot(df24['install_day'], df24['336h_revenue_usd_pred_by_24h'], linestyle='-.',label = '336h revenue usd pred by 24h',alpha=0.9)
    ax1.plot(df48['install_day'], df48['336h_revenue_usd_pred_by_48h'], linestyle='-.',label = '336h revenue usd pred by 48h',alpha=0.9)
    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))

    ax2 = ax1.twinx()
    ax2.set_ylabel('MAPE', )
    ax2.plot(df24['install_day'], df24['mape24_336'], linestyle='--',label = 'MAPE 24h',alpha=0.5)
    ax2.plot(df48['install_day'], df48['mape48_336'], linestyle='--',label = 'MAPE 48h',alpha=0.5)
    ax2.legend(loc='upper right', bbox_to_anchor=(1, 1))

    fig.tight_layout()
    plt.savefig('/src/data/lwIosPayData_48hours_20240101_20240310_336h.png')
    plt.clf()


    

if __name__ == '__main__':
    startDayStr = '20240101'
    endDayStr = '20240310'
    corr(startDayStr,endDayStr)
    