# af 对数
# skansdkcvupdatelog.csv是AF的上传日志
# 和数数打点做对比
# 1、直接和ods_platform_appsflyer_events中用户数、金额
# 2、和数数打点做对比，s_af_sdk_update_skan用户数、金额
# 3、和BI的24小时付费用户数、金额做对比

import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj

def getAFData():
    # 读取AF的上传日志
    df = pd.read_csv('skansdkcvupdatelog.csv')
    # 拥有列
    # "appsflyer_id","install_time","timestamp","event_name","event_revenue_u_s_d","conversion_value"
    
    # 将 event_revenue_u_s_d 列转换为浮点数，无法转换的值替换为 0
    df['event_revenue_u_s_d'] = pd.to_numeric(df['event_revenue_u_s_d'], errors='coerce').fillna(0)
    
    # 从 install_time 中提取 install_date
    df['install_date'] = df['install_time'].str[:10]

    # 按 install_date 分组并进行聚合
    df = df.groupby('install_date').agg(
        {
            'appsflyer_id': 'nunique',
            'event_revenue_u_s_d': 'sum'
        }
    ).reset_index()

    # 重命名列以更好地反映其内容
    df.rename(columns={'appsflyer_id': 'af_user_count', 'event_revenue_u_s_d': 'af_total_revenue'}, inplace=True)

    return df

def getSSData():
    # 读取 CSV 文件
    df = pd.read_csv('ss_20240603_20240702.csv')
    
    # 重命名列以便于处理
    df.columns = [
        'install_date', 
        's_af_sdk_update_skan_user_count', 
        's_af_sdk_update_skan_total_revenue', 
        'ss_pay_user_count', 
        'ss_pay_total_revenue'
    ]
    
    # 将数值列转换为浮点数
    df['s_af_sdk_update_skan_total_revenue'] = pd.to_numeric(df['s_af_sdk_update_skan_total_revenue'], errors='coerce').fillna(0)
    df['ss_pay_total_revenue'] = pd.to_numeric(df['ss_pay_total_revenue'], errors='coerce').fillna(0)
    
    # 将用户数列转换为整数
    df['s_af_sdk_update_skan_user_count'] = pd.to_numeric(df['s_af_sdk_update_skan_user_count'], errors='coerce').fillna(0).astype(int)
    df['ss_pay_user_count'] = pd.to_numeric(df['ss_pay_user_count'], errors='coerce').fillna(0).astype(int)
    
    # 返回处理后的 DataFrame
    df.sort_values(by='install_date', inplace=True)
    return df

def getBIData():
    df = pd.read_csv('bi_20240703_1617.csv')
    # 重命名列以便于处理
    df.columns = [
        'install_date', 
        'cost',
        'roi24', 
        'bi_pay_total_revenue'
    ]

    df = df[['install_date', 'bi_pay_total_revenue']]
    # install_date 从类似 20240703 转换为 2024-07-03
    df['install_date'] = pd.to_datetime(df['install_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df.sort_values(by='install_date', inplace=True)
    return df

def check1():
    afDf = getAFData()
    ssDf = getSSData()
    biDf = getBIData()

    df = pd.merge(afDf, ssDf, on='install_date', how='left')
    df = pd.merge(df, biDf, on='install_date', how='left')

    
    df['af and ss1 mape'] = abs(df['af_total_revenue'] - df['s_af_sdk_update_skan_total_revenue']) / df['s_af_sdk_update_skan_total_revenue']
    df['af and ss2 mape'] = abs(df['af_total_revenue'] - df['ss_pay_total_revenue']) / df['ss_pay_total_revenue']
    df['af and bi mape'] = abs(df['af_total_revenue'] - df['bi_pay_total_revenue']) / df['bi_pay_total_revenue']

    print('af and ss1 mape = ', df['af and ss1 mape'].mean())
    print('af and ss2 mape = ', df['af and ss2 mape'].mean())
    print('af and bi mape = ', df['af and bi mape'].mean())

    df.to_csv('/src/data/zk2/check1.csv', index=False)
    # 将af_total_revenue,s_af_sdk_update_skan_total_revenue,ss_pay_total_revenue,bi_pay_total_revenue
    # 画在一张图上，用install_date转为日期后作为x轴
    # 保存到 /src/data/zk2/check1.png

    # 将 install_date 转换为日期类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 设置 install_date 为索引
    df.set_index('install_date', inplace=True)

    # 绘制图表
    plt.figure(figsize=(10, 6))
    plt.plot(df.index, df['af_total_revenue'], label='AF Total Revenue')
    plt.plot(df.index, df['s_af_sdk_update_skan_total_revenue'], label='SDK Update SKAN Total Revenue')
    plt.plot(df.index, df['ss_pay_total_revenue'], label='SS Pay Total Revenue')
    plt.plot(df.index, df['bi_pay_total_revenue'], label='BI Pay Total Revenue')

    plt.xlabel('Install Date')
    plt.ylabel('Total Revenue')
    plt.title('Total Revenue Over Time')
    plt.legend()
    plt.grid(True)

    # 保存图表
    plt.savefig('/src/data/zk2/check1.png')


if __name__ == '__main__':
    check1()
    
