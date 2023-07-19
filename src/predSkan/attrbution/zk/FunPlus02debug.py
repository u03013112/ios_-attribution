import pandas as pd
import matplotlib
from matplotlib import font_manager

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)


def getRetFromMC():
    sql = '''
        select *
        from topwar_ios_funplus02_raw
        where day > 0
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('topwar_ios_funplus02_raw'), index=False)

def getSsotRetFromMC():
    sql = '''
        select *
        from topwar_ios_funplus02_ssot_raw
        where day > 0
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('topwar_ios_funplus02_ssot_raw'), index=False)

def getSsotFromMC():
    sql = '''
        select
            campaign,
            revenue_d7_double,
            day
        from
            rg_ai_bj.ads_appsflyer_ssot
        where
            day > 20230401
            and revenue_d7_double > 0
        ;
    '''
    df = execSqlBj(sql)
    df.to_csv(getFilename('ads_appsflyer_ssot'), index=False)

# 将ssot，融合归因，模糊归因的结论合并
def ssot2ret():
    df = pd.read_csv(getFilename('ads_appsflyer_ssot'))
    # df 列 campaign
    # campaign 中 如果有 'VO' 或者 ‘AEO’ media = ‘Facebook Ads’
    # campaign 中 如果有 'Tiktok' media = ‘bytedanceglobal_int’
    # campaign 中 如果有 'UAC' media = ‘googleadwords_int’
    # 其他的 media = ‘unknown’
    df['media'] = 'unknown'
    df.loc[df['campaign'].str.contains('VO|AEO'), 'media'] = 'Facebook Ads'
    df.loc[df['campaign'].str.contains('Tiktok'), 'media'] = 'bytedanceglobal_int'
    df.loc[df['campaign'].str.contains('UAC'), 'media'] = 'googleadwords_int'
    df = df.groupby(['media', 'day']).sum().reset_index()
    # df.to_csv(getFilename('ads_appsflyer_ssot_ret'), index=False)
    # 将df列day格式转换，从类似20230401转换为2023-04-01
    df['day'] = pd.to_datetime(df['day'], format='%Y%m%d')
    df['day'] = df['day'].dt.strftime('%Y-%m-%d')
    df.rename(columns={
        'revenue_d7_double':'SSOT 7日收入（美元）'
    }, inplace=True)
    df2 = pd.read_csv(getFilename('funplus02tSsot4'))
    df2.rename(columns={
        'install_date': 'day',
        '7_days_revenue':'融合归因+模糊归因 7日收入（美元）'
    }, inplace=True)
    df2.drop(columns=['roi'], inplace=True)

    df3 = pd.read_csv(getFilename('funplus02t3'))
    df3.rename(columns={
        'install_date': 'day',
        '7_days_revenue':'融合归因 7日收入（美元）'
    }, inplace=True)
    df3.drop(columns=['roi','cost'], inplace=True)
    
    df4 = pd.read_csv(getFilename('funplus02t3redownload'))
    df4.rename(columns={
        'install_date': 'day',
        '7_days_revenue':'融合归因（排除重下载） 7日收入（美元）'
    }, inplace=True)
    df4.drop(columns=['roi','cost'], inplace=True)

    df5 = pd.read_csv(getFilename('funplus02tSsot4redownload'))
    df5.rename(columns={
        'install_date': 'day',
        '7_days_revenue':'融合归因+模糊归因（排除重下载） 7日收入（美元）'
    }, inplace=True)
    df5.drop(columns=['roi','cost'], inplace=True)

    df6 = pd.read_csv(getFilename('funplus02t3Adv'))
    df6.rename(columns={
        'install_date': 'day',
        '7_days_revenue':'融合归因Adv 7日收入（美元）'
    }, inplace=True)
    df6.drop(columns=['roi','cost'], inplace=True)

    mergeDf = df.merge(df2, how='left', on=['media', 'day'])
    mergeDf = mergeDf.merge(df3, how='left', on=['media', 'day'])
    mergeDf = mergeDf.merge(df4, how='left', on=['media', 'day'])
    mergeDf = mergeDf.merge(df5, how='left', on=['media', 'day'])
    mergeDf = mergeDf.merge(df6, how='left', on=['media', 'day'])

    mergeDf.to_csv(getFilename('funplus02tSsot4Ret'), index=False)

import matplotlib.pyplot as plt
def draw():
    df = pd.read_csv(getFilename('funplus02tSsot4Ret'))
    matplotlib.font_manager._rebuild()

    font = font_manager.FontProperties(fname='/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc')
    plt.rcParams['font.family'] = font.get_name()

    import numpy as np
    def mape(y_true, y_pred):
        return np.mean(np.abs((y_pred - y_true) / y_true)) * 100

    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        df1['day'] = pd.to_datetime(df1['day'])
        df1 = df1.set_index('day')
        df1 = df1.sort_index()
        df1 = df1.rolling(7).mean()
        df1 = df1.reset_index()
        # df1.to_csv(getFilename('funplus02tSsot7_%s' % media), index=False)
        print(media)
        # print(df1.corr())
        print('SSOT 7日收入（美元） 与 融合归因+模糊归因 7日收入（美元） 相关系数：', df1['SSOT 7日收入（美元）'].corr(df1['融合归因+模糊归因 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因+模糊归因（排除重下载） 7日收入（美元） 相关系数：', df1['SSOT 7日收入（美元）'].corr(df1['融合归因+模糊归因（排除重下载） 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因 7日收入（美元） 相关系数：', df1['SSOT 7日收入（美元）'].corr(df1['融合归因 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因（排除重下载） 7日收入（美元） 相关系数：', df1['SSOT 7日收入（美元）'].corr(df1['融合归因（排除重下载） 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因Adv 7日收入（美元） 相关系数：', df1['SSOT 7日收入（美元）'].corr(df1['融合归因Adv 7日收入（美元）']))
        print('\n')
        print('SSOT 7日收入（美元） 与 融合归因+模糊归因 7日收入（美元） MAPE：', mape(df1['SSOT 7日收入（美元）'], df1['融合归因+模糊归因 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因+模糊归因（排除重下载） 7日收入（美元） MAPE：', mape(df1['SSOT 7日收入（美元）'], df1['融合归因+模糊归因（排除重下载） 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因 7日收入（美元） MAPE：', mape(df1['SSOT 7日收入（美元）'], df1['融合归因 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因（排除重下载） 7日收入（美元） MAPE：', mape(df1['SSOT 7日收入（美元）'], df1['融合归因（排除重下载） 7日收入（美元）']))
        print('SSOT 7日收入（美元） 与 融合归因Adv 7日收入（美元） MAPE：', mape(df1['SSOT 7日收入（美元）'], df1['融合归因Adv 7日收入（美元）']))
        print('\n')
        print('融合归因+模糊归因 7日收入（美元）/SSOT 7日收入（美元） = ', df1['融合归因+模糊归因 7日收入（美元）'].sum() / df1['SSOT 7日收入（美元）'].sum())
        print('融合归因+模糊归因（排除重下载） 7日收入（美元）/SSOT 7日收入（美元） = ', df1['融合归因+模糊归因（排除重下载） 7日收入（美元）'].sum() / df1['SSOT 7日收入（美元）'].sum())
        print('融合归因 7日收入（美元）/SSOT 7日收入（美元） = ', df1['融合归因 7日收入（美元）'].sum() / df1['SSOT 7日收入（美元）'].sum())
        print('融合归因（排除重下载） 7日收入（美元）/SSOT 7日收入（美元） = ', df1['融合归因（排除重下载） 7日收入（美元）'].sum() / df1['SSOT 7日收入（美元）'].sum())
        print('融合归因Adv 7日收入（美元）/SSOT 7日收入（美元） = ', df1['融合归因Adv 7日收入（美元）'].sum() / df1['SSOT 7日收入（美元）'].sum())

        # 绘制图形
        plt.figure(figsize=(18, 6))
        plt.plot(df1['day'], df1['SSOT 7日收入（美元）'], label='SSOT 7日收入（美元）')
        plt.plot(df1['day'], df1['融合归因+模糊归因 7日收入（美元）'], label='融合归因+模糊归因 7日收入（美元）')
        plt.plot(df1['day'], df1['融合归因 7日收入（美元）'], label='融合归因 7日收入（美元）')
        plt.plot(df1['day'], df1['融合归因（排除重下载） 7日收入（美元）'], label='融合归因（排除重下载） 7日收入（美元）')
        plt.plot(df1['day'], df1['融合归因+模糊归因（排除重下载） 7日收入（美元）'], label='融合归因+模糊归因（排除重下载） 7日收入（美元）')
        plt.plot(df1['day'], df1['融合归因Adv 7日收入（美元）'], label='融合归因Adv 7日收入（美元）')
        
        plt.xlabel('Install Date')
        plt.ylabel('7-Day 收入（美元）')
        plt.legend()
        plt.title('7-Day 收入（美元）对比图（media=%s）' % media)

        # 保存图形
        plt.savefig(getFilename('funplus02tSsot7_%s' % media, ext='jpg'))
        # 清理图形，以便下次绘制
        plt.clf()

def checkRet1():
    df = pd.read_csv(getFilename('topwar_ios_funplus02_raw'))
    
    dfCol = df.columns.tolist()
    dfCol.remove('appsflyer_id')
    dfCol.remove('install_date')
    dfCol.remove('day')

    for col in dfCol:
        # 打印这一列大于1的行数，以及总行数，以及比例
        l1 = len(df[df[col] > 1])
        l2 = len(df)
        print(col, l1, l2, l1/l2)
    
    # 打印dfCol中所有列的和大于1的行数，以及总行数，以及比例
    df['total'] = df[dfCol].sum(axis=1)
    l1 = len(df[df['total'] > 1.5])
    l2 = len(df)
    print('total', l1, l2, l1/l2)

def checkSsotRet1():
    df = pd.read_csv(getFilename('topwar_ios_funplus02_ssot_raw'))
    
    dfCol = df.columns.tolist()
    dfCol.remove('appsflyer_id')
    dfCol.remove('install_date')
    dfCol.remove('day')

    for col in dfCol:
        # 打印这一列大于1的行数，以及总行数，以及比例
        l1 = len(df[df[col] > 1])
        l2 = len(df)
        print(col, l1, l2, l1/l2)
    
    # 打印dfCol中所有列的和大于1的行数，以及总行数，以及比例
    df['total'] = df[dfCol].sum(axis=1)
    l1 = len(df[df['total'] > 1.5])
    l2 = len(df)
    print('total', l1, l2, l1/l2)


if __name__ == '__main__':
    # getRetFromMC()
    # getSsotRetFromMC()
    # checkRet1()
    # checkSsotRet1()
    # getSsotFromMC()
    ssot2ret()
    draw()


