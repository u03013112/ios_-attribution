# 不同的国家分布，对付费率、留存、付费留存的影响

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 国家分布于付费率
def step1():
    filename = '日服国家分布2_20240101_20250416.csv'
    df = pd.read_csv(filename)

    df = df.rename(columns={
        '用户国家地区': 'country',
        '服务器ID': 'server_id',
        '分析指标': 'key',
        '2024-01-01至2025-04-16': 'cnt'
    })

    # server_id 是类似 'APS608'的字符串
    df['server_id_int'] = df['server_id'].str.extract('(\d+)').astype(int)

    # key 内容修改，'S新支付.触发用户数' -> pay,'S登录.触发用户数' -> login,'付费率' -> pay_rate
    df['key'] = df['key'].replace({
        'S新支付.触发用户数': 'pay',
        'S登录.触发用户数': 'login',
        '付费率': 'pay_rate'
    })

    # 国家只针对'ID','JP','MY',其他国家统一改为'OTHERS',并合并
    df['countryGroup'] = 'OTHERS'
    df.loc[df['country'] == 'ID', 'countryGroup'] = 'ID'
    df.loc[df['country'] == 'JP', 'countryGroup'] = 'JP'
    df.loc[df['country'] == 'MY', 'countryGroup'] = 'MY'

    df = df.groupby(['countryGroup', 'server_id_int', 'key']).agg({'cnt': 'sum'}).reset_index()
    df = df.fillna(0)

    # pivot table，输出列： countryGroup, server_id_int, pay, login, pay_rate
    df = df.pivot_table(
        index=['countryGroup', 'server_id_int'],
        columns='key',
        values='cnt',
        aggfunc='sum'
    ).reset_index()
    # 因为有国家合并，所以pay_rate 需要重新计算
    df['pay_rate'] = df['pay'] / df['login']
    

    # 按先找server_id_int 进行分组，计算不同countryGroup的user_rate
    df['user_rate'] = df.groupby('server_id_int')['login'].transform(lambda x: x / x.sum())

    df = df.sort_values(by=['countryGroup','user_rate'])
    df.to_csv('/src/data/20250416_step1.csv', index=False)

    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_rate
        countryDf = df[df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=countryDf, x='user_rate', y='pay_rate')
        plt.title(f'{countryGroup} user_rate vs pay_rate')
        plt.xlabel('user rate')
        plt.ylabel('pay rate')
        plt.savefig(f'/src/data/20250416_step1_{countryGroup}.png')
        plt.close()
        # 计算x，y的相关系数，并打印
        corr = countryDf['user_rate'].corr(countryDf['pay_rate'])
        print(f'{countryGroup} 用户占比 与 付费率 线性相关系数: {corr}')



def step2():
    step1Df = pd.read_csv('/src/data/20250416_step1.csv')

    # 7日留存数据读取
    retention7Df0 = pd.read_csv('留存7日_20230101_20250416.csv')
    retention7Df = retention7Df0[
        (retention7Df0['初始事件发生时间'] == '阶段值')
        & (retention7Df0['指标'] == '留存率')
    ].copy()

    retention7Df = retention7Df.rename(columns={
        '服务器ID':'server_id',
        '7日':'retention_7',
    })
    # 将server_id 不是以 'APS'开头的行删除
    retention7Df = retention7Df[retention7Df['server_id'].str.startswith('APS')]

    retention7Df['server_id_int'] = retention7Df['server_id'].str.extract('(\d+)').astype(int)
    retention7Df = retention7Df[['server_id_int', 'retention_7']]

    # retention_7 是类似"10.23%"的字符串，去掉百分号，转为float
    retention7Df['retention_7'] = retention7Df['retention_7'].str.replace('%', '')
    # 去掉百分号后，转为float，如果不能转为float，填充为0
    retention7Df['retention_7'] = pd.to_numeric(retention7Df['retention_7'], errors='coerce')

    step2_7Df = pd.merge(step1Df, retention7Df, on='server_id_int', how='left')
    step2_7Df = step2_7Df.fillna(0)

    step2_7Df.to_csv('/src/data/20250416_step2_7.csv', index=False)

    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_rate
        countryDf = step2_7Df[step2_7Df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=countryDf, x='user_rate', y='retention_7')
        plt.title(f'{countryGroup} user_rate vs retention_7')
        plt.xlabel('user rate')
        plt.ylabel('retention_7')
        plt.savefig(f'/src/data/20250416_step2_7{countryGroup}.png')
        plt.close()

        # 计算x，y的相关系数，并打印
        corr = countryDf['user_rate'].corr(countryDf['retention_7'])
        print(f'{countryGroup} 用户占比 与 7日留存 线性相关系数: {corr}')


def step3():
    step1Df = pd.read_csv('/src/data/20250416_step1.csv')

    # 3个月留存数据读取
    retention3Df0 = pd.read_csv('留存3月_20230101_20250416.csv')
    retention3Df = retention3Df0[
        (retention3Df0['初始事件发生时间'] == '阶段值')
        & (retention3Df0['指标'] == '留存率')
    ].copy()
    retention3Df = retention3Df.rename(columns={
        '服务器ID':'server_id',
        '3月':'retention_3',
    })
    # 将server_id 不是以 'APS'开头的行删除
    retention3Df = retention3Df[retention3Df['server_id'].str.startswith('APS')]
    retention3Df['server_id_int'] = retention3Df['server_id'].str.extract('(\d+)').astype(int)
    retention3Df = retention3Df[['server_id_int', 'retention_3']]
    # retention_3 是类似"10.23%"的字符串，去掉百分号，转为float
    retention3Df['retention_3'] = retention3Df['retention_3'].str.replace('%', '')
    # 去掉百分号后，转为float，如果不能转为float，填充为0
    retention3Df['retention_3'] = pd.to_numeric(retention3Df['retention_3'], errors='coerce')
    step3_3Df = pd.merge(step1Df, retention3Df, on='server_id_int', how='left')
    step3_3Df = step3_3Df.fillna(0)
    step3_3Df = step3_3Df[step3_3Df['retention_3'] > 0]
    step3_3Df.to_csv('/src/data/20250416_step3_3.csv', index=False)
    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_rate
        countryDf = step3_3Df[step3_3Df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=countryDf, x='user_rate', y='retention_3')
        plt.title(f'{countryGroup} user_rate vs retention_3')
        plt.xlabel('user rate')
        plt.ylabel('retention_3')
        plt.savefig(f'/src/data/20250416_step3_3{countryGroup}.png')
        plt.close()

        # 计算x，y的相关系数，并打印
        corr = countryDf['user_rate'].corr(countryDf['retention_3'])
        print(f'{countryGroup} 用户占比 与 3月留存 线性相关系数: {corr}')

def step4():
    step1Df = pd.read_csv('/src/data/20250416_step1.csv')

    # 7日付费留存数据读取
    retention7Df0 = pd.read_csv('付费留存7日_20230101_20250416.csv')
    retention7Df = retention7Df0[
        (retention7Df0['初始事件发生时间'] == '阶段值')
        & (retention7Df0['指标'] == '留存率')
    ].copy()
    retention7Df = retention7Df.rename(columns={
        '服务器ID':'server_id',
        '7日':'pay_retention_7',
    })
    # 将server_id 不是以 'APS'开头的行删除
    retention7Df = retention7Df[retention7Df['server_id'].str.startswith('APS')]
    retention7Df['server_id_int'] = retention7Df['server_id'].str.extract('(\d+)').astype(int)
    retention7Df = retention7Df[['server_id_int', 'pay_retention_7']]
    # retention_7 是类似"10.23%"的字符串，去掉百分号，转为float
    retention7Df['pay_retention_7'] = retention7Df['pay_retention_7'].str.replace('%', '')
    # 去掉百分号后，转为float，如果不能转为float，填充为0
    retention7Df['pay_retention_7'] = pd.to_numeric(retention7Df['pay_retention_7'], errors='coerce')
    step4_7Df = pd.merge(step1Df, retention7Df, on='server_id_int', how='left')
    step4_7Df = step4_7Df.fillna(0)
    step4_7Df = step4_7Df[step4_7Df['pay_retention_7'] > 0]
    step4_7Df.to_csv('/src/data/20250416_step4_7.csv', index=False)
    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_retention_7
        countryDf = step4_7Df[step4_7Df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=countryDf, x='user_rate', y='pay_retention_7')
        plt.title(f'{countryGroup} user_rate vs pay_retention_7')
        plt.xlabel('user rate')
        plt.ylabel('pay_retention_7')
        plt.savefig(f'/src/data/20250416_step4_7{countryGroup}.png')
        plt.close()

        # 计算x，y的相关系数，并打印
        corr = countryDf['user_rate'].corr(countryDf['pay_retention_7'])
        print(f'{countryGroup} 用户占比 与 7日付费留存 线性相关系数: {corr}')


def step5():
    step1Df = pd.read_csv('/src/data/20250416_step1.csv')

    # 3月付费留存数据读取
    retention3Df0 = pd.read_csv('付费留存3月_20230101_20250416.csv')
    retention3Df = retention3Df0[
        (retention3Df0['初始事件发生时间'] == '阶段值')
        & (retention3Df0['指标'] == '留存率')
    ].copy()
    retention3Df = retention3Df.rename(columns={
        '服务器ID':'server_id',
        '3月':'pay_retention_3',
    })
    # 将server_id 不是以 'APS'开头的行删除
    retention3Df = retention3Df[retention3Df['server_id'].str.startswith('APS')]
    retention3Df['server_id_int'] = retention3Df['server_id'].str.extract('(\d+)').astype(int)
    retention3Df = retention3Df[['server_id_int', 'pay_retention_3']]
    # retention_7 是类似"10.23%"的字符串，去掉百分号，转为float
    retention3Df['pay_retention_3'] = retention3Df['pay_retention_3'].str.replace('%', '')
    # 去掉百分号后，转为float，如果不能转为float，填充为0
    retention3Df['pay_retention_3'] = pd.to_numeric(retention3Df['pay_retention_3'], errors='coerce')
    step5_3Df = pd.merge(step1Df, retention3Df, on='server_id_int', how='left')
    step5_3Df = step5_3Df.fillna(0)
    step5_3Df = step5_3Df[step5_3Df['pay_retention_3'] > 0]
    step5_3Df.to_csv('/src/data/20250416_step5.csv', index=False)
    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_retention_7
        countryDf = step5_3Df[step5_3Df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=countryDf, x='user_rate', y='pay_retention_3')
        plt.title(f'{countryGroup} user_rate vs pay_retention_3')
        plt.xlabel('user rate')
        plt.ylabel('pay_retention_3')
        plt.savefig(f'/src/data/20250416_step5_{countryGroup}.png')
        plt.close()

        # 计算x，y的相关系数，并打印
        corr = countryDf['user_rate'].corr(countryDf['pay_retention_3'])
        print(f'{countryGroup} 用户占比 与 3月付费留存 线性相关系数: {corr}')
    


if __name__ == "__main__":
    step1()
    step2()
    step3()
    step4()
    step5()
