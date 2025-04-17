# 不同的国家分布，对付费率、留存、付费留存的影响

import pandas as pd
import matplotlib.pyplot as plt

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
    df['user_rate'] = df.groupby('server_id_int')['pay_rate'].transform(lambda x: x / x.sum())

    df = df.sort_values(by=['countryGroup','user_rate'])

    countryGroupList = ['JP', 'ID', 'MY']
    for countryGroup in countryGroupList:
        # 画图,x轴是user_rate，y轴是pay_rate
        countryDf = df[df['countryGroup'] == countryGroup]
        plt.figure(figsize=(10, 6))


        # 保存图像 /src/data/20250416_step1_{countryGroup}.png


        




    
    




    

    return df


if __name__ == "__main__":
    step1()
