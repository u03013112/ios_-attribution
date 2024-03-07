# 法务需求
# LW与TW的活跃用户和付费用户的重叠情况

import pandas as pd

def step1():
    df = pd.read_csv('lastwar30.csv')
    # 将df['S登录.总次数']转为int类型，如果不能转换，则为0
    df['S登录.总次数'] = df['S登录.总次数'].apply(lambda x: int(x) if str(x).isdigit() else 0)
    activeDf = df[df['S登录.总次数']>=7]
    activeDf = activeDf[['lwu_android_gaid']]
    activeDf.to_csv('/src/data/lastwar_active_gaid.csv', index=False)

    df['S新支付.总次数'] = df['S新支付.总次数'].apply(lambda x: int(x) if str(x).isdigit() else 0)
    payDf = df[df['S新支付.总次数']>0]
    payDf = payDf[['lwu_android_gaid']]
    payDf.to_csv('/src/data/lastwar_pay_gaid.csv', index=False)


if __name__ == '__main__':
    step1()