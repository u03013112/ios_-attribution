import pandas as pd


gaidDf = pd.read_csv('topheros_gaid_20250123.csv')

# 去除"时间"列
gaidDf = gaidDf.drop(columns=['时间'])
gaidDf['真实登录.总次数'] = gaidDf['真实登录.总次数'].apply(lambda x: int(x) if x != '-' else 0)
gaidDf['支付.总次数'] = gaidDf['支付.总次数'].apply(lambda x: int(x) if x != '-' else 0)

loginDf = gaidDf[gaidDf['真实登录.总次数'] >= 7]
print(loginDf.head())
loginDf[['gaid']].to_csv('/src/data/topheros_login_gaid_20250123.csv', index=False)

payDf = gaidDf[gaidDf['支付.总次数'] > 0]
print(payDf.head())
payDf[['gaid']].to_csv('/src/data/topheros_pay_gaid_20250123.csv', index=False)