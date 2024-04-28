import pandas as pd


skanDf = pd.read_csv('skan.csv')
skanDf.rename(columns={'af_order_id':'order_id'}, inplace=True)
purchaseDf = pd.read_csv('purchase.csv')
purchaseDf.rename(columns={'平台订单ID':'order_id'}, inplace=True)

df = pd.merge(purchaseDf, skanDf, how='outer', on='order_id', suffixes=('_purchase', '_skan'))

# 找到skan中有，purchase中没有的
df1 = df[df['账户ID_purchase'].isnull() & df['账户ID_skan'].notnull()]
print(df1[['order_id']])
print(len(df1))

# 找到purchase中有，skan中没有的
df2 = df[df['账户ID_purchase'].notnull() & df['账户ID_skan'].isnull()]
print(df2[['order_id']])
print(len(df2))