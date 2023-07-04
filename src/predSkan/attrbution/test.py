import pandas as pd

# df = pd.read_csv('/src/data/zk2/attribution1ReStep24hoursGeo.csv')
# df['total'] = df['googleadwords_int count'] + df['Facebook Ads count'] + df['bytedanceglobal_int count'] + df['snapchat_int count']
# print(len(df[df['total'] > 1]))
# print(len(df))

import sys
sys.path.append('/src')
from src.maxCompute import execSql


sql = '''
    select *
    from topwar_ios_funplus02_raw
    where day > 0
    and install_date = '2023-06-20';
'''

# df = execSql(sql)
# df.to_csv('/src/data/zk2/topwar_ios_funplus02_raw.csv', index=False)

df = pd.read_csv('/src/data/zk2/topwar_ios_funplus02_raw.csv')
df['total'] = df['googleadwords_int count'] + df['facebook ads count'] + df['bytedanceglobal_int count']
l1 = len(df[df['total'] > 1])
l2 = len(df)
print(l1,l2,l1/l2)
