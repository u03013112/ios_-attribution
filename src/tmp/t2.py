import pandas as pd

userDf = pd.read_csv('/src/data/zk/userDf2.csv',dtype={'customer_user_id':str})
# skanDf = pd.read_csv('/src/data/zk/skanDf2.csv')


ret = userDf.loc[
    (userDf['install_timestamp'] >= 1698454800) &
    (userDf['install_timestamp'] <= 1698627600) &
    (userDf['cv'] == 7) 
]

print(ret)

# print(userDf['install_timestamp'].min())
# print(userDf['install_timestamp'].max())