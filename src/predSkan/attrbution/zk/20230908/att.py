# 归因，用iOS那套代码
# 将归因细化到用户，并且将过分配的部分去掉，保持一定程度的欠分配

import pandas as pd

import sys
sys.path.append('/src')

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)


userDf = pd.read_csv(getFilename('attribution1ReStep24hoursGeo'))
userDf['total_count'] = userDf['googleadwords_int count'] + userDf['Facebook Ads count'] + userDf['bytedanceglobal_int count'] + userDf['snapchat_int count']
# print(userDf.head(5))
print('过分配数量：',len(userDf.loc[userDf['total_count']>1]))
print('过分配用户数：',userDf.loc[userDf['total_count']>1]['user_count'].sum())
print(userDf.loc[userDf['total_count']>1][['cv','googleadwords_int count','Facebook Ads count','bytedanceglobal_int count','snapchat_int count','total_count']].head(5))
print('用户总数：',userDf['user_count'].sum())
print('过分配用户占比：',userDf.loc[userDf['total_count']>1]['user_count'].sum()/userDf['user_count'].sum())