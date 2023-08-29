import pandas as pd

# 创建示例attributeDf
attributeDf = pd.DataFrame({'user index': [0, 0,0, 1, 1, 2, 2],
                            'media': ['A', 'B', 'A','A', 'B', 'A', 'B'],
                            'rate': [0.1, 0.2,0.5, 0.3, 0.4, 0.5, 0.6]})

# 创建示例userDf
userDf = pd.DataFrame({'user index': [0, 1, 2]})
mediaList = ['A', 'B']

# 初始化media rate列
for media in mediaList:
    userDf[media + ' rate'] = 0

# 更新media rate
for media in mediaList:
    userDf[media + ' rate'] = attributeDf[attributeDf['media'] == media].groupby('user index')['rate'].sum()

print(userDf)
