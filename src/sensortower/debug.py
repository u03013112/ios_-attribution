import pandas as pd
from scipy.stats import spearmanr, kendalltau
from sklearn.preprocessing import MinMaxScaler

# 你提供的数据
data = {
    'date': ['2024-01-15T00:00:00Z', '2024-01-08T00:00:00Z', '2024-01-01T00:00:00Z', 
             '2023-12-25T00:00:00Z', '2023-12-18T00:00:00Z', '2023-12-11T00:00:00Z', 
             '2023-12-04T00:00:00Z', '2023-11-27T00:00:00Z', '2023-11-20T00:00:00Z', 
             '2023-11-13T00:00:00Z', '2023-11-06T00:00:00Z'],
    'downloads_0': [10230, 5597, 5927, 15977, 15308, 2317, 2307, 12283, 43612, 3368, 2628],
    'downloads_1': [873039, 763104, 826434, 823872, 850134, 960408, 898978, 898978, 901804, 1149705, 1228664]
}
df = pd.DataFrame(data)

# 计算皮尔逊相关系数
pearson_corr = df['downloads_0'].corr(df['downloads_1'])
print(f"Pearson correlation: {pearson_corr}")

# 计算斯皮尔曼等级相关系数
spearman_corr, _ = spearmanr(df['downloads_0'], df['downloads_1'])
print(f"Spearman correlation: {spearman_corr}")

# 计算肯德尔等级相关系数
kendall_corr, _ = kendalltau(df['downloads_0'], df['downloads_1'])
print(f"Kendall correlation: {kendall_corr}")
