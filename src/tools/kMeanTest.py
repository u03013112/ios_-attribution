import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

# 加载数据
df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')

# 过滤和排序数据
df = df[df['payUsd'] > 0]
df = df.sort_values(by=['payUsd']).reset_index(drop=True)

# 付费金额数据
data = df['payUsd'].values.reshape(-1, 1)

# 使用k-means算法对数据进行聚类，分为30个簇
kmeans = KMeans(n_clusters=31, random_state=0).fit(data)

# 获取分箱边界
breaks = sorted(np.unique(kmeans.cluster_centers_).tolist())

# 打印分箱结果
print("分箱边界:", breaks)

# 定义一个函数，根据分箱结果将付费金额映射到cv值
def map_payment_to_cv(payment, breaks):
    for i in range(len(breaks) - 1):
        if payment > breaks[i] and payment <= breaks[i+1]:
            return i + 1
    return len(breaks) - 1

# 测试新的付费金额数据
new_payment = 15
cv = map_payment_to_cv(new_payment, breaks)
print("新付费金额{}美元对应的cv值为: {}".format(new_payment, cv))
