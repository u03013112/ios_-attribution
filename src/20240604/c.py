import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

filename = '/Users/u03013112/Downloads/polar4ai_jiangyu_kmeans.csv'

# 读取CSV文件
df = pd.read_csv(filename)

# 过滤掉 pay_success_realtime 等于 29 的行
df_filtered = df[df['pay_success_realtime'] != 29]

# 解析 event_id_list 列
def parse_event_id_list(x):
    return list(map(int, x.strip('[]').split(',')))

df_filtered['event_id_list'] = df_filtered['event_id_list'].apply(parse_event_id_list)

# 统一 event_id_list 的长度
max_len = df_filtered['event_id_list'].apply(len).max()
df_filtered['event_id_list'] = df_filtered['event_id_list'].apply(lambda x: x + [0] * (max_len - len(x)))

# 使用 pay_success_realtime 作为分类结果
predicted_cluster = df_filtered['pay_success_realtime'].values

# 将数据和标签转换为 DataFrame
event_id_list_df = pd.DataFrame(df_filtered['event_id_list'].tolist())
event_id_list_df['label'] = predicted_cluster

# 计算每个 pay_success_realtime 的用户数
user_counts = event_id_list_df['label'].value_counts().sort_index()

# 标准化特征
scaler = StandardScaler()
event_id_list_scaled = scaler.fit_transform(event_id_list_df.drop(columns='label'))

# 计算质心
centroids = pd.DataFrame(event_id_list_scaled).groupby(event_id_list_df['label']).mean().values

# 计算每个样本到其所属聚类质心的距离
distances = []
for label in np.unique(predicted_cluster):
    cluster_data = event_id_list_scaled[event_id_list_df['label'] == label]
    centroid = centroids[label]
    distance = np.linalg.norm(cluster_data - centroid, axis=1)
    distances.append(distance)

# 计算分类阈值（例如，最大距离和平均距离）
max_distances = [np.max(distance) for distance in distances]
mean_distances = [np.mean(distance) for distance in distances]

# 将质心转换回原始的 event_id_list 格式
centroids_original = scaler.inverse_transform(centroids)
centroids_list = centroids_original.tolist()
centroids_list = [[int(round(value)) for value in centroid] for centroid in centroids_list]

# 创建结果 DataFrame
centroids_df = pd.DataFrame(centroids_list, columns=[f'feature_{i}' for i in range(len(centroids_list[0]))])
centroids_df['label'] = np.unique(predicted_cluster)
centroids_df['user_count'] = user_counts.values
centroids_df['max_distance'] = max_distances
centroids_df['mean_distance'] = mean_distances

# 按 user_count 列降序排列
centroids_df = centroids_df.sort_values(by='user_count', ascending=False)

centroids_df = centroids_df[['label', 'user_count', 'max_distance', 'mean_distance'] + [f'feature_{i}' for i in range(len(centroids_list[0]))]]

# # 打印质心和分类阈值
# print("质心 (event_id_list 格式):", centroids_list)
# print("最大距离:", max_distances)
# print("平均距离:", mean_distances)

# 保存质心和分类阈值到 CSV 文件
centroids_df.to_csv('kmeans_centroids_and_thresholds.csv', index=False)
