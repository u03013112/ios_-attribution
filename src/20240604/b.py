import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import adjusted_rand_score
import joblib

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

# 将 event_id_list 转换为适合 K-means 的格式
mlb = MultiLabelBinarizer()
event_id_list_encoded = mlb.fit_transform(df_filtered['event_id_list'])

# 定义参数范围
n_clusters_range = [25, 29, 33]
n_init_range = [10, 20, 30]
max_iter_range = [300, 500, 700]

# 定义运行次数
num_repeats = 5

# 存储结果
results = []

# 网格搜索
for n_clusters in n_clusters_range:
    for n_init in n_init_range:
        for max_iter in max_iter_range:
            for repeat in range(num_repeats):
                kmeans = KMeans(n_clusters=n_clusters, init='k-means++', random_state=repeat, n_init=n_init, max_iter=max_iter)
                predicted_cluster = kmeans.fit_predict(event_id_list_encoded)
                ari_score = adjusted_rand_score(df_filtered['pay_success_realtime'], predicted_cluster)
                
                # 存储结果
                results.append({
                    'n_clusters': n_clusters,
                    'n_init': n_init,
                    'max_iter': max_iter,
                    'repeat': repeat,
                    'ari_score': ari_score
                })
                
                # 打印结果
                print(f'n_clusters: {n_clusters}, n_init: {n_init}, max_iter: {max_iter}, repeat: {repeat}, ARI: {ari_score:.2f}')

# 转换结果为 DataFrame
results_df = pd.DataFrame(results)

# 保存结果到CSV文件
results_df.to_csv('kmeans_grid_search_results.csv', index=False)