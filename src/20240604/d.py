import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

filename = '/Users/u03013112/Downloads/polar4ai_jiangyu_kmeans.csv'

# 读取CSV文件
df = pd.read_csv(filename)
df['count'] = 1

df2 = df.groupby('pay_success_realtime').agg({
    'event_id_list_size': 'mean',
    'count': 'sum'
}).reset_index()

df2 = df2.sort_values('count', ascending=False)

print(df2)
df2.to_csv('polar4ai_jiangyu_kmeans_grouped.csv', index=False)