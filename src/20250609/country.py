import os
import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO


def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/lw_cost_revenue_country_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    app_package,
    country,
    sum(cost_value_usd) as cost,
    sum(revenue_h24) as revenue_h24,
    sum(revenue_d1) as revenue_d1,
    sum(revenue_d7) as revenue_d7,
    sum(revenue_d30) as revenue_d30
from
    dws_overseas_public_roi
where
    app = '502'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package,
    country
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

def main():
    df = getRevenueData('20250101', '20250501')
    # print(df.head())

    iOSDf = df[df['app_package'] == 'id6448786147'].copy()
    # 计算 30 日收入/7 日收入的比率
    iOSDf['ratio'] = iOSDf['revenue_d30'] / iOSDf['revenue_d7']
    # print(iOSDf[iOSDf['country'] == 'US'].head())
    iOSDf_clean = iOSDf.dropna(subset=['ratio']).copy()
    # 按国家分组，计算平均值和标准差
    grouped = iOSDf_clean.groupby('country')['ratio'].agg(['mean', 'std']).reset_index()
    # print(grouped[grouped['country'] == 'US'])
    # 清理数据，去除 NaN 值
    grouped_clean = grouped.dropna().copy()
    # 使用 KMeans 聚类
    n_clusters = 10
    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    grouped_clean['cluster'] = kmeans.fit_predict(grouped_clean[['mean', 'std']])
    
    clusters = grouped_clean.groupby('cluster')
    for cluster_id, group in clusters:
        mean = group['mean'].mean()
        std = group['std'].mean()
        countries = ', '.join(group['country'].tolist())
        print(f"Cluster {cluster_id}: mean={mean:.2f}, std={std:.2f}")
        print(f"Countries: {countries}\n")
    




if __name__ == "__main__":
    main()
    print("Script executed successfully.")