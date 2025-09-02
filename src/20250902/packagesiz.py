import pandas as pd


import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2




sql = """
SELECT
	CASE
		WHEN country IN (
			'AD',
			'AT',
			'AU',
			'BE',
			'CA',
			'CH',
			'DE',
			'DK',
			'FI',
			'FR',
			'HK',
			'IE',
			'IS',
			'IT',
			'LI',
			'LU',
			'MC',
			'NL',
			'NO',
			'NZ',
			'SE',
			'SG',
			'UK',
			'MO',
			'IL',
			'TW'
		) THEN 'T1'
		WHEN country = 'US' THEN 'US'
		WHEN country = 'JP' THEN 'JP'
		WHEN country = 'KR' THEN 'KR'
		WHEN country = 'BR' THEN 'BR'
		WHEN country = 'IN' THEN 'IN'
		WHEN country = 'ID' THEN 'ID'
		WHEN country IN ('SA', 'AE', 'QA', 'KW', 'BH', 'OM') THEN 'GCC'
		ELSE 'other'
	END AS country_tier,
	CASE
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-08-27') THEN 721
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-08-20') THEN 723
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-08-13') THEN 736
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-08-06') THEN 746
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-07-30') THEN 771
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-07-23') THEN 825
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-07-16') THEN 1120 -- 1.12GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-07-10') THEN 1140 -- 1.14GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-07-02') THEN 1250 -- 1.25GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-06-25') THEN 1250 -- 1.25GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-06-18') THEN 1240 -- 1.24GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-06-11') THEN 1300 -- 1.3GB转换为MB
		WHEN to_date(install_day, 'yyyyMMdd') >= date('2025-06-04') THEN 1310 -- 1.31GB转换为MB
		ELSE 1500 -- 1.5GB转换为MB，2025.6.4之前
	END AS package_size_mb,
	SUM(cost_value_usd) as total_cost,
	SUM(installs) as total_installs,
	CASE
		WHEN SUM(installs) > 0 THEN SUM(cost_value_usd) / SUM(installs)
		ELSE NULL
	END AS cpi
FROM
	marketing.attribution.dws_overseas_public_roi
WHERE
	install_day BETWEEN '20250501'
	and '20250831'
	AND app_package = 'com.fun.lastwar.gp'
	AND facebook_segment IN ('country', 'N/A')
	AND mediasource != 'Organic'
	AND installs > 0 -- 确保有安装数据
GROUP BY
	country_tier,
	package_size_mb
HAVING
	SUM(installs) >= 100 -- 过滤掉安装量太少的数据，确保统计意义
ORDER BY
	country_tier,
	package_size_mb
;
"""



df = execSql(sql)

# 按照国家进行分组
# 组内按照安装包大小进行排序
# 计算每个国家组内的皮尔森相关系数，斯皮尔曼相关系数，并保存到csv

from scipy.stats import pearsonr, spearmanr
import numpy as np

# 确保数据按照country_tier和package_size_mb排序
df = df.sort_values(['country_tier', 'package_size_mb'])

# 计算相关系数的结果列表
correlation_results = []

# 按国家分组计算相关系数
for country_tier in df['country_tier'].unique():
    country_data = df[df['country_tier'] == country_tier].copy()
    
    # 确保该国家有足够的数据点进行相关性分析（至少3个点）
    if len(country_data) >= 3:
        # 提取安装包大小和CPI数据
        package_sizes = country_data['package_size_mb'].values
        cpi_values = country_data['cpi'].values
        
        # 移除CPI为空值的数据
        valid_mask = ~np.isnan(cpi_values)
        if np.sum(valid_mask) >= 3:  # 确保有效数据点足够
            valid_package_sizes = package_sizes[valid_mask]
            valid_cpi_values = cpi_values[valid_mask]
            
            # 计算皮尔森相关系数
            try:
                pearson_corr, pearson_p = pearsonr(valid_package_sizes, valid_cpi_values)
            except:
                pearson_corr, pearson_p = np.nan, np.nan
            
            # 计算斯皮尔曼相关系数
            try:
                spearman_corr, spearman_p = spearmanr(valid_package_sizes, valid_cpi_values)
            except:
                spearman_corr, spearman_p = np.nan, np.nan
            
            # 添加到结果列表
            correlation_results.append({
                'country_tier': country_tier,
                'data_points': len(valid_package_sizes),
                'total_installs': country_data['total_installs'].sum(),
                'total_cost': country_data['total_cost'].sum(),
                'avg_cpi': country_data['cpi'].mean(),
                'pearson_correlation': pearson_corr,
                'pearson_p_value': pearson_p,
                'spearman_correlation': spearman_corr,
                'spearman_p_value': spearman_p,
                'min_package_size': valid_package_sizes.min(),
                'max_package_size': valid_package_sizes.max(),
                'min_cpi': valid_cpi_values.min(),
                'max_cpi': valid_cpi_values.max()
            })

# 转换为DataFrame
correlation_df = pd.DataFrame(correlation_results)

# 按相关系数排序
correlation_df = correlation_df.sort_values('pearson_correlation', ascending=False)

# 保存原始数据到CSV
df.to_csv('/src/data/20250902_package_size_analysis_raw_data.csv', index=False, encoding='utf-8')
print("原始数据已保存到: /src/data/20250902_package_size_analysis_raw_data.csv")

# 保存相关系数分析结果到CSV
correlation_df.to_csv('/src/data/20250902_package_size_correlation_analysis.csv', index=False, encoding='utf-8')
print("相关系数分析结果已保存到: /src/data/20250902_package_size_correlation_analysis.csv")

# 打印分析结果摘要
print("\n=== 安装包大小与CPI相关性分析结果 ===")
print(f"总共分析了 {len(correlation_df)} 个国家/地区")
print("\n各国家/地区相关性分析结果:")
for _, row in correlation_df.iterrows():
    print(f"\n{row['country_tier']}:")
    print(f"  数据点数量: {row['data_points']}")
    print(f"  总安装量: {row['total_installs']:,.0f}")
    print(f"  平均CPI: ${row['avg_cpi']:.4f}")
    print(f"  皮尔森相关系数: {row['pearson_correlation']:.4f} (p-value: {row['pearson_p_value']:.4f})")
    print(f"  斯皮尔曼相关系数: {row['spearman_correlation']:.4f} (p-value: {row['spearman_p_value']:.4f})")
    print(f"  安装包大小范围: {row['min_package_size']:.0f}MB - {row['max_package_size']:.0f}MB")
    print(f"  CPI范围: ${row['min_cpi']:.4f} - ${row['max_cpi']:.4f}")

print(f"\n分析完成！详细结果已保存到CSV文件中。")
