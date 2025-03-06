import pandas as pd


df = pd.read_csv('lastwarFacebookIOS.csv')
df['day'] = pd.to_datetime(df['安装日期 Raw'], format='%Y%m%d')
df['分日installs'] = df['分日installs'].str.replace(',', '').astype(int)
df['分日revenue24h'] = df['分日revenue24h'].str.replace(',', '').str.replace('$', '').astype(float)
df['分日revenue7d'] = df['分日revenue7d'].str.replace(',', '').str.replace('$', '').astype(float)
df['SKA安装(含重装)'] = df['SKA安装(含重装)'].str.replace(',', '').astype(int)
df['SKA收入'] = df['SKA收入'].str.replace(',', '').str.replace('$', '').astype(float)
df['融合归因installs'] = df['融合归因installs'].str.replace(',', '').astype(int)
df['融合归因revenue7d'] = df['融合归因revenue7d'].str.replace(',', '').str.replace('$', '').astype(float)
df['融合归因revenue24h'] = df['融合归因revenue24h'].str.replace(',', '').str.replace('$', '').astype(float)
print(df.head())


# 分析数据
# 将数据按照day分为3段：从开始到2024-12-15，2024-12-15到2025-02-17，2025-02-17到结束
# 先分析安装量，即 '分日installs', 'SKA安装(含重装)', '融合归因installs'
# 计算3列的均值差距，线性相关系数；然后3段的均值差距，线性相关系数，打印到终端

# 然后分析24小时收入，即 '分日revenue24h', 'SKA收入', '融合归因revenue24h'
# 与上面一样，计算均值差距，线性相关系数，打印到终端

# 最后分析7天收入，即 '分日revenue7d', '融合归因revenue7d'
# 与上面一样，计算均值差距，线性相关系数，打印到终端
# 定义时间段
periods = [
    (df['day'].min(), pd.to_datetime('2024-12-15')),
    (pd.to_datetime('2024-12-15'), pd.to_datetime('2025-02-17')),
    (pd.to_datetime('2025-02-17'), df['day'].max())
]

# 分析函数
def analyze_data(df, columns, periods):
    for start, end in periods:
        period_df = df[(df['day'] >= start) & (df['day'] < end)]
        print(f"\nPeriod: {start.date()} to {end.date()}")
        
        for col1, col2 in zip(columns[:-1], columns[1:]):
            mean_diff = period_df[col1].mean() - period_df[col2].mean()
            corr = period_df[[col1, col2]].corr().iloc[0, 1]
            print(f"Mean difference between {col1} and {col2}: {mean_diff:.2f}")
            print(f"Correlation between {col1} and {col2}: {corr:.2f}")

# 分析安装量
print("\nAnalyzing Installs:")
analyze_data(df, ['分日installs', 'SKA安装(含重装)', '融合归因installs'], periods)

# 分析24小时收入
print("\nAnalyzing 24h Revenue:")
analyze_data(df, ['分日revenue24h', 'SKA收入', '融合归因revenue24h'], periods)

# 分析7天收入
print("\nAnalyzing 7d Revenue:")
analyze_data(df, ['分日revenue7d', '融合归因revenue7d'], periods)

import matplotlib.pyplot as plt

# 定义垂直线的位置
vertical_lines = ['2024-12-15', '2025-02-17']
vertical_lines = pd.to_datetime(vertical_lines)

# 绘制第一个图：installs
plt.figure(figsize=(24, 6))
plt.plot(df['day'], df['分日installs'], label='AF installs')
plt.plot(df['day'], df['SKA安装(含重装)'], label='SKA installs')
plt.plot(df['day'], df['融合归因installs'], label='Merge installs')

for line in vertical_lines:
    plt.axvline(x=line, color='gray', linestyle='--')

plt.xlabel('Day')
plt.ylabel('Installs')
plt.title('Installs Over Time')
plt.legend()
plt.grid(True)
plt.savefig('/src/data/lastwarFacebookIOS_install.png')
plt.close()

# 绘制第二个图：revenue24h
plt.figure(figsize=(24, 6))
plt.plot(df['day'], df['分日revenue24h'], label='AF revenue24h')
plt.plot(df['day'], df['SKA收入'], label='SKA revenue24h')
plt.plot(df['day'], df['融合归因revenue24h'], label='Merge revenue24h')

for line in vertical_lines:
    plt.axvline(x=line, color='gray', linestyle='--')

plt.xlabel('Day')
plt.ylabel('Revenue 24h')
plt.title('Revenue 24h Over Time')
plt.legend()
plt.grid(True)
plt.savefig('/src/data/lastwarFacebookIOS_revenue24h.png')
plt.close()

# 绘制第三个图：revenue7d
plt.figure(figsize=(24, 6))
plt.plot(df['day'], df['分日revenue7d'], label='AF revenue7d')
plt.plot(df['day'], df['融合归因revenue7d'], label='Merge revenue7d')

for line in vertical_lines:
    plt.axvline(x=line, color='gray', linestyle='--')

plt.xlabel('Day')
plt.ylabel('Revenue 7d')
plt.title('Revenue 7d Over Time')
plt.legend()
plt.grid(True)
plt.savefig('/src/data/lastwarFacebookIOS_revenue7d.png')
plt.close()