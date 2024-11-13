import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression

# 添加自定义模块路径
sys.path.append('/src')
from src.maxCompute import execSql as execSql_local

# # 使用本地的 execSql 函数
# execSql = execSql_local

# # 1. 修改 SQL 查询，获取数据延长到20241111日为止
# sql = '''
# select
#     install_day,
#     sum(server_login_users) as dau,
#     sum(installs) as installs
# from
#     rg_bi.dws_overseas_server_roi_public_v2
# where
#     app = '116'
#     and install_day between '20240101'
#     and '20241111'
#     and country = 'JP'
# group by
#     install_day
# ;
# '''

# # 执行 SQL 并获取数据
# data = execSql(sql)

# # 确保数据已经加载
# if data is None or len(data) == 0:
#     print("没有获取到数据，请检查 SQL 查询或数据源。")
#     sys.exit(1)

# # 将数据转换为 pandas DataFrame（假设 execSql 返回的数据可转换为 DataFrame）
# df = pd.DataFrame(data)

# # 数据预览
# print("原始数据预览：")
# print(df.head())

# # 将 install_day 转换为日期格式
# df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

# # 按自然月提取年份和月份
# df['year_month'] = df['install_day'].dt.to_period('M')

# # 按日期排序
# df = df.sort_values('install_day')

# # 保存到 CSV（可选）
# df.to_csv('/src/data/lingqiang.csv', index=False)

# 从 CSV 读取数据，并解析 install_day 为日期时间类型
df = pd.read_csv('/src/data/lingqiang.csv', parse_dates=['install_day'])

# 将 year_month 转换回 Period 类型（因为读取 CSV 后会变成字符串）
df['year_month'] = pd.to_datetime(df['year_month']).dt.to_period('M')

# 过滤掉不完整的11月数据
max_date = df['install_day'].max()
max_month = max_date.to_period('M')
# 假设11月是最新的且不完整
df_complete = df[df['year_month'] < max_month]

# 按自然月分组，计算每月的平均新增用户数和平均 DAU
monthly = df_complete.groupby('year_month').agg(
    avg_installs=pd.NamedAgg(column='installs', aggfunc='mean'),
    avg_dau=pd.NamedAgg(column='dau', aggfunc='mean')
).reset_index()

# 计算 DAU 差异（当前月平均 DAU - 上月平均 DAU）
monthly['dau_diff'] = monthly['avg_dau'].diff()

# 删除第一个月，因为它没有上个月的数据来计算差异
monthly = monthly.dropna()

# 重置索引（可选）
monthly = monthly.reset_index(drop=True)

# 输出每个月的 DAU 差距和新增用户数
print("\n每个月的 DAU 差距和新增用户数（按月平均）：")
print(monthly[['year_month', 'dau_diff', 'avg_installs']])

# 2. 仅保留从5月开始的数据
# 假设数据包含2024年的数据，可以根据实际年份调整
monthly = monthly[monthly['year_month'] >= '2024-05']

print("\n过滤后从5月开始的数据：")
print(monthly)

# 3. 计算相关系数，选择最近5个月
recent_months = 5
if len(monthly) < recent_months:
    print(f"数据不足{recent_months}个月，无法进行相关系数计算。")
    sys.exit(1)

recent_data = monthly.tail(recent_months)

# 计算 Pearson 相关系数
corr = recent_data['dau_diff'].corr(recent_data['avg_installs'])
print(f"\n最近 {recent_months} 个月的 DAU 差异与新增用户数之间的 Pearson 相关系数: {corr:.4f}")

# 确保相关系数足够高（根据您的描述，0.9255）
if corr < 0.8:
    print("相关系数较低，可能不适合进行线性回归拟合。")
else:
    # 4. 进行线性回归拟合，找到 w 和 b
    X = recent_data[['avg_installs']].values  # 需要转换为二维数组
    y = recent_data['dau_diff'].values

    model = LinearRegression()
    model.fit(X, y)

    w = model.coef_[0]
    b = model.intercept_

    print(f"\n线性回归结果：dau_diff = {w:.4f} * avg_installs + {b:.4f}")

    # 5. 可视化最近5个月的拟合结果
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='avg_installs', y='dau_diff', data=recent_data, label='数据点')
    plt.plot(recent_data['avg_installs'], model.predict(X), color='red', label=f'拟合线: y={w:.2f}x + {b:.2f}')
    plt.xlabel('平均新增用户数 (avg_installs)')
    plt.ylabel('DAU 差异 (dau_diff)')
    plt.title('最近5个月的 DAU 差异与新增用户数关系及线性拟合')
    plt.legend()
    plt.tight_layout()
    plt.savefig('/src/data/lingqiang_recent5_fit.png')
    # plt.show()

    # 6. 使用回归模型预测所需的 avg_installs
    target_dau = 33000
    current_dau = monthly['avg_dau'].iloc[-1]
    dau_diff_needed = target_dau - current_dau
    print(f"\n截至 {max_date.date()} 的当前 DAU: {current_dau}")
    print(f"目标 DAU: {target_dau} 于 2024-12-15 之前")
    print(f"目标 DAU 差异 (dau_diff_needed): {dau_diff_needed:.2f}")

    # 根据线性回归模型: dau_diff = w * avg_installs + b
    # 需要求解 avg_installs = (dau_diff_needed - b) / w
    if w != 0:
        required_avg_installs = (dau_diff_needed - b) / w
        print(f"根据回归模型，达到目标 DAU 差异所需的平均新增用户数: {required_avg_installs:.2f}")
    else:
        required_avg_installs = 0
        print("斜率 w 为 0，无法根据回归模型计算所需的平均新增用户数。")

    # 将估算结果添加到 DataFrame（可选）
    forecast_df = pd.DataFrame({
        'year_month': [monthly['year_month'].max() + 1],  # 下一个月
        'required_avg_installs': [required_avg_installs]
    })

    forecast_df.to_csv('/src/data/required_avg_installs.csv', index=False)
    print("\n所需平均新增用户数已保存到 /src/data/required_avg_installs.csv")
