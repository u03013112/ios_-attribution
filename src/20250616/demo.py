# 安装必要库（如果未安装，先运行下面注释掉的代码）
# !pip install pymc numpy pandas matplotlib arviz

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pymc as pm
import arviz as az


# 1. 模拟数据生成
np.random.seed(42)

date_range = pd.date_range(start='2025-01-01', end='2025-06-01', freq='D')
n_days = len(date_range)

# 创建趋势：1月-3月缓慢上升，3月-6月缓慢下降
trend = np.piecewise(np.arange(n_days),
                     [np.arange(n_days) < 60, np.arange(n_days) >= 60],
                     [lambda x: 1000 + x * 2,  # 前60天缓慢上升
                      lambda x: 1120 - (x - 60) * 1.5])  # 后面缓慢下降

# 加入随机噪音
observed_income = trend + np.random.normal(0, 15, size=n_days)

# 放入DataFrame方便处理
data = pd.DataFrame({
    'date': date_range,
    'income': observed_income,
    'true_trend': trend
})

# 2. 贝叶斯模型构建（随机游走趋势）
with pm.Model() as model:
    sigma_obs = pm.Exponential('sigma_obs', 1.0)

    # 趋势项用高斯随机游走表示
    sigma_trend = pm.Exponential('sigma_trend', 1.0)
    trend = pm.GaussianRandomWalk('trend', sigma=sigma_trend, shape=n_days)

    # 观测数据围绕趋势波动
    obs = pm.Normal('obs', mu=trend, sigma=sigma_obs, observed=data['income'])

    # 模型推断
    trace = pm.sample(1000, tune=1000, target_accept=0.95, random_seed=42)

# 3. 结果可视化
# 提取趋势的后验均值和置信区间
trend_posterior = az.summary(trace, var_names=['trend'], hdi_prob=0.95)
trend_mean = trend_posterior['mean'].values
trend_lower = trend_posterior['hdi_2.5%'].values
trend_upper = trend_posterior['hdi_97.5%'].values

# 绘图
plt.figure(figsize=(12, 6))
plt.plot(data['date'], data['income'], label='Observed Income', color='blue', alpha=0.5)
plt.plot(data['date'], trend_mean, label='Estimated Trend', color='red')
plt.fill_between(data['date'], trend_lower, trend_upper, color='red', alpha=0.3, label='95% Credible Interval')
plt.title('Bayesian Trend Estimation of Server Income')
plt.xlabel('Date')
plt.ylabel('Income')
plt.legend()
plt.grid(alpha=0.3)

# 保存图片
plt.tight_layout()
plt.savefig('/src/data/server_income_trend.png', dpi=200)
# plt.show()
