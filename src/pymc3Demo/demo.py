import numpy as np
import pymc as pm
import matplotlib.pyplot as plt

# 模拟数据
np.random.seed(42)
n_samples = 1000
media1_ad_spend = np.random.normal(1000, 200, n_samples)
media2_ad_spend = np.random.normal(2000, 300, n_samples)

# 真正的参数
beta_0_true = 1000
beta_1_true = 1
beta_2_true = 2
epsilon_true = 50

# 生成收益数据
revenue = (beta_0_true +
           beta_1_true * media1_ad_spend +
           beta_2_true * media2_ad_spend +
           np.random.normal(0, epsilon_true, n_samples))

# 贝叶斯模型
with pm.Model() as model:
    # 先验分布
    beta_0 = pm.Normal('beta_0', mu=0, sigma=100)
    beta_1 = pm.Normal('beta_1', mu=0, sigma=1)
    beta_2 = pm.Normal('beta_2', mu=0, sigma=1)
    epsilon = pm.HalfNormal('epsilon', sigma=50)
    
    # 线性模型
    mu = beta_0 + beta_1 * media1_ad_spend + beta_2 * media2_ad_spend
    
    # 似然函数
    revenue_obs = pm.Normal('revenue_obs', mu=mu, sigma=epsilon, observed=revenue)
    
    # 采样
    trace = pm.sample(2000)

import arviz as az

# 输出结果
summary = az.summary(trace, hdi_prob=0.95)
print(summary)

# 绘制后验分布
az.plot_posterior(trace, var_names=['beta_0', 'beta_1', 'beta_2', 'epsilon'])
plt.show()
