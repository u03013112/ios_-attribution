import pymc as pm
import numpy as np
import matplotlib.pyplot as plt

# 模拟数据
np.random.seed(42)
days = np.arange(30)  # 30 天的数据
weekdays = days % 7  # 模拟星期几，0 表示周一，6 表示周日
平时收入 = 500  # 平时收入
周期性影响 = np.where(weekdays >= 5, 100, 0)  # 周六、周日 +100
随机噪音 = np.random.normal(0, 10, size=days.size)  # 随机噪音
收入 = 平时收入 + 周期性影响 + 随机噪音

# 贝叶斯模型
with pm.Model() as model:
    # 平时收入的先验分布
    平时收入_prior = pm.Normal("平时收入", mu=500, sigma=50)
    
    # 周期性影响的先验分布
    周末加成_prior = pm.Normal("周末加成", mu=100, sigma=20)
    
    # 预期收入
    mu = 平时收入_prior + pm.math.switch(weekdays >= 5, 周末加成_prior, 0)
    
    # 似然函数（观察数据的概率分布）
    sigma = pm.HalfNormal("sigma", sigma=10)
    收入_obs = pm.Normal("收入_obs", mu=mu, sigma=sigma, observed=收入)
    
    # 进行推断
    trace = pm.sample(1000, tune=2000)

# 后验分析
pm.plot_trace(trace)
# plt.show()
plt.savefig("trace_plot.png")

# 打印后验总结
summary = pm.summary(trace)
print(summary)
