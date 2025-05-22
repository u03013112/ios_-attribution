# 用安卓自然量估测iOS自然量

import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import lognorm, gamma, kstest
from statsmodels.distributions.empirical_distribution import ECDF

收入数据 = np.array([
    21538.4842,9228.12523,10651.7503,9171.230523,9186.567383,12491.29232,15116.74385,9797.815213,10032.61235,7011.729468,8489.078624,8870.782158,11179.95994,12936.20033,7883.047212,9215.757382,8272.305585,5997.572053,9662.25733,13344.64777,13593.79301,9512.413516,13513.1249,12310.46663,10098.50584,13775.25181,12877.22926,14487.44502,9653.561169,7788.892911,7164.823406,10100.17953,8705.913782,13810.74019,10271.55717,8946.016603,7283.019365,8064.709558,8301.756287,13240.40779,10889.29252,11543.38243,7044.426132,7030.732859,10223.64721,5711.831518,7715.489506,16662.64274,10616.94368,9018.286227,7248.488926,6434.28075,6401.778453,6641.941876,14207.64379,12792.08375,9497.37583,10392.25239,8869.726594,9541.659227,8340.998026,17815.98564,10514.94416,7837.347461,8451.784792,6422.456276,8072.292037,7391.895827,14331.36355,11405.93117,6821.300192,9404.551579,8031.932308,7207.243816,10105.13263,10876.63509,12259.58081
])  # 替换为你的收入数据


shape, loc, scale = lognorm.fit(收入数据, floc=0)
# 计算 mu 和 sigma
mu = np.log(scale)
sigma = shape
# 打印估计的参数
print(f"形状参数 (shape): {shape}")
print(f"位置参数 (loc): {loc}")
print(f"尺度参数 (scale): {scale}")
print(f"均值参数 (mu): {mu}")
print(f"标准差参数 (sigma): {sigma}")

# 拟合对数正态分布
shape_ln, loc_ln, scale_ln = lognorm.fit(收入数据, floc=0)
log_likelihood_ln = np.sum(lognorm.logpdf(收入数据, shape_ln, loc_ln, scale_ln))
aic_ln = 2 * len(收入数据) - 2 * log_likelihood_ln

# 拟合 Gamma 分布
alpha_g, loc_g, beta_g = gamma.fit(收入数据, floc=0)
log_likelihood_g = np.sum(gamma.logpdf(收入数据, alpha_g, loc_g, beta_g))
aic_g = 2 * len(收入数据) - 2 * log_likelihood_g

# KS检验
ks_stat_ln, ks_pvalue_ln = kstest(收入数据, 'lognorm', args=(shape_ln, loc_ln, scale_ln))
ks_stat_g, ks_pvalue_g = kstest(收入数据, 'gamma', args=(alpha_g, loc_g, beta_g))

# 打印结果
print(f"Log-Normal AIC: {aic_ln}, Log-Likelihood: {log_likelihood_ln}, KS: {ks_stat_ln}, p-value: {ks_pvalue_ln}")
print(f"Gamma AIC: {aic_g}, Log-Likelihood: {log_likelihood_g}, KS: {ks_stat_g}, p-value: {ks_pvalue_g}")

# 可视化拟合结果
plt.hist(收入数据, bins=20, density=True, alpha=0.5, color='g', label='数据直方图')
x = np.linspace(min(收入数据), max(收入数据), 100)
pdf_ln = lognorm.pdf(x, shape_ln, loc_ln, scale_ln)
pdf_g = gamma.pdf(x, alpha_g, loc_g, beta_g)
plt.plot(x, pdf_ln, 'r-', lw=2, label='对数正态拟合')
plt.plot(x, pdf_g, 'b-', lw=2, label='Gamma拟合')
plt.xlabel('收入')
plt.ylabel('概率密度')
plt.title('收入分布拟合')
plt.legend()
# plt.show()
plt.savefig("/src/data/收入分布拟合.png")
