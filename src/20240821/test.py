import numpy as np

# 数据
slg_top3_sov_sum = [0.3619924, 0.213121, 0.2382918, 0.2939306, 0.4632784, 0.3127666]
lastwar_sov_sum = [0.1834806, 0.087791, 0.0915714, 0.102128, 0.2010136, 0.13643]

# 计算皮尔逊相关系数
correlation_coefficient = np.corrcoef(slg_top3_sov_sum, lastwar_sov_sum)[0, 1]

print("皮尔逊相关系数:", correlation_coefficient)
