# 预测用户的7日回收金额
# 原始数据：每个用户一行，列应该包括：用户id，注册时间（天），用户来源（广告媒体），7日回收金额，24小时候内一系列行为特征
# 输入特征为用户24小时候内一系列行为特征，比如：付费次数，付费金额，注册时间，升级次数等
# 输出结果，用户7日回收金额
# 预测方式，每个用户逐个预测
# 目标评定标准，按照注册时间（天），和用户来源（广告媒体），分组（groupby，真实7日回收与预测7日回收求和），计算分组后的MAPE与R2，训练可以先主要关注MAPE
# 优先神经网络方案  