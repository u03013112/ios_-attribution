import jenkspy
import pandas as pd
import numpy as np


df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
df = df[df['payUsd'] > 0]
df = df.sort_values(by=['payUsd']).reset_index(drop=True)

# print(df.head())
# 付费金额数据
data = df['payUsd']

# 使用Jenks自然断点法对数据进行分箱，分为3个区间
breaks = jenkspy.jenks_breaks(data, n_classes=30)

# 打印分箱结果
print("分箱边界:",len(breaks), breaks)

# # 定义一个函数，根据分箱结果将付费金额映射到cv值
# def map_payment_to_cv(payment, breaks):
#     for i in range(len(breaks) - 1):
#         if payment > breaks[i] and payment <= breaks[i+1]:
#             return i + 1
#     return len(breaks) - 1

# # 测试新的付费金额数据
# new_payment = 15
# cv = map_payment_to_cv(new_payment, breaks)
# print("新付费金额{}美元对应的cv值为: {}".format(new_payment, cv))
