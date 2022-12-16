import numpy as np

a = np.arange(100).reshape(-1,10)
sum = a[:,:2].sum(axis=1).reshape(-1,1)
print(sum)
# # b = a/sum
# # print(b)
# min = a.min(axis=1).reshape(-1,1)
# print(min)
# # b = a - min
# # print(b)
# c = min/min
# print(np.nan_to_num(c))

# print((0.003797195253505933 - 0.0017804154302670622)/(0.006612007405448294 - 0.0017804154302670622))

# a = np.load('/src/data/doc/media2/google/1/google_20221215_041735/mean.npy')
# print(a)