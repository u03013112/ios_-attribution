import numpy as np


# 尝试进行拆分训练

# 将输入先拆分，然后每组单独进入一个dnn
# 每组最终输出一个结果
# 然后将所有结果求和
# 尝试是否可以训练处较好的结果

# 样本可以采取分开2组数
# 一组是乘以5+2，另一组是乘以8+1
# 然后看看是否可以得到一个较好的结果

# 如果有较好的结果，看看是否可以将每一组的结果单独获得

from tensorflow import keras
from tensorflow.keras import Input,layers

def dataMake():
    x1 = np.random.rand(100)
    y1 = x1*5+2
    x1 = x1.reshape(-1,1)

    x2 = np.random.rand(100)
    y2 = x2*8+1
    x2 = x2.reshape(-1,1)

    x = np.concatenate((x1,x2),axis = 1)
    y = y1+y2

    # print(x1,y1)
    # print(x2,y2)
    # print(x,y)
    return x,y

def slice(x,index):
    return x[:,index*(3):(index+1)*(3)]

def sum(inputs):
    return np.sum(inputs)

inputLayer = Input(shape = (6,))

layer1_0 = layers.Lambda(slice,arguments={'index':0})(inputLayer)
layer2_0 = layers.Dense(32, activation='relu')(layer1_0)
layer3_0 = layers.Dense(1, activation='relu')(layer2_0)

layer1_1 = layers.Lambda(slice,arguments={'index':1})(inputLayer)
layer2_1 = layers.Dense(32, activation='relu')(layer1_1)
layer3_1 = layers.Dense(1, activation='relu')(layer2_1)

added = layers.Add()([layer3_0, layer3_1])

outputLayer = layers.Dense(1)(added)

mod = keras.models.Model(inputLayer,outputLayer)

x = np.array([
    [0,1,2,3,4,5],
    [1,2,3,4,5,6]
])

print(mod.predict(x))


dataMake()