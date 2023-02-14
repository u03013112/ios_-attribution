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

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import Input,layers

def dataMake():
    x1 = np.random.rand(100)
    y1 = x1*2
    x1 = x1.reshape(-1,1)

    x2 = np.random.rand(100)
    y2 = x2*3
    x2 = x2.reshape(-1,1)

    x = np.concatenate((x1,x2),axis = 1)
    y = y1+y2

    # print(x1,y1)
    # print(x2,y2)
    # print(x,y)
    return x,y

# 暂时就拆2组，0和1
def slice(x,index):
    return x[:,index:index+1]

inputLayer = Input(
    shape = (2,),
    name = 'input'
)

layer1 = layers.Lambda(
    slice,
    arguments={'index':0},
    name = 'slice1'
)(inputLayer)

layer1_1 = layers.Dense(
    1, 
    activation='relu',
    kernel_initializer = keras.initializers.Ones(),
    bias_initializer = 'zeros',
    name = 'layer1_1'
)(layer1)

layer2 = layers.Lambda(
    slice,
    arguments={'index':1},
    name = 'slice2'
)(inputLayer)

layer2_1 = layers.Dense(
    1, 
    activation='relu',
    kernel_initializer = keras.initializers.Ones(),
    bias_initializer = 'zeros',
    name = 'layer2_1'
)(layer2)


added = layers.Add()([layer1_1, layer2_1])

outputLayer = layers.Dense(1)(added)

mod = keras.models.Model(inputLayer,outputLayer)

mod.compile(
    optimizer="RMSprop",
    # optimizer = 'adadelta',
    loss='mse'
)
keras.utils.plot_model(mod, '/src/data/mod.png', show_shapes=True)
# 获得某一层的权重和偏置
weight_Dense_1,bias_Dense_1 = mod.get_layer('layer1_1').get_weights()
print(weight_Dense_1,bias_Dense_1)

# 给定keras模型，如deepxi.model， deepxi模型可参考：  https://github.com/anicolson/DeepXi

# 查看模型可训练参数
# for v in mod.trainable_variables:
# 	print(str(v.name) + ', ' + str(v.shape)) 	# 变量名+变量shape
# 	print(str(v.value))							# 变量值

# 查看所有参数：
model_variables = mod.variables
for v in model_variables:
	print(str(v.name) + ', ' + str(v.shape))
	


x,y = dataMake()
# earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
# mod.fit(x, y, epochs=100
#     ,validation_split=0.2
#     ,batch_size = 32
#     # ,callbacks=[earlyStoppingValLoss]
# )


# testX = np.array([1,1,2,2,3,3]).reshape(-1,2)
# print(testX)
# print(mod.predict(testX))


