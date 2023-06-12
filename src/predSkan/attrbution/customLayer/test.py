import numpy as np
import tensorflow as tf
from tensorflow.keras import Input, Model
from tensorflow.keras.layers import Dense, Concatenate
from tensorflow.keras import backend as K

# 自定义损失函数
def custom_loss(y_true, y_pred):
    # 将y_pred拆分为两个输出
    # output1, output2 = tf.split(y_pred, num_or_size_splits=2, axis=-1)
    # sum_outputs = output1 + output2
    sum_outputs = K.sum(y_pred, axis=-1)
    loss = tf.reduce_mean(tf.square(y_true - sum_outputs))

    # 打印中间变量和损失值
    tf.print("y_pred:", y_pred)
    tf.print("y_true:", y_true)
    tf.print("sum_outputs:", sum_outputs)
    tf.print("Loss:", loss)

    return loss

# 构建简化模型
input1 = Input(shape=(1,))
input2 = Input(shape=(1,))
output1 = Dense(1, use_bias=False)(input1)
output2 = Dense(1, use_bias=False)(input2)
concat_outputs = Concatenate()([output1, output2])
model = Model(inputs=[input1, input2], outputs=concat_outputs)

# 编译模型
model.compile(optimizer='RMSprop', loss=custom_loss)

# 简单数据
X1_train = np.array([[1]])
X2_train = np.array([[2]])
y_train = np.array([[3]], dtype=np.float32)

# 训练模型（仅1个epoch）
model.fit([X1_train, X2_train], y_train, epochs=1)

# 获取并打印权重
weights = model.get_weights()
print("Weight 1:", weights[0][0][0])
print("Weight 2:", weights[1][0][0])
