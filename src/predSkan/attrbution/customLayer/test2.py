import numpy as np
import tensorflow as tf
from tensorflow.keras import Input, Model
from tensorflow.keras.layers import Dense, Add, Layer, Concatenate
from tensorflow.keras.losses import MeanSquaredError

class ConstraintPenalty(Layer):
    def call(self, inputs):
        # input
        input0 = inputs[:,0]
        # output
        input1 = inputs[:,1]
        # tf.print('input0:', input0)
        # tf.print('input1:', input1)

        lower_bound = 1.5 * input0
        upper_bound = 2.5 * input0

        # tf.print('lower_bound:', lower_bound)
        # tf.print('upper_bound:', upper_bound)


        lower_violation = tf.maximum(0.0, lower_bound - input1)
        upper_violation = tf.maximum(0.0, input1 - upper_bound)

        # tf.print('lower_violation:', lower_violation)
        # tf.print('upper_violation:', upper_violation)

        penalty = 1000 * (lower_violation + upper_violation)

        # tf.print('penalty:', penalty)

        self.add_loss(tf.reduce_mean(penalty))
        
        return input1

# 构建模型
input1 = Input(shape=(1,))
input2 = Input(shape=(1,))
output1 = Dense(1, use_bias=False)(input1)
output2 = Dense(1, use_bias=False)(input2)
penalty_output1 = ConstraintPenalty()(Concatenate()([input1, output1]))
penalty_output2 = ConstraintPenalty()(Concatenate()([input2, output2]))
sum_outputs = Add()([penalty_output1, penalty_output2])
# sum_outputs = Add()([output1, output2])
model = Model(inputs=[input1, input2], outputs=sum_outputs)

# 编译模型
mse_loss = MeanSquaredError()
model.compile(optimizer='RMSprop', loss=mse_loss)

# 训练数据
X1_train = np.array([1, 1, 1])
X2_train = np.array([2, 2, 2])
y_train = np.array([10, 10, 10])

# 训练模型
model.fit([X1_train, X2_train], y_train, epochs=5000)

# 获取并打印权重
weights = model.get_weights()
print("Weight 1:", weights[0][0][0])
print("Weight 2:", weights[1][0][0])

# 预测结果
X1_test = np.array([1,2,3])
X2_test = np.array([2,3,4])
y_pred = model.predict([X1_test, X2_test])
print('y_pred:', y_pred)
