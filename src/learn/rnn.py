# rnn初体验，python，keras
# 数据生成环境：模拟广告回收，其中x是广告花费，y是广告回收，但是中间存在一个隐变量a
# a是广告回收的效率，由多重市场因素组成，难以形成有效数据采集。a每天都会变化，但是变化幅度有限，每天变化不超过5%
# 生成训练集100组数据，测试集60组数据
# 其中测试集不是直接进行测试，而是会先将前10个作为已知，后面50个进行预测，然后再与真实值进行比较
# 评估指标：MAPE，R2
# 用rnn和dnn分别做预测，比较两者的效果

import numpy as np
import random
from keras.models import Sequential
from keras.layers import Dense, SimpleRNN
from sklearn.metrics import mean_absolute_percentage_error, r2_score

# 数据生成
def generate_data(num_samples):
    x = np.random.uniform(0, 100, num_samples)
    a = np.random.uniform(0.1, 1, num_samples)
    for i in range(1, num_samples):
        a[i] = a[i - 1] * (1 + random.uniform(-0.05, 0.05))
    y = x * a
    return x, y

# 数据预处理
def create_dataset(x, y, time_steps=10):
    x_train, y_train = [], []
    for i in range(len(x) - time_steps):
        x_train.append(x[i : (i + time_steps)])
        y_train.append(y[i + time_steps])
    return np.array(x_train).reshape(-1, time_steps, 1), np.array(y_train)

# RNN模型
def rnn_model(train_x, train_y, test_x, time_steps=10):
    train_x, train_y = create_dataset(train_x, train_y, time_steps)
    test_x, _ = create_dataset(test_x, np.zeros(len(test_x)), time_steps)

    model = Sequential()
    model.add(SimpleRNN(10, input_shape=(time_steps, 1)))
    model.add(Dense(1))
    model.compile(optimizer='RMSprop', loss='mse')

    model.fit(train_x, train_y, epochs=100,batch_size = 1
        # , verbose=0
    )

    preds = model.predict(test_x).flatten()

    return preds


# DNN模型
def dnn_model(train_x, train_y, test_x):
    model = Sequential()
    model.add(Dense(10, input_shape=(1,), activation='relu'))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')

    model.fit(train_x, train_y, epochs=100, verbose=0)

    preds = model.predict(test_x).flatten()

    return preds

# 生成训练集和测试集
train_x, train_y = generate_data(100)
test_x, test_y = generate_data(60)

# 预测
rnn_preds = rnn_model(train_x, train_y, test_x)
dnn_preds = dnn_model(train_x, train_y, test_x)

# 评估指标
mape_rnn = mean_absolute_percentage_error(test_y[10:], rnn_preds)
mape_dnn = mean_absolute_percentage_error(test_y, dnn_preds)

r2_rnn = r2_score(test_y[10:], rnn_preds)
r2_dnn = r2_score(test_y, dnn_preds)

print("RNN MAPE:", mape_rnn)
print("DNN MAPE:", mape_dnn)
print("RNN R2:", r2_rnn)
print("DNN R2:", r2_dnn)
