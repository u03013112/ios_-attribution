import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import json
import base64

# 生成训练数据
x_train = np.array([i for i in range(1, 101)])
y_train = 2 * x_train + 3

# 创建并编译模型
model = Sequential()
model.add(Dense(64, input_dim=1, activation='relu'))
model.add(Dense(32, activation='relu'))
model.add(Dense(1, activation='linear'))

model.compile(optimizer='RMSprop', loss='mean_squared_error')

# 训练模型
model.fit(x_train, y_train, epochs=500, validation_split=0.2, verbose=1)

# 生成测试数据
x_test = np.array([200, 300, 400])

# 使用原始模型进行预测
original_predictions = model.predict(x_test)
print("Original model predictions:", original_predictions)

# 获取模型架构的 JSON 字符串
model_json = model.to_json()

# 获取模型权重
model_weights = model.get_weights()

# 将模型权重转换为二进制数据
model_weights_binary = [w.tobytes() for w in model_weights]

# 将二进制数据进行 Base64 编码
model_weights_base64 = [base64.b64encode(w).decode('utf-8') for w in model_weights_binary]

# 创建一个包含模型架构和权重的字典
model_dict = {
    'model_json': model_json,
    'model_weights': model_weights_base64
}

# 将字典序列化为 JSON 字符串
model_str = json.dumps(model_dict)

# 打印 JSON 字符串（可选）
print(model_str)

# 保存模型字符串到文件
with open('model.json', 'w') as f:
    f.write(model_str)
