import numpy as np
from tensorflow.keras.models import Sequential, model_from_json
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

# 从 JSON 字符串反序列化为字典
model_dict = json.loads(model_str)

# 获取模型架构的 JSON 字符串
model_json = model_dict['model_json']

# 获取模型权重的 Base64 编码字符串
model_weights_base64 = model_dict['model_weights']

# 将 Base64 编码字符串解码为二进制数据
model_weights_binary = [base64.b64decode(w) for w in model_weights_base64]

# 获取模型权重的形状
model_weights_shapes = [w.shape for w in model_weights]

# 将二进制数据转换回 numpy 数组
model_weights = [np.frombuffer(w, dtype=np.float32).reshape(shape) for w, shape in zip(model_weights_binary, model_weights_shapes)]

# 从 JSON 字符串中加载模型架构
loaded_model = model_from_json(model_json)

# 将权重加载到模型中
loaded_model.set_weights(model_weights)

# 编译模型
loaded_model.compile(optimizer='RMSprop', loss='mean_squared_error')

# 打印模型摘要（可选）
loaded_model.summary()

# 使用恢复的模型进行预测
loaded_predictions = loaded_model.predict(x_test)
print("Loaded model predictions:", loaded_predictions)

# 比较两次预测的结果
if np.allclose(original_predictions, loaded_predictions):
    print("The model was saved and loaded correctly.")
else:
    print("There was an error in saving/loading the model.")
