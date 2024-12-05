import numpy as np
from tensorflow.keras.models import model_from_json
import json
import base64

# 生成测试数据
x_test = np.array([200, 300, 400])

# 从文件中读取模型字符串
with open('model.json', 'r') as f:
    model_str = f.read()

# 从 JSON 字符串反序列化为字典
model_dict = json.loads(model_str)

# 获取模型架构的 JSON 字符串
model_json = model_dict['model_json']

# 获取模型权重的 Base64 编码字符串
model_weights_base64 = model_dict['model_weights']

# 将 Base64 编码字符串解码为二进制数据
model_weights_binary = [base64.b64decode(w) for w in model_weights_base64]

# 从 JSON 字符串中加载模型架构
loaded_model = model_from_json(model_json)

# 获取模型权重的形状
# 通过加载模型架构来动态获取权重的形状
dummy_weights = loaded_model.get_weights()
model_weights_shapes = [w.shape for w in dummy_weights]

# 将二进制数据转换回 numpy 数组
model_weights = [np.frombuffer(w, dtype=np.float32).reshape(shape) for w, shape in zip(model_weights_binary, model_weights_shapes)]

# 将权重加载到模型中
loaded_model.set_weights(model_weights)

# 编译模型
loaded_model.compile(optimizer='RMSprop', loss='mean_squared_error')

# 打印模型摘要（可选）
loaded_model.summary()

# 使用恢复的模型进行预测
loaded_predictions = loaded_model.predict(x_test)
print("Loaded model predictions:", loaded_predictions)
