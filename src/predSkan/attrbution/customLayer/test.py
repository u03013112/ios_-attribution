import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# 创建数据
np.random.seed(42)
X = np.random.rand(100, 5)  # 100个样本，每个样本有5个特征（i1, i2, i3, i4, i5）
w_true = np.array([2, 3, 4, 5, 6])  # 真实权重（w1, w2, w3, w4, w5）

# 计算y = i1 * (1 + w1) + i2 * (1 + w2) + i3 * (1 + w3) + i4 * (1 + w4) + i5 * (1 + w5)
y = np.dot(X, w_true + 1)

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 建模并训练
model = LinearRegression()
model.fit(X_train, y_train)

# 获取并打印权重
w_pred = model.coef_
print("Predicted weights (w1, w2, w3, w4, w5):", w_pred)

# 验证模型预测结果（训练集和测试集）
y_train_pred = model.predict(X_train)
y_test_pred = model.predict(X_test)

mse_train = np.mean((y_train_pred - y_train) ** 2)
mse_test = np.mean((y_test_pred - y_test) ** 2)

print("Mean squared error (train):", mse_train)
print("Mean squared error (test):", mse_test)
