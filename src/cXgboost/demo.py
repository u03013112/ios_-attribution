import warnings
warnings.filterwarnings('ignore')

import os
import random
import time
import xgboost
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score  # 新增评估指标

# Random generators initialization
seed=42
random.seed(seed)
os.environ["PYTHONHASHSEED"] = str(seed)
np.random.seed(seed)

# Load data
data = np.load('Data/train.npz')
trainX, trainT, trainY, train_potential_Y = data['X'], data['T'], data['Y'], data['potential_Y']
data = np.load('Data/test.npz')
testX, testT, testY, test_potential_Y = data['X'], data['T'], data['Y'], data['potential_Y']

# Custom loss (保持不变)
treatment = np.array([[1,0] if x == 0 else [0,1] for x in trainT]).flatten()
def custom_loss(y_true:np.ndarray=None, y_pred:np.ndarray=None)->(np.ndarray, np.ndarray):
    grad = 2*(y_pred.flatten() - y_true.flatten()) * treatment
    hess = 2 * treatment
    return grad, hess

# Train model
model = xgboost.XGBRegressor(
    n_estimators=500, max_depth=4, objective=custom_loss,
    learning_rate=1e-2, n_jobs=-1, tree_method="hist"
)
yt_train = np.concatenate([trainY.reshape(-1,1), trainT.reshape(-1,1)], axis=1)
model.fit(trainX, yt_train, eval_set=[(trainX, train_potential_Y), (testX, test_potential_Y)], verbose=25)

# 新增评估函数
def evaluate_predictions(Y_true, Y_pred, potential_Y, T):
    """评估观测值和潜在结果的预测准确性"""
    # 1. 观测值Y的预测精度
    mse = mean_squared_error(Y_true, Y_pred[:,0])  # 第一列为Y的预测
    r2 = r2_score(Y_true, Y_pred[:,0])
    print(f"[Y预测] MSE: {mse:.4f}, R²: {r2:.4f}")
    
    # 2. 潜在结果potential_Y的预测精度
    # 分离处理组(T=1)和对照组(T=0)的预测
    Y_pred_0 = Y_pred[T==0, 0]  # 对照组预测Y
    Y_pred_1 = Y_pred[T==1, 0]  # 处理组预测Y
    
    # 计算反事实预测误差
    mse_potential_0 = mean_squared_error(potential_Y[T==1, 0], Y_pred_1)  # 处理组的反事实Y0
    mse_potential_1 = mean_squared_error(potential_Y[T==0, 1], Y_pred_0)  # 对照组的反事实Y1
    print(f"[potential_Y预测] 反事实MSE - Y0: {mse_potential_0:.4f}, Y1: {mse_potential_1:.4f}")

# 计算预测值
train_y_hat = model.predict(trainX)
test_y_hat = model.predict(testX)

# 评估训练集和测试集
print("\n=== 训练集评估 ===")
evaluate_predictions(trainY, train_y_hat, train_potential_Y, trainT)
print("\n=== 测试集评估 ===")
evaluate_predictions(testY, test_y_hat, test_potential_Y, testT)

# 因果效应评估 (保持不变)
real_ATE = (test_potential_Y[:,1] - test_potential_Y[:,0]).mean()
Error_ATE = np.abs(np.mean(test_y_hat[:,0] - test_y_hat[:,1]) - real_ATE)  # 修改为更清晰的ATE误差计算
PEHE_score = np.sqrt(np.mean(( (test_potential_Y[:,1] - test_potential_Y[:,0]) - 
                              (test_y_hat[:,0] - test_y_hat[:,1]) )**2))

print(f"\n=== 因果效应 ===")
print(f"真实ATE: {real_ATE:.3f}")
print(f"预测ATE误差: {Error_ATE:.3f}")
print(f"PEHE: {PEHE_score:.3f}")