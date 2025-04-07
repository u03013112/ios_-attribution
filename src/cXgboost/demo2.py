import warnings
warnings.filterwarnings('ignore')

import os
import random
import numpy as np
import xgboost
from sklearn.metrics import mean_squared_error, r2_score

# Random generators initialization
seed = 42
random.seed(seed)
os.environ["PYTHONHASHSEED"] = str(seed)
np.random.seed(seed)

def generate_data(num_samples=1000):
    X = np.random.normal(0, 1, num_samples)
    T = np.random.binomial(1, p=0.5, size=num_samples)
    # Increase the effect of X and T on Y
    Y = 15 * X + 18 * T + np.random.normal(0, 1, num_samples)
    potential_Y = np.zeros((num_samples, 2))
    potential_Y[:, 0] = 15 * X + np.random.normal(0, 1, num_samples)  # Y0
    potential_Y[:, 1] = 15 * X + 18 + np.random.normal(0, 1, num_samples)  # Y1
    return X, T, Y, potential_Y

# Generate training and testing data
trainX, trainT, trainY, train_potential_Y = generate_data(num_samples=1000)
testX, testT, testY, test_potential_Y = generate_data(num_samples=500)

# Reshape data
trainX = trainX.reshape(-1, 1)
testX = testX.reshape(-1, 1)

# Custom loss function
treatment = np.array([[1, 0] if x == 0 else [0, 1] for x in trainT]).flatten()
def custom_loss(y_true: np.ndarray = None, y_pred: np.ndarray = None) -> (np.ndarray, np.ndarray):
    grad = 2 * (y_pred.flatten() - y_true.flatten()) * treatment
    hess = 2 * treatment
    return grad, hess

# Train model
model = xgboost.XGBRegressor(
    n_estimators=500, max_depth=4, objective=custom_loss,
    learning_rate=1e-2, n_jobs=-1, tree_method="hist"
)
yt_train = np.concatenate([trainY.reshape(-1, 1), trainT.reshape(-1, 1)], axis=1)
model.fit(trainX, yt_train, eval_set=[(trainX, train_potential_Y), (testX, test_potential_Y)], verbose=25)

# Evaluation function
def evaluate_predictions(Y_true, Y_pred, potential_Y, T):
    mse = mean_squared_error(Y_true, Y_pred[:, 0])
    r2 = r2_score(Y_true, Y_pred[:, 0])
    print(f"[Y预测] MSE: {mse:.4f}, R²: {r2:.4f}")
    
    Y_pred_0 = Y_pred[T == 0, 0]
    Y_pred_1 = Y_pred[T == 1, 0]
    
    mse_potential_0 = mean_squared_error(potential_Y[T == 1, 0], Y_pred_1)
    mse_potential_1 = mean_squared_error(potential_Y[T == 0, 1], Y_pred_0)
    print(f"[potential_Y预测] 反事实MSE - Y0: {mse_potential_0:.4f}, Y1: {mse_potential_1:.4f}")

# Calculate predictions
train_y_hat = model.predict(trainX)
test_y_hat = model.predict(testX)

# Evaluate training and testing sets
print("\n=== 训练集评估 ===")
evaluate_predictions(trainY, train_y_hat, train_potential_Y, trainT)
print("\n=== 测试集评估 ===")
evaluate_predictions(testY, test_y_hat, test_potential_Y, testT)

# Causal effect evaluation
real_ATE = (test_potential_Y[:, 1] - test_potential_Y[:, 0]).mean()
Error_ATE = np.abs(np.mean(test_y_hat[:, 0] - test_y_hat[:, 1]) - real_ATE)
PEHE_score = np.sqrt(np.mean(((test_potential_Y[:, 1] - test_potential_Y[:, 0]) - 
                              (test_y_hat[:, 0] - test_y_hat[:, 1]))**2))

print(f"\n=== 因果效应 ===")
print(f"真实ATE: {real_ATE:.3f}")
print(f"预测ATE误差: {Error_ATE:.3f}")
print(f"PEHE: {PEHE_score:.3f}")
