import warnings
warnings.filterwarnings('ignore')

import os
import random
import numpy as np
import pandas as pd
from dowhy import CausalModel
from sklearn.metrics import mean_squared_error

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

# Generate data
trainX, trainT, trainY, train_potential_Y = generate_data(num_samples=1000)
testX, testT, testY, test_potential_Y = generate_data(num_samples=500)

# Create DataFrame
data = pd.DataFrame({'X': trainX, 'T': trainT, 'Y': trainY})

# Define causal model
model = CausalModel(
    data=data,
    treatment='T',
    outcome='Y',
    common_causes=['X']
)

# Identify causal effect
identified_estimand = model.identify_effect()

# Estimate causal effect
estimate = model.estimate_effect(identified_estimand, method_name="backdoor.linear_regression")

# Calculate ATE
estimated_ate = estimate.value
print(f"Estimated ATE: {estimated_ate}")

# Calculate performance metrics
real_ATE = (test_potential_Y[:, 1] - test_potential_Y[:, 0]).mean()
Error_ATE = np.abs(estimated_ate - real_ATE)
PEHE_score = np.sqrt(np.mean(((test_potential_Y[:, 1] - test_potential_Y[:, 0]) - estimated_ate)**2))

print(f"真实ATE: {real_ATE:.3f}")
print(f"预测ATE误差: {Error_ATE:.3f}")
print(f"PEHE: {PEHE_score:.3f}")
