import numpy as np
import pandas as pd
from prophet import Prophet
from econml.dml import LinearDML
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

# 生成示例数据
# 时间序列数据
date_rng = pd.date_range(start='2020-01-01', end='2021-01-01', freq='D')
df = pd.DataFrame(date_rng, columns=['ds'])
df['y'] = 10 + 0.05 * df.index + 2 * (df.index % 7) + np.random.normal(0, 1, len(df))

# 外部因素数据
df['external_factor'] = (df.index % 30) + np.random.normal(0, 1, len(df))

# 使用Prophet进行时间序列预测
prophet_model = Prophet()
prophet_model.fit(df)

future = prophet_model.make_future_dataframe(periods=30)
forecast = prophet_model.predict(future)

# 使用EconML进行因果分析
# 目标是分析external_factor对y的影响
X = df[['external_factor']]
y = df['y']
T = df['external_factor']  # 处理变量

# 分割数据集
X_train, X_test, y_train, y_test, T_train, T_test = train_test_split(X, y, T, test_size=0.2, random_state=42)

# 使用双重机器学习方法
est = LinearDML(model_y=RandomForestRegressor(),
                model_t=RandomForestRegressor(),
                discrete_treatment=False)

est.fit(y_train, T_train, X=X_train)

# 估计因果效应
treatment_effect = est.effect(X_test)

# 输出结果
print("Estimated treatment effect:", treatment_effect.mean())

# 结合结果
# 可以根据因果效应调整Prophet预测结果，或用于解释预测中的变化
